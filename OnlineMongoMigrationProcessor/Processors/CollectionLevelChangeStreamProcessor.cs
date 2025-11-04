using MongoDB.Bson;
using MongoDB.Driver;
using OnlineMongoMigrationProcessor.Helpers;
using OnlineMongoMigrationProcessor.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using static OnlineMongoMigrationProcessor.MongoHelper;

#pragma warning disable CS8602 // Dereference of a possibly null reference.

namespace OnlineMongoMigrationProcessor
{
    public class CollectionLevelChangeStreamProcessor : ChangeStreamProcessor
    {
        public CollectionLevelChangeStreamProcessor(Log log, MongoClient sourceClient, MongoClient targetClient, JobList jobList, MigrationJob job, MigrationSettings config, bool syncBack = false)
            : base(log, sourceClient, targetClient, jobList, job, config, syncBack)
        {
        }

        protected override async Task ProcessChangeStreamsAsync(CancellationToken token)
        {
            _log.ShowInMonitor($"{_syncBackPrefix}ProcessChangeStreamsAsync started. Token cancelled: {token.IsCancellationRequested}, ExecutionCancelled: {ExecutionCancelled}");
            _log.WriteLine($"{_syncBackPrefix}ProcessChangeStreamsAsync initialization - ConcurrentProcessors: {_concurrentProcessors}, ProcessorRunMaxDuration: {_processorRunMaxDurationInSec}s, MigrationUnits: {_migrationUnitsToProcess.Count}", LogType.Verbose);

            bool isVCore = (_syncBack ? _job.TargetEndpoint : _job.SourceEndpoint)
                .Contains("mongocluster.cosmos.azure.com", StringComparison.OrdinalIgnoreCase);
            _log.WriteLine($"{_syncBackPrefix}Environment detection - IsVCore: {isVCore}, SyncBack: {_syncBack}", LogType.Debug);

            int index = 0;

            // Get the latest sorted keys
            var sortedKeys = _migrationUnitsToProcess
                .OrderByDescending(kvp => kvp.Value.CSNormalizedUpdatesInLastBatch)
                .Select(kvp => kvp.Key)
                .ToList();

            _log.WriteLine($"{_syncBackPrefix}Starting collection-level change stream processing for {sortedKeys.Count} collection(s). Each round-robin batch will process {Math.Min(_concurrentProcessors, sortedKeys.Count)} collections. Max duration per batch {_processorRunMaxDurationInSec} seconds.");

            long loops = 0;
            bool oplogSuccess = true;

            while (!token.IsCancellationRequested && !ExecutionCancelled)
            {
                var totalKeys = sortedKeys.Count;


                while (index < totalKeys && !token.IsCancellationRequested && !ExecutionCancelled)
                {
                    var tasks = new List<Task>();
                    var collectionProcessed = new List<string>();

                    // Determine the batch
                    var batchKeys = sortedKeys.Skip(index).Take(_concurrentProcessors).ToList();
                    var batchUnits = batchKeys
                        .Select(k => _migrationUnitsToProcess.TryGetValue(k, out var unit) ? unit : null)
                        .Where(u => u != null)
                        .ToList();

                    //total of batchUnits.All(u => u.CSUpdatesInLastBatch)
                    long totalUpdatesInBatch = batchUnits.Sum(u => u.CSNormalizedUpdatesInLastBatch);

                    //total of  _migrationUnitsToProcess
                    long totalUpdatesInAll = _migrationUnitsToProcess.Sum(kvp => kvp.Value.CSNormalizedUpdatesInLastBatch);

                    float timeFactor = totalUpdatesInAll > 0 ? (float)totalUpdatesInBatch / totalUpdatesInAll : 1;
                    _log.WriteLine($"{_syncBackPrefix}Batch load calculation - TotalUpdatesInBatch: {totalUpdatesInBatch}, TotalUpdatesInAll: {totalUpdatesInAll}, TimeFactor: {timeFactor:F3}", LogType.Verbose);

                    int seconds = GetBatchDurationInSeconds(timeFactor);
                    _log.WriteLine($"{_syncBackPrefix}Batch duration computed - Seconds: {seconds}, Collections: {batchKeys.Count}", LogType.Debug);
                    
                    
                    foreach (var key in batchKeys)
                    {
                        if (_migrationUnitsToProcess.TryGetValue(key, out var unit))
                        {
                            collectionProcessed.Add(key);
                            unit.CSLastBatchDurationSeconds = seconds; // Store the factor for each unit
                            // Don't pass token to Task.Run - each collection manages its own timeout via CancellationTokenSource inside ProcessCollectionChangeStream
                            string collectionName = key; // Capture for closure
                            
                            
                            try
                            {
                                var task = Task.Run(async () =>
                                {
                                    try
                                    {
                                        await ProcessCollectionChangeStream(unit, true, seconds);
                                    }
                                    catch (Exception ex)
                                    {
                                        _log.WriteLine($"{_syncBackPrefix}Unhandled exception in Task.Run for collection {collectionName}: {ex}", LogType.Error);
                                        throw; // Re-throw to ensure Task.WhenAll sees the failure
                                    }
                                });
                                
                                tasks.Add(task);
                            }
                            catch
                            {
                                throw;
                            }
                        }
                    }

                    _log.WriteLine($"{_syncBackPrefix}Processing change streams for collections: {string.Join(", ", collectionProcessed)}. Batch Duration {seconds} seconds", LogType.Info);
                    
                    // Log memory stats before batch processing
                    LogMemoryStats("BeforeBatch");

                    try
                    {
                        await Task.WhenAll(tasks);
                    }
                    catch (Exception ex)
                    {
                        _log.WriteLine($"{_syncBackPrefix}Task.WhenAll threw exception: {ex.Message}", LogType.Error);

                        // Log individual task states
                        for (int i = 0; i < tasks.Count; i++)
                        {
                            try
                            {
                                var task = tasks[i];
                                
                                if (task.IsFaulted)
                                {
                                    var baseEx = task.Exception?.GetBaseException();
                                    _log.WriteLine($"{_syncBackPrefix}Task {i} FAULTED: {baseEx?.Message}", LogType.Error);
                                }
                                else if (task.IsCanceled)
                                {
                                    _log.WriteLine($"{_syncBackPrefix}Task {i} CANCELED", LogType.Warning);
                                }
                            }
                            catch
                            {
                            }
                        }

                        throw; // Re-throw to let outer exception handler deal with it
                    }


                    index += _concurrentProcessors;

                    // Pause between batches to allow memory recovery and reduce CPU spikes
                    // Increased to 5000ms to address OOM issues and server CPU spikes
                    Thread.Sleep(5000);
                    
                    // Force garbage collection between batches to prevent memory buildup
                    GC.Collect(2, GCCollectionMode.Aggressive, true, true);
                    GC.WaitForPendingFinalizers();
                    
                    // Log memory stats after batch processing and GC
                    LogMemoryStats("AfterBatch");
                }

                _log.WriteLine($"{_syncBackPrefix}Completed round {loops + 1} of change stream processing for all {totalKeys} collection(s). Re-sorting by load and starting new round...");

                index = 0;
                // Sort the dictionary after all processing is complete
                sortedKeys = _migrationUnitsToProcess
                    .OrderByDescending(kvp => kvp.Value.CSNormalizedUpdatesInLastBatch)
                    .Select(kvp => kvp.Key)
                    .ToList();

                loops++;
                // every 4 loops, check for oplog count, doesn't work on vcore
                if (loops % 4 == 0 && oplogSuccess && !isVCore && !_syncBack)
                {
                    _log.WriteLine($"{_syncBackPrefix}Oplog check triggered at loop {loops}", LogType.Verbose);
                    foreach (var unit in _migrationUnitsToProcess)
                    {
                        if (unit.Value.CursorUtcTimestamp > DateTime.MinValue)
                        {
                            // Convert DateTime to Unix timestamp (seconds since Jan 1, 1970)
                            long secondsSinceEpoch = new DateTimeOffset(unit.Value.CursorUtcTimestamp.ToLocalTime()).ToUnixTimeSeconds();

                            _ = Task.Run(() =>
                            {
                                try
                                {
                                    oplogSuccess = MongoHelper.GetPendingOplogCountAsync(_log, _sourceClient, secondsSinceEpoch, unit.Key);
                                }
                                catch (Exception ex)
                                {
                                    _log.WriteLine($"{_syncBackPrefix}Exception in oplog count Task.Run for {unit.Key}: {ex}", LogType.Error);
                                    oplogSuccess = false; // Set to false on exception
                                }
                            });
                            if (!oplogSuccess)
                                break;
                        }
                    }
                }
            }
        }

        private async Task ProcessCollectionChangeStream(MigrationUnit mu, bool IsCSProcessingRun = false, int seconds = 0)
        {
            string collectionKey = $"{mu.DatabaseName}.{mu.CollectionName}";
            _log.WriteLine($"{_syncBackPrefix}ProcessCollectionChangeStream started for {collectionKey} - IsCSProcessingRun: {IsCSProcessingRun}, Seconds: {seconds}", LogType.Verbose);

            try
            {
                string databaseName = mu.DatabaseName;
                string collectionName = mu.CollectionName;

                IMongoDatabase sourceDb;
                IMongoDatabase targetDb;

                IMongoCollection<BsonDocument>? sourceCollection = null;
                IMongoCollection<BsonDocument>? targetCollection = null;

                if (!_syncBack)
                {
                    sourceDb = _sourceClient.GetDatabase(databaseName);
                    sourceCollection = sourceDb.GetCollection<BsonDocument>(collectionName);

                    if (!_job.IsSimulatedRun)
                    {
                        targetDb = _targetClient.GetDatabase(databaseName);
                        targetCollection = targetDb.GetCollection<BsonDocument>(collectionName);
                    }
                }
                else
                {
                    // For sync back, we use the source collection as the target and vice versa
                    targetDb = _sourceClient.GetDatabase(databaseName);
                    targetCollection = targetDb.GetCollection<BsonDocument>(collectionName);

                    sourceDb = _targetClient.GetDatabase(databaseName);
                    sourceCollection = sourceDb.GetCollection<BsonDocument>(collectionName);
                }

                try
                {
                    // Calculate seconds first so we can use it in ChangeStreamOptions
                    if (seconds == 0)
                        seconds = GetBatchDurationInSeconds(.5f); //get seconds from config or use default

                    // MaxAwaitTime should be shorter than the cancellation timeout to allow proper cleanup
                    // Use 80% of the total duration to ensure cursor returns before cancellation
                    int maxAwaitSeconds = Math.Max(5, (int)(seconds * 0.8));
                    _log.WriteLine($"{_syncBackPrefix}ChangeStream timing - TotalDuration: {seconds}s, MaxAwaitTime: {maxAwaitSeconds}s for {collectionKey}", LogType.Debug);

                    // Default options; will be overridden based on resume strategy
                    ChangeStreamOptions options = new ChangeStreamOptions { BatchSize = 500, FullDocument = ChangeStreamFullDocumentOption.UpdateLookup, MaxAwaitTime = TimeSpan.FromSeconds(maxAwaitSeconds) };

                    DateTime startedOn;
                    DateTime timeStamp;
                    string resumeToken = string.Empty;
                    string? version = string.Empty;

                    if (!_syncBack)
                    {
                        timeStamp = mu.CursorUtcTimestamp;
                        resumeToken = mu.ResumeToken ?? string.Empty;
                        version = _job.SourceServerVersion;
                        if (mu.ChangeStreamStartedOn.HasValue)
                        {
                            startedOn = mu.ChangeStreamStartedOn.Value;
                        }
                        else
                        {
                            startedOn = DateTime.MinValue; // Example default value
                        }
                    }
                    else
                    {
                        timeStamp = mu.SyncBackCursorUtcTimestamp;
                        resumeToken = mu.SyncBackResumeToken ?? string.Empty;
                        version = "8"; //hard code for target
                        if (mu.SyncBackChangeStreamStartedOn.HasValue)
                        {
                            startedOn = mu.SyncBackChangeStreamStartedOn.Value;
                        }
                        else
                        {
                            startedOn = DateTime.MinValue; // Example default value
                        }
                    }

                    if (!mu.InitialDocumenReplayed && !_job.IsSimulatedRun && !_job.AggresiveChangeStream)
                    {
                        // Guard targetCollection for non-simulated runs
                        if (targetCollection == null)
                        {
                            var targetDb2 = _targetClient.GetDatabase(databaseName);
                            targetCollection = targetDb2.GetCollection<BsonDocument>(collectionName);
                        }
                        if (AutoReplayFirstChangeInResumeToken(mu.ResumeDocumentId, mu.ResumeTokenOperation, sourceCollection!, targetCollection!, mu))
                        {
                            // If the first change was replayed, we can proceed
                            mu.InitialDocumenReplayed = true;
                            _jobList?.Save();
                        }
                        else
                        {
                            _log.WriteLine($"{_syncBackPrefix}Failed to replay the first change for {sourceCollection!.CollectionNamespace}. Skipping change stream processing for this collection.", LogType.Error);
                            throw new Exception($"Failed to replay the first change for {sourceCollection!.CollectionNamespace}. Skipping change stream processing for this collection.");
                        }
                    }

                    if (timeStamp > DateTime.MinValue && !mu.ResetChangeStream && resumeToken == null && !(_job.JobType == JobType.RUOptimizedCopy && !_job.ProcessingSyncBack)) //skip CursorUtcTimestamp if its reset 
                    {
                        var bsonTimestamp = MongoHelper.ConvertToBsonTimestamp(timeStamp.ToLocalTime());
                        options = new ChangeStreamOptions { BatchSize = 500, FullDocument = ChangeStreamFullDocumentOption.UpdateLookup, StartAtOperationTime = bsonTimestamp, MaxAwaitTime = TimeSpan.FromSeconds(maxAwaitSeconds) };
                        _log.WriteLine($"{_syncBackPrefix}Resume strategy: StartAtOperationTime - Timestamp: {timeStamp} for {collectionKey}", LogType.Verbose);
                    }
                    else if (!string.IsNullOrEmpty(resumeToken) && !mu.ResetChangeStream) //skip resume token if its reset, both version  having resume token
                    {
                        options = new ChangeStreamOptions { BatchSize = 500, FullDocument = ChangeStreamFullDocumentOption.UpdateLookup, ResumeAfter = BsonDocument.Parse(resumeToken), MaxAwaitTime = TimeSpan.FromSeconds(maxAwaitSeconds) };
                        _log.WriteLine($"{_syncBackPrefix}Resume strategy: ResumeAfter token for {collectionKey}", LogType.Verbose);
                    }
                    else if (string.IsNullOrEmpty(resumeToken) && version.StartsWith("3")) //for Mongo 3.6
                    {
                        options = new ChangeStreamOptions { BatchSize = 500, FullDocument = ChangeStreamFullDocumentOption.UpdateLookup, MaxAwaitTime = TimeSpan.FromSeconds(maxAwaitSeconds) };
                        _log.WriteLine($"{_syncBackPrefix}Resume strategy: No resume (MongoDB 3.x) for {collectionKey}", LogType.Verbose);
                    }
                    else if (startedOn > DateTime.MinValue && !version.StartsWith("3"))  //newer version
                    {
                        var bsonTimestamp = MongoHelper.ConvertToBsonTimestamp((DateTime)startedOn);
                        options = new ChangeStreamOptions { BatchSize = 500, FullDocument = ChangeStreamFullDocumentOption.UpdateLookup, StartAtOperationTime = bsonTimestamp, MaxAwaitTime = TimeSpan.FromSeconds(maxAwaitSeconds) };
                        _log.WriteLine($"{_syncBackPrefix}Resume strategy: StartAtOperationTime from ChangeStreamStartedOn - StartedOn: {startedOn} for {collectionKey}", LogType.Verbose);
                        if (mu.ResetChangeStream)
                        {
                            ResetCounters(mu);
                            _log.WriteLine($"{_syncBackPrefix}Counters reset for {collectionKey}", LogType.Debug);
                        }

                        mu.ResetChangeStream = false; //reset the start time after setting resume token
                    }

                    using var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(seconds));
                    CancellationToken cancellationToken = cancellationTokenSource.Token;

                    _log.ShowInMonitor($"{_syncBackPrefix}Monitoring change stream with new batch for {sourceCollection!.CollectionNamespace}. Batch Duration {seconds} seconds");

                    // In simulated runs, use source collection as a placeholder to avoid null target warnings
                    if (_job.IsSimulatedRun && targetCollection == null)
                    {
                        targetCollection = sourceCollection;
                    }

                    await WatchCollection(mu, options, sourceCollection!, targetCollection!, cancellationToken, seconds);
                }
                catch (OperationCanceledException ex)
                {
                    _log.WriteLine($"{_syncBackPrefix}OperationCanceledException in ProcessCollectionChangeStream for {sourceCollection!.CollectionNamespace}: {ex.Message}", LogType.Info);
                }
                catch (MongoCommandException ex) when (ex.ToString().Contains("Resume of change stream was not possible"))
                {
                    // Handle other potential exceptions
                    _log.WriteLine($"{_syncBackPrefix}Oplog is full. Error processing change stream for {sourceCollection.CollectionNamespace}. Details: {ex}", LogType.Error);
                    _log.ShowInMonitor($"{_syncBackPrefix}Oplog is full. Error processing change stream for {sourceCollection.CollectionNamespace}. Details: {ex}");
                }
                catch (MongoCommandException ex) when (ex.Message.Contains("Expired resume token") || ex.Message.Contains("cursor"))
                {
                    _log.WriteLine($"{_syncBackPrefix}Resume token has expired or cursor is invalid for {sourceCollection.CollectionNamespace}.", LogType.Error);
                    _log.ShowInMonitor($"{_syncBackPrefix}Resume token has expired or cursor is invalid for {sourceCollection.CollectionNamespace}.");
                }
            }
            catch (Exception ex)
            {
                _log.WriteLine($"{_syncBackPrefix}Error processing change stream for {mu.DatabaseName}.{mu.CollectionName}. Details: {ex}", LogType.Error);
            }
        }

        private async Task WatchCollection(MigrationUnit mu, ChangeStreamOptions options, IMongoCollection<BsonDocument> sourceCollection, IMongoCollection<BsonDocument> targetCollection, CancellationToken cancellationToken, int seconds)
        {
            string collectionKey = $"{mu.DatabaseName}.{mu.CollectionName}";
            _log.WriteLine($"{_syncBackPrefix}WatchCollection started for {collectionKey} - Duration: {seconds}s", LogType.Verbose);

            bool isVCore = (_syncBack ? _job.TargetEndpoint : _job.SourceEndpoint)
                .Contains("mongocluster.cosmos.azure.com", StringComparison.OrdinalIgnoreCase);

            long counter = 0;
            BsonDocument userFilterDoc = new BsonDocument();

            userFilterDoc = MongoHelper.GetFilterDoc(mu.UserFilter);

            ChangeStreamDocuments changeStreamDocuments = new ChangeStreamDocuments();
            bool shouldProcessFinalBatch = true; // Flag to control finally block execution

            try
            {

                List<BsonDocument> pipeline;
                if (_job.JobType == JobType.RUOptimizedCopy)
                {
                    pipeline = new List<BsonDocument>()
                    {
                        new BsonDocument("$match", new BsonDocument("operationType",
                            new BsonDocument("$in", new BsonArray { "insert", "update", "replace", "delete" }))),
                        new BsonDocument("$project", new BsonDocument
                        {
                            { "operationType", 1 },
                            { "_id", 1 },
                            { "fullDocument", 1 },
                            { "ns", 1 },
                            { "documentKey", 1 }
                        })
                    };
                }
                else
                {
                    pipeline = new List<BsonDocument>();
                }

                var pipelineArray = pipeline.ToArray();

                // OOM PREVENTION: Check memory before creating Watch cursor to prevent crash during allocation
                if (IsMemoryExhausted(out long memMB, out long memMaxMB, out double memPercent))
                {
                    _log.ShowInMonitor($"{_syncBackPrefix}Memory pressure HIGH before Watch() - {memMB}MB / {memMaxMB}MB ({memPercent:F1}%) - waiting for recovery");
                    
                    // Force GC and wait for memory recovery before attempting Watch()
                    GC.Collect(2, GCCollectionMode.Aggressive, blocking: true, compacting: true);
                    await Task.Delay(2000); // Give GC time to complete
                    
                    await WaitForMemoryRecoveryAsync(collectionKey);
                    
                }

                // Watch timeout should be longer than batch duration to allow full batch processing
                // Add 30 second buffer to prevent premature timeout when waiting for changes
                int watchTimeoutSeconds = seconds + 30;
                _log.WriteLine($"{_syncBackPrefix}Watch task timeout set to {watchTimeoutSeconds}s for {collectionKey}", LogType.Verbose);

                // Create a separate CTS for the watch task that we can cancel if it times out
                using var watchCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                var watchToken = watchCts.Token;

                
                var watchTask = Task.Run(() =>
                {
                    try
                    {
                        
                        // Add periodic heartbeat logs to detect if Watch() hangs
                        using var watchCts = new CancellationTokenSource();
                        var heartbeatTask = Task.Run(async () =>
                        {
                            int seconds = 0;
                            while (!watchCts.Token.IsCancellationRequested)
                            {
                                await Task.Delay(5000, watchCts.Token);
                                seconds += 5;
                            }
                        });
                        
                        var cursor = sourceCollection.Watch<ChangeStreamDocument<BsonDocument>>(pipelineArray, options, watchToken);
                        
                        watchCts.Cancel();
                        try { heartbeatTask.Wait(1000); } catch { }
                        
                        return cursor;
                    }
                    catch (OperationCanceledException)
                    {
                        throw; // Re-throw to let the timeout handler deal with it
                    }
                    catch (Exception ex)
                    {
                        _log.WriteLine($"{_syncBackPrefix}Exception in Watch Task.Run for {collectionKey}: {ex}", LogType.Error);
                        throw; // Re-throw to let the timeout handler deal with it
                    }
                }, watchToken);
                
                
                var watchTimeoutTask = Task.Delay(TimeSpan.FromSeconds(watchTimeoutSeconds), CancellationToken.None);

                var completedTask = await Task.WhenAny(watchTask, watchTimeoutTask);

                if (completedTask == watchTimeoutTask)
                {
                    _log.WriteLine($"{_syncBackPrefix}Watch task timed out after {watchTimeoutSeconds}s for {collectionKey}, cancelling watch task", LogType.Debug);
                    // Cancel the watch task to prevent it from running indefinitely
                    watchCts.Cancel();
                    
                    // Give it a moment to cancel gracefully
                    try
                    {
                        await Task.WhenAny(watchTask, Task.Delay(2000));
                    }
                    catch (Exception ex)
                    {
                        _log.WriteLine($"{_syncBackPrefix}Exception while cancelling watch task for {collectionKey}: {ex.Message}", LogType.Debug);
                    }
                    
                    // Clear accumulated data to prevent memory leak
                    int clearedCount = changeStreamDocuments.DocsToBeInserted.Count + 
                                      changeStreamDocuments.DocsToBeUpdated.Count + 
                                      changeStreamDocuments.DocsToBeDeleted.Count;
                    _log.WriteLine($"{_syncBackPrefix}Clearing {clearedCount} accumulated documents due to watch timeout for {collectionKey}", LogType.Debug);
                    changeStreamDocuments.DocsToBeInserted.Clear();
                    changeStreamDocuments.DocsToBeUpdated.Clear();
                    changeStreamDocuments.DocsToBeDeleted.Clear();
                    
                    // Update counters before early return
                    mu.CSUpdatesInLastBatch = 0;
                    mu.CSNormalizedUpdatesInLastBatch = 0;
                    _jobList?.Save();
                    shouldProcessFinalBatch = false; // Skip finally block processing
                    
                    _log.WriteLine($"{_syncBackPrefix}Watch() timed out after {watchTimeoutSeconds}s for {collectionKey} - will retry in next batch", LogType.Debug);
                    return; // Skip this collection, will retry in next batch
                }
                IChangeStreamCursor<ChangeStreamDocument<BsonDocument>> cursor;
                try
                {
                    cursor = await watchTask;
                }
                catch (Exception ex)
                {
                    _log.WriteLine($"{_syncBackPrefix}Failed to create change stream cursor for {collectionKey}: {ex}", LogType.Verbose);
                    
                    
                    // Clear accumulated data to prevent memory leak
                    changeStreamDocuments.DocsToBeInserted.Clear();
                    changeStreamDocuments.DocsToBeUpdated.Clear();
                    changeStreamDocuments.DocsToBeDeleted.Clear();
                    
                    // Update counters before early return
                    mu.CSUpdatesInLastBatch = 0;
                    mu.CSNormalizedUpdatesInLastBatch = 0;
                    _jobList?.Save();
                    shouldProcessFinalBatch = false; // Skip finally block processing
                    return; // Skip this collection, will retry in next batch
                }

                using (cursor)
                {
                    string lastProcessedToken = string.Empty;
                    _log.WriteLine($"{_syncBackPrefix}Change stream cursor created successfully for {collectionKey}, starting enumeration", LogType.Verbose);

                    if (_job.SourceServerVersion.StartsWith("3"))
                    {
                        foreach (var change in cursor.ToEnumerable(cancellationToken))
                        {
                            if (cancellationToken.IsCancellationRequested || ExecutionCancelled)
                            {
                                _log.WriteLine($"{_syncBackPrefix}Change stream processing cancelled for {sourceCollection!.CollectionNamespace}", LogType.Info);
                                break; // Exit loop, let finally block handle cleanup
                            }

                            lastProcessedToken = string.Empty;
                            _resumeTokenCache.TryGetValue($"{sourceCollection!.CollectionNamespace}", out string? token1);
                            lastProcessedToken = token1 ?? string.Empty;

                            if (lastProcessedToken == change.ResumeToken.ToJson())
                            {
                                mu.CSUpdatesInLastBatch = 0;
                                mu.CSNormalizedUpdatesInLastBatch = 0;
                                return; // Skip processing if the event has already been processed
                            }

                            try
                            {
                                if (!ProcessCursor(change, cursor, targetCollection, sourceCollection.CollectionNamespace.ToString(), mu, changeStreamDocuments, ref counter, userFilterDoc))
                                    break; // Exit loop on error, let finally block handle cleanup
                            }
                            catch (Exception ex)
                            {
                                _log.WriteLine($"{_syncBackPrefix}Exception in ProcessCursor for {collectionKey}: {ex.Message}", LogType.Debug);
                                break; // Exit loop on exception, let finally block handle cleanup
                            }

                            // MEMORY SAFETY: Flush accumulated changes periodically to prevent OOM
                            int totalAccumulated = changeStreamDocuments.DocsToBeInserted.Count +
                                                  changeStreamDocuments.DocsToBeUpdated.Count +
                                                  changeStreamDocuments.DocsToBeDeleted.Count;

                            if (totalAccumulated >= 1000) // Reduced from 5000 to prevent OOM with concurrent collections
                            {
                                _log.WriteLine($"{_syncBackPrefix}Memory safety threshold reached - {totalAccumulated} documents accumulated for {collectionKey}", LogType.Debug);
                                // BACKPRESSURE: Check global pending writes before adding more load
                                int pending = GetGlobalPendingWriteCount();
                                while (pending >= MAX_GLOBAL_PENDING_WRITES)
                                {
                                    _log.WriteLine($"{_syncBackPrefix}Backpressure applied - Pending writes: {pending}/{MAX_GLOBAL_PENDING_WRITES} for {collectionKey}", LogType.Verbose);
                                    await WaitWithExponentialBackoffAsync(pending, sourceCollection.CollectionNamespace.ToString());

                                    // Check if memory recovered after wait
                                    if (IsMemoryExhausted(out long currentMB, out long maxMB, out double percent))
                                    {
                                        _log.ShowInMonitor($"{_syncBackPrefix}Memory pressure detected: {currentMB}MB / {maxMB}MB ({percent:F1}%)");
                                        await WaitForMemoryRecoveryAsync(sourceCollection.CollectionNamespace.ToString());
                                    }

                                    pending = GetGlobalPendingWriteCount();
                                }

                                _log.WriteLine($"{_syncBackPrefix}Memory safety flush: Processing {totalAccumulated} accumulated changes for {sourceCollection.CollectionNamespace}", LogType.Info);

                                IncrementGlobalPendingWrites();
                                try
                                {
                                    // Flush accumulated changes
                                    await BulkProcessChangesAsync(
                                        mu,
                                        targetCollection,
                                        insertEvents: changeStreamDocuments.DocsToBeInserted,
                                        updateEvents: changeStreamDocuments.DocsToBeUpdated,
                                        deleteEvents: changeStreamDocuments.DocsToBeDeleted);

                                    // Update resume token after successful flush
                                    if (!string.IsNullOrEmpty(changeStreamDocuments.LatestResumeToken))
                                    {
                                        if (!_syncBack)
                                        {
                                            mu.ResumeToken = changeStreamDocuments.LatestResumeToken;
                                            mu.CursorUtcTimestamp = changeStreamDocuments.LatestTimestamp;
                                        }
                                        else
                                        {
                                            mu.SyncBackResumeToken = changeStreamDocuments.LatestResumeToken;
                                            mu.SyncBackCursorUtcTimestamp = changeStreamDocuments.LatestTimestamp;
                                        }
                                        _resumeTokenCache[$"{sourceCollection.CollectionNamespace}"] = changeStreamDocuments.LatestResumeToken;
                                    }

                                    // Clear collections to free memory
                                    changeStreamDocuments.DocsToBeInserted.Clear();
                                    changeStreamDocuments.DocsToBeUpdated.Clear();
                                    changeStreamDocuments.DocsToBeDeleted.Clear();

                                    _log.WriteLine($"{_syncBackPrefix}Memory safety flush completed, memory released for {sourceCollection.CollectionNamespace}", LogType.Debug);
                                }
                                finally
                                {
                                    DecrementGlobalPendingWrites();
                                }
                            }
                        }
                    }
                    else
                    {
                        _log.WriteLine($"{_syncBackPrefix}Processing MongoDB 4.0+ change stream with MoveNext pattern for {collectionKey}", LogType.Verbose);
                        // Watchdog: Track when MoveNext was last called to detect hangs
                        DateTime lastMoveNextCall = DateTime.UtcNow;
                        int moveNextTimeoutSeconds = seconds + 10; // Add 10 second buffer beyond batch duration
                        _log.WriteLine($"{_syncBackPrefix}Watchdog enabled with {moveNextTimeoutSeconds}s timeout for MoveNext calls", LogType.Debug);

                        while (true)
                        {
                            if (cancellationToken.IsCancellationRequested || ExecutionCancelled)
                            {
                                _log.WriteLine($"{_syncBackPrefix}Change stream processing cancelled for {sourceCollection!.CollectionNamespace}", LogType.Debug);
                                break; // Exit loop, let finally block handle cleanup
                            }

                            // Watchdog: Check if MoveNext is taking too long (stuck)
                            if ((DateTime.UtcNow - lastMoveNextCall).TotalSeconds > moveNextTimeoutSeconds)
                            {
                                _log.WriteLine($"{_syncBackPrefix}Watchdog timeout: MoveNext exceeded {moveNextTimeoutSeconds}s for {sourceCollection!.CollectionNamespace}. Breaking out to prevent hang.", LogType.Warning);
                                break; // Exit the while loop, batch will complete in finally block
                            }

                            bool hasMore;
                            try
                            {
                                lastMoveNextCall = DateTime.UtcNow;
                                hasMore = cursor.MoveNext(cancellationToken);
                            }
                            catch (OperationCanceledException)
                            {
                                // Expected when cancellationToken times out
                                _log.WriteLine($"{_syncBackPrefix}MoveNext cancelled by timeout for {sourceCollection!.CollectionNamespace} - completing batch", LogType.Debug);
                                break; // Exit loop, process accumulated batch
                            }

                            if (!hasMore)
                            {
                                // No more data available, exit loop
                                break;
                            }

                            int changeCount = 0;
                            foreach (var change in cursor.Current)
                            {
                                changeCount++;
                                
                                if (cancellationToken.IsCancellationRequested || ExecutionCancelled)
                                {
                                    _log.WriteLine($"{_syncBackPrefix}Change stream processing cancelled for {sourceCollection!.CollectionNamespace}", LogType.Debug);
                                    break; // Exit inner loop, outer loop will also break
                                }

                                lastProcessedToken = string.Empty;
                                _resumeTokenCache.TryGetValue($"{sourceCollection!.CollectionNamespace}", out string? token2);
                                lastProcessedToken = token2 ?? string.Empty;

                                if (lastProcessedToken == change.ResumeToken.ToJson() && _job.JobType != JobType.RUOptimizedCopy)
                                {
                                    mu.CSUpdatesInLastBatch = 0;
                                    mu.CSNormalizedUpdatesInLastBatch = 0;
                                    return; // Skip processing if the event has already been processed
                                }

                                try
                                {
                                    if (!ProcessCursor(change, cursor, targetCollection, sourceCollection.CollectionNamespace.ToString(), mu, changeStreamDocuments, ref counter, userFilterDoc))
                                        break; // Exit loop on error, let finally block handle cleanup
                                }
                            catch (Exception ex)
                            {
                                _log.WriteLine($"{_syncBackPrefix}Exception in ProcessCursor for {collectionKey}: {ex.Message}", LogType.Error);
                                break; // Exit loop on exception, let finally block handle cleanup
                            }                                // MEMORY SAFETY: Flush accumulated changes periodically to prevent OOM
                                // Check every 1000 documents to avoid excessive memory buildup (reduced from 5000)
                                int totalAccumulated = changeStreamDocuments.DocsToBeInserted.Count +
                                                      changeStreamDocuments.DocsToBeUpdated.Count +
                                                      changeStreamDocuments.DocsToBeDeleted.Count;

                                if (totalAccumulated >= 1000) // Reduced from 5000 to prevent OOM with concurrent collections
                                {
                                    _log.WriteLine($"{_syncBackPrefix}Memory safety threshold reached - {totalAccumulated} documents accumulated for {collectionKey}", LogType.Debug);
                                    // BACKPRESSURE: Check global pending writes before adding more load
                                    int pending = GetGlobalPendingWriteCount();

                                    while (pending >= MAX_GLOBAL_PENDING_WRITES)
                                    {
                                        _log.WriteLine($"{_syncBackPrefix}Backpressure applied - Pending writes: {pending}/{MAX_GLOBAL_PENDING_WRITES} for {collectionKey}", LogType.Verbose);
                                        await WaitWithExponentialBackoffAsync(pending, sourceCollection.CollectionNamespace.ToString());

                                        // Check if memory recovered after wait
                                        if (IsMemoryExhausted(out long currentMB, out long maxMB, out double percent))
                                        {
                                            _log.ShowInMonitor($"{_syncBackPrefix}Memory pressure detected: {currentMB}MB / {maxMB}MB ({percent:F1}%)");
                                            await WaitForMemoryRecoveryAsync(sourceCollection.CollectionNamespace.ToString());
                                        }

                                        pending = GetGlobalPendingWriteCount();
                                    }

                                    _log.WriteLine($"{_syncBackPrefix}Memory safety flush: Processing {totalAccumulated} accumulated changes for {sourceCollection.CollectionNamespace}", LogType.Info);

                                    IncrementGlobalPendingWrites();
                                    try
                                    {
                                        // Flush accumulated changes
                                        await BulkProcessChangesAsync(
                                            mu,
                                            targetCollection,
                                            insertEvents: changeStreamDocuments.DocsToBeInserted,
                                            updateEvents: changeStreamDocuments.DocsToBeUpdated,
                                            deleteEvents: changeStreamDocuments.DocsToBeDeleted);

                                        // Update resume token after successful flush
                                        if (!string.IsNullOrEmpty(changeStreamDocuments.LatestResumeToken))
                                        {
                                            if (!_syncBack)
                                            {
                                                mu.ResumeToken = changeStreamDocuments.LatestResumeToken;
                                                mu.CursorUtcTimestamp = changeStreamDocuments.LatestTimestamp;
                                            }
                                            else
                                            {
                                                mu.SyncBackResumeToken = changeStreamDocuments.LatestResumeToken;
                                                mu.SyncBackCursorUtcTimestamp = changeStreamDocuments.LatestTimestamp;
                                            }
                                            _resumeTokenCache[$"{sourceCollection.CollectionNamespace}"] = changeStreamDocuments.LatestResumeToken;
                                        }

                                        // Clear collections to free memory
                                        changeStreamDocuments.DocsToBeInserted.Clear();
                                        changeStreamDocuments.DocsToBeUpdated.Clear();
                                        changeStreamDocuments.DocsToBeDeleted.Clear();

                                        _log.WriteLine($"{_syncBackPrefix}Memory safety flush completed, memory released for {sourceCollection.CollectionNamespace}", LogType.Info);
                                    }
                                    finally
                                    {
                                        DecrementGlobalPendingWrites();
                                    }
                                }
                            }

                            if (ExecutionCancelled)
                            {
                                break; // Exit loop, let finally block handle cleanup
                            }
                        }
                    } // End of else (non-MongoDB 3.x)
                } // End of using (cursor)
            } // End of main try block
            catch (OperationCanceledException ex)
            {
                _log.WriteLine($"{_syncBackPrefix}OperationCanceledException in WatchCollection for {sourceCollection!.CollectionNamespace}: {ex.Message}", LogType.Info);
            }
            catch (Exception ex)
            {
                _log.WriteLine($"{_syncBackPrefix}Exception in WatchCollection for {sourceCollection!.CollectionNamespace}: {ex.Message}", LogType.Error);
                throw;
            }
            finally
            {
                _log.WriteLine($"{_syncBackPrefix}WatchCollection finally block - ShouldProcessFinalBatch: {shouldProcessFinalBatch} for {collectionKey}", LogType.Verbose);
                // Only process final batch if we didn't exit early (e.g., due to timeout or cursor creation failure)
                if (shouldProcessFinalBatch)
                {
                    try
                    {
                        int totalChanges = changeStreamDocuments.DocsToBeInserted.Count +
                                          changeStreamDocuments.DocsToBeUpdated.Count +
                                          changeStreamDocuments.DocsToBeDeleted.Count;

                    if (totalChanges > 0)
                    {
                        // NO BACKPRESSURE HERE: This finally block REDUCES memory pressure by writing accumulated changes
                        // Blocking writes here would prevent the very thing that frees up memory
                        // Backpressure is applied at read/accumulation points (periodic flushes) only
                        _log.ShowInMonitor($"{_syncBackPrefix}Processing batch for {sourceCollection.CollectionNamespace}: {totalChanges} changes (I:{changeStreamDocuments.DocsToBeInserted.Count}, U:{changeStreamDocuments.DocsToBeUpdated.Count}, D:{changeStreamDocuments.DocsToBeDeleted.Count})");
                        _log.WriteLine($"{_syncBackPrefix}Final batch processing - Total: {totalChanges}, Inserts: {changeStreamDocuments.DocsToBeInserted.Count}, Updates: {changeStreamDocuments.DocsToBeUpdated.Count}, Deletes: {changeStreamDocuments.DocsToBeDeleted.Count}", LogType.Debug);
                    }

                    IncrementGlobalPendingWrites();

                    try
                    {
                        // Process all pending changes in the batch - AWAIT properly to ensure completion before next batch
                        await BulkProcessChangesAsync(
                            mu,
                            targetCollection,
                            insertEvents: changeStreamDocuments.DocsToBeInserted,
                            updateEvents: changeStreamDocuments.DocsToBeUpdated,
                            deleteEvents: changeStreamDocuments.DocsToBeDeleted);

                        if (totalChanges > 0)
                        {
                            _log.ShowInMonitor($"{_syncBackPrefix}Batch processing completed for {sourceCollection.CollectionNamespace}: {totalChanges} changes written successfully");
                        }
                        
                        // CRITICAL: Clear the lists after successful processing to prevent memory leak
                        changeStreamDocuments.DocsToBeInserted.Clear();
                        changeStreamDocuments.DocsToBeUpdated.Clear();
                        changeStreamDocuments.DocsToBeDeleted.Clear();
                    }
                    finally
                    {
                        DecrementGlobalPendingWrites();
                    }

                    // CRITICAL: Only update resume token and timestamp AFTER successful batch write
                    // This ensures we can recover from the last successful checkpoint on failure
                    if (!string.IsNullOrEmpty(changeStreamDocuments.LatestResumeToken))
                    {
                        _log.WriteLine($"{_syncBackPrefix}Updating resume token after successful batch - Timestamp: {changeStreamDocuments.LatestTimestamp} for {collectionKey}", LogType.Verbose);
                        if (!_syncBack)
                        {
                            mu.ResumeToken = changeStreamDocuments.LatestResumeToken;
                            mu.CursorUtcTimestamp = changeStreamDocuments.LatestTimestamp;
                        }
                        else
                        {
                            mu.SyncBackResumeToken = changeStreamDocuments.LatestResumeToken;
                            mu.SyncBackCursorUtcTimestamp = changeStreamDocuments.LatestTimestamp;
                        }

                        _resumeTokenCache[$"{sourceCollection.CollectionNamespace}"] = changeStreamDocuments.LatestResumeToken;
                    }

                    mu.CSUpdatesInLastBatch = counter;
                    mu.CSNormalizedUpdatesInLastBatch = (long)(counter / (mu.CSLastBatchDurationSeconds > 0 ? mu.CSLastBatchDurationSeconds : 1));
                    _log.WriteLine($"{_syncBackPrefix}Batch counters updated - CSUpdatesInLastBatch: {counter}, CSNormalizedUpdatesInLastBatch: {mu.CSNormalizedUpdatesInLastBatch} for {collectionKey}", LogType.Verbose);
                    _jobList?.Save();

                    if (counter > 0)
                    {
                        _log.ShowInMonitor($"{_syncBackPrefix}Watch cycle completed for {sourceCollection.CollectionNamespace}: {counter} events processed in batch");
                    }
                }
                catch (Exception ex)
                {
                    _log.ShowInMonitor($"{_syncBackPrefix}ERROR processing batch for {sourceCollection.CollectionNamespace}: {ex.Message}");
                    _log.WriteLine($"{_syncBackPrefix}Error processing changes in batch for {sourceCollection.CollectionNamespace}. Details: {ex}", LogType.Error);
                    // On failure, resume token is NOT updated - we will resume from the last successful checkpoint
                }
                } // End of if (shouldProcessFinalBatch)
            }
        }

        // This method retrieves the event associated with the ResumeToken
        private bool AutoReplayFirstChangeInResumeToken(string? documentId, ChangeStreamOperationType opType, IMongoCollection<BsonDocument> sourceCollection, IMongoCollection<BsonDocument> targetCollection, MigrationUnit mu)
        {
            if (documentId == null || string.IsNullOrEmpty(documentId))
            {
                _log.WriteLine($"Auto replay is empty for {sourceCollection.CollectionNamespace}.", LogType.Info);
                return true; // Skip if no document ID is provided
            }
            else
            {
                _log.WriteLine($"Auto replay for {opType} operation with _id {documentId} in {sourceCollection.CollectionNamespace}.", LogType.Info);
            }

            var bsonDoc = BsonDocument.Parse(documentId);
            var filter = MongoHelper.BuildFilterFromDocumentKey(bsonDoc);
            var result = sourceCollection.Find(filter).FirstOrDefault(); // Retrieve the document for the resume token

            try
            {
                IncrementEventCounter(mu, opType);
                switch (opType)
                {
                    case ChangeStreamOperationType.Insert:
                        if (result == null || result.IsBsonNull)
                        {
                            _log.WriteLine($"No document found for insert operation with _id {documentId} in {sourceCollection.CollectionNamespace}. Skipping insert.", LogType.Warning);
                            return true; // Skip if no document found
                        }
                        targetCollection.InsertOne(result);
                        IncrementDocCounter(mu, opType);
                        return true;
                    case ChangeStreamOperationType.Update:
                    case ChangeStreamOperationType.Replace:
                        if (result == null || result.IsBsonNull)
                        {
                            _log.WriteLine($"Processing {opType} operation for {sourceCollection.CollectionNamespace} with _id {documentId}. No document found on source, deleting it from target.", LogType.Info);
                            var deleteTTLFilter = MongoHelper.BuildFilterFromDocumentKey(bsonDoc);
                            try
                            {
                                targetCollection.DeleteOne(deleteTTLFilter);
                                IncrementDocCounter(mu, ChangeStreamOperationType.Delete);
                            }
                            catch
                            { }
                            return true;
                        }
                        else
                        {
                            targetCollection.ReplaceOne(filter, result, new ReplaceOptions { IsUpsert = true });
                            IncrementDocCounter(mu, opType);
                            return true;
                        }
                    case ChangeStreamOperationType.Delete:
                        var deleteFilter = Builders<BsonDocument>.Filter.Eq("_id", documentId);
                        targetCollection.DeleteOne(deleteFilter);
                        IncrementDocCounter(mu, opType);
                        return true;
                    default:
                        _log.WriteLine($"Unhandled operation type: {opType}", LogType.Error);
                        return false;
                }
            }
            catch (MongoException mex) when (opType == ChangeStreamOperationType.Insert && mex.Message.Contains("DuplicateKey"))
            {
                // Ignore duplicate key errors for inserts, typically caused by reprocessing of the same change stream
                return true;
            }
            catch (Exception ex)
            {
                _log.WriteLine($"Error processing operation {opType} on {sourceCollection.CollectionNamespace} with _id {documentId}. Details: {ex}", LogType.Error);
                return false; // Return false to indicate failure in processing
            }
        }

        private bool ProcessCursor(ChangeStreamDocument<BsonDocument> change, IChangeStreamCursor<ChangeStreamDocument<BsonDocument>> cursor, IMongoCollection<BsonDocument> targetCollection, string collNameSpace, MigrationUnit mu, ChangeStreamDocuments changeStreamDocuments, ref long counter, BsonDocument userFilterDoc)
        {
            try
            {
                _log.WriteLine($"{_syncBackPrefix}ProcessCursor - OperationType: {change.OperationType}, DocumentKey: {change.DocumentKey?.ToJson() ?? "null"}, Counter: {counter + 1}", LogType.Verbose);
                //check if user filter condition is met
                if (change.OperationType != ChangeStreamOperationType.Delete)
                {
                    if (userFilterDoc.Elements.Count() > 0
                        && !MongoHelper.CheckForUserFilterMatch(change.FullDocument, userFilterDoc))
                        return true;
                }

                counter++;

                DateTime timeStamp = DateTime.MinValue;

                if (!_job.SourceServerVersion.StartsWith("3") && change.ClusterTime != null)
                {
                    timeStamp = MongoHelper.BsonTimestampToUtcDateTime(change.ClusterTime); // Convert BsonTimestamp to DateTime
                }
                else if (!_job.SourceServerVersion.StartsWith("3") && change.WallTime != null) //for 4.0 and above
                {
                    timeStamp = change.WallTime.Value; // Use WallTime for 4.0 and above
                }

                // Throttle UI updates to once per second to prevent Blazor rendering OOM
                string collectionKey = $"{mu.DatabaseName}.{mu.CollectionName}";
                DateTime now = DateTime.UtcNow;
                bool shouldUpdateUI = false;

                if (!_lastUIUpdateTime.TryGetValue(collectionKey, out DateTime lastUpdate) || 
                    (now - lastUpdate).TotalMilliseconds >= UI_UPDATE_INTERVAL_MS)
                {
                    _lastUIUpdateTime[collectionKey] = now;
                    shouldUpdateUI = true;
                }

                // Show on monitor only once per second, but always log to file
                if (shouldUpdateUI)
                {
                    if (timeStamp == DateTime.MinValue)
                        _log.ShowInMonitor($"{_syncBackPrefix}{change.OperationType} operation detected in {collNameSpace} for _id: {change.DocumentKey["_id"]}. Sequence in batch #{counter}");
                    else
                        _log.ShowInMonitor($"{_syncBackPrefix}{change.OperationType} operation detected in {collNameSpace} for _id: {change.DocumentKey["_id"]} with TS (UTC): {timeStamp}. Sequence in batch #{counter}");
                }
                

                ProcessChange(change, targetCollection, collNameSpace, changeStreamDocuments, _job.IsSimulatedRun, mu);

                // NOTE: Resume token and timestamp are NOT persisted here anymore
                // They will only be persisted after successful batch write completion
                // This ensures we can recover from the last successful checkpoint

                // Break if execution is canceled
                if (ExecutionCancelled)
                    return false;

                return true;
            }
            catch (Exception ex)
            {
                _log.WriteLine($"{_syncBackPrefix}Error processing cursor. Details: {ex}", LogType.Error);
                return false;
            }
        }

        private void ProcessChange(ChangeStreamDocument<BsonDocument> change, IMongoCollection<BsonDocument> targetCollection, string collNameSpace, ChangeStreamDocuments changeStreamDocuments, bool isWriteSimulated, MigrationUnit mu)
        {
            BsonValue idValue = BsonNull.Value;

            try
            {
                if (!change.DocumentKey.TryGetValue("_id", out idValue))
                {
                    _log.WriteLine($"{_syncBackPrefix}Error processing operation {change.OperationType} on {collNameSpace}. Change stream event is missing _id in DocumentKey.", LogType.Error);
                    return;
                }

                switch (change.OperationType)
                {
                    case ChangeStreamOperationType.Insert:
                        IncrementEventCounter(mu, change.OperationType);
                        // Accumulate inserts even in simulation mode so counters get updated
                        if (change.FullDocument != null && !change.FullDocument.IsBsonNull)
                            changeStreamDocuments.AddInsert(change);
                        break;
                    case ChangeStreamOperationType.Update:
                    case ChangeStreamOperationType.Replace:
                        IncrementEventCounter(mu, change.OperationType);
                        var filter = Builders<BsonDocument>.Filter.Eq("_id", idValue);
                        if (change.FullDocument == null || change.FullDocument.IsBsonNull)
                        {
                            // Skip actual delete operation in simulation mode
                            if (!isWriteSimulated)
                            {
                                _log.WriteLine($"{_syncBackPrefix}Processing {change.OperationType} operation for {collNameSpace} with _id {idValue}. No document found on source, deleting it from target.", LogType.Info);
                                var deleteTTLFilter = Builders<BsonDocument>.Filter.Eq("_id", idValue);
                                try
                                {
                                    targetCollection.DeleteOne(deleteTTLFilter);
                                    IncrementEventCounter(mu, ChangeStreamOperationType.Delete);
                                }
                                catch
                                { }
                            }
                        }
                        else
                        {
                            // Accumulate updates even in simulation mode so counters get updated
                            changeStreamDocuments.AddUpdate(change);
                        }
                        break;
                    case ChangeStreamOperationType.Delete:
                        IncrementEventCounter(mu, change.OperationType);
                        // Accumulate deletes even in simulation mode so counters get updated
                        changeStreamDocuments.AddDelete(change);
                        break;
                    default:
                        _log.WriteLine($"{_syncBackPrefix}Unhandled operation type: {change.OperationType}", LogType.Error);
                        break;
                }

                if (changeStreamDocuments.DocsToBeInserted.Count + changeStreamDocuments.DocsToBeUpdated.Count + changeStreamDocuments.DocsToBeDeleted.Count > _config.ChangeStreamMaxDocsInBatch)
                {
                    int batchSize = changeStreamDocuments.DocsToBeInserted.Count + changeStreamDocuments.DocsToBeUpdated.Count + changeStreamDocuments.DocsToBeDeleted.Count;
                    _log.WriteLine($"{_syncBackPrefix}Change stream max batch size exceeded - Flushing {batchSize} changes for {collNameSpace}", LogType.Debug);

                    // Process the changes in bulk if the batch size exceeds the limit
                    // Use Task.Run().Wait() to avoid deadlock - this is a controlled synchronous wait
                    Task.Run(async () =>
                    {
                        await BulkProcessChangesAsync(
                            mu,
                            targetCollection,
                            insertEvents: changeStreamDocuments.DocsToBeInserted,
                            updateEvents: changeStreamDocuments.DocsToBeUpdated,
                            deleteEvents: changeStreamDocuments.DocsToBeDeleted);
                    }).GetAwaiter().GetResult();

                    // CRITICAL: Only update resume token AFTER successful mid-batch flush
                    // This ensures we can recover from the last successful checkpoint on failure
                    if (!string.IsNullOrEmpty(changeStreamDocuments.LatestResumeToken))
                    {
                        _log.WriteLine($"{_syncBackPrefix}Mid-batch checkpoint - Resume token updated after successful flush for {collNameSpace}", LogType.Debug);
                        if (!_syncBack)
                        {
                            mu.ResumeToken = changeStreamDocuments.LatestResumeToken;
                            mu.CursorUtcTimestamp = changeStreamDocuments.LatestTimestamp;
                        }
                        else
                        {
                            mu.SyncBackResumeToken = changeStreamDocuments.LatestResumeToken;
                            mu.SyncBackCursorUtcTimestamp = changeStreamDocuments.LatestTimestamp;
                        }

                        _resumeTokenCache[$"{collNameSpace}"] = changeStreamDocuments.LatestResumeToken;

                        _log.WriteLine($"{_syncBackPrefix}Mid-batch checkpoint updated for {collNameSpace}: Resume token persisted after successful flush", LogType.Verbose);
                    }

                    _jobList?.Save();

                    // Clear the lists and metadata after successful processing
                    changeStreamDocuments.DocsToBeInserted.Clear();
                    changeStreamDocuments.DocsToBeUpdated.Clear();
                    changeStreamDocuments.DocsToBeDeleted.Clear();
                    changeStreamDocuments.ClearMetadata(); // Clear to start fresh for next batch
                }
            }
            catch (Exception ex)
            {
                _log.WriteLine($"{_syncBackPrefix}Error processing operation {change.OperationType} on {collNameSpace} with _id {idValue}. Details: {ex}", LogType.Error);
            }
        }

        /// <summary>
        /// Log memory statistics to help identify memory pressure and potential OOM issues
        /// </summary>
        private void LogMemoryStats(string context)
        {
            try
            {
                var gcMemory = GC.GetTotalMemory(false);
                var workingSet = Environment.WorkingSet;
                var gcMemoryMB = gcMemory / 1024 / 1024;
                var workingSetMB = workingSet / 1024 / 1024;
                
                _log.WriteLine($"{_syncBackPrefix}MEMORY [{context}] - GC: {gcMemoryMB}MB, WorkingSet: {workingSetMB}MB", LogType.Verbose);
                
                // Show warning in monitor if memory usage is high (over 1GB GC memory)
                if (gcMemoryMB > 1024)
                {
                    _log.ShowInMonitor($"{_syncBackPrefix}HIGH MEMORY WARNING [{context}] - GC: {gcMemoryMB}MB, WorkingSet: {workingSetMB}MB");
                }
            }
            catch (Exception ex)
            {
                // Memory logging itself can fail under extreme pressure
                _log.WriteLine($"{_syncBackPrefix}Failed to log memory stats [{context}]: {ex.Message}", LogType.Debug);
            }
        }
    }
}
