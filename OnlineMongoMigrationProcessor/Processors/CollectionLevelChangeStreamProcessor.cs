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

            bool isVCore = (_syncBack ? _job.TargetEndpoint : _job.SourceEndpoint)
                .Contains("mongocluster.cosmos.azure.com", StringComparison.OrdinalIgnoreCase);

            int index = 0;

            // Get the latest sorted keys
            var sortedKeys = _migrationUnitsToProcess
                .OrderByDescending(kvp => kvp.Value.CSNormalizedUpdatesInLastBatch)
                .Select(kvp => kvp.Key)
                .ToList();

            _log.WriteLine($"{_syncBackPrefix}Starting collection-level change stream processing for {sortedKeys.Count} collection(s). Each round-robin batch will process {Math.Min(_concurrentProcessors, sortedKeys.Count)} collections. Max duration per batch {_processorRunMaxDurationInSec} seconds.");

            long loops = 0;
            bool oplogSuccess = true;

            //_log.WriteLine($"{_syncBackPrefix}About to enter main loop: token.IsCancellationRequested={token.IsCancellationRequested}, ExecutionCancelled={ExecutionCancelled}, sortedKeys.Count={sortedKeys.Count}");
            _log.WriteLine($"{_syncBackPrefix}[DEBUG] About to enter main loop: token.IsCancellationRequested={token.IsCancellationRequested}, ExecutionCancelled={ExecutionCancelled}, sortedKeys.Count={sortedKeys.Count}", LogType.Debug);

            while (!token.IsCancellationRequested && !ExecutionCancelled)
            {
                _log.WriteLine($"{_syncBackPrefix}[DEBUG] Starting new round {loops + 1}, total collections: {sortedKeys.Count}", LogType.Debug);
                var totalKeys = sortedKeys.Count;


                while (index < totalKeys && !token.IsCancellationRequested && !ExecutionCancelled)
                {
                    _log.WriteLine($"{_syncBackPrefix}[DEBUG] Inner loop iteration: index={index}, totalKeys={totalKeys}, cancellation={token.IsCancellationRequested}, executionCancelled={ExecutionCancelled}", LogType.Debug);

                    var tasks = new List<Task>();
                    var collectionProcessed = new List<string>();

                    // Determine the batch
                    var batchKeys = sortedKeys.Skip(index).Take(_concurrentProcessors).ToList();
                    _log.WriteLine($"{_syncBackPrefix}Batch starting: index={index}, will process {batchKeys.Count} collections", LogType.Debug);
                    var batchUnits = batchKeys
                        .Select(k => _migrationUnitsToProcess.TryGetValue(k, out var unit) ? unit : null)
                        .Where(u => u != null)
                        .ToList();

                    //total of batchUnits.All(u => u.CSUpdatesInLastBatch)
                    long totalUpdatesInBatch = batchUnits.Sum(u => u.CSNormalizedUpdatesInLastBatch);

                    //total of  _migrationUnitsToProcess
                    long totalUpdatesInAll = _migrationUnitsToProcess.Sum(kvp => kvp.Value.CSNormalizedUpdatesInLastBatch);

                    float timeFactor = totalUpdatesInAll > 0 ? (float)totalUpdatesInBatch / totalUpdatesInAll : 1;

                    int seconds = GetBatchDurationInSeconds(timeFactor);
                    foreach (var key in batchKeys)
                    {
                        if (_migrationUnitsToProcess.TryGetValue(key, out var unit))
                        {
                            collectionProcessed.Add(key);
                            unit.CSLastBatchDurationSeconds = seconds; // Store the factor for each unit
                            // Don't pass token to Task.Run - each collection manages its own timeout via CancellationTokenSource inside ProcessCollectionChangeStream
                            string collectionName = key; // Capture for closure
                            var task = Task.Run(async () =>
                            {
                                try
                                {
                                    _log.WriteLine($"{_syncBackPrefix}[DEBUG] Task.Run starting for collection {collectionName}", LogType.Debug);
                                    await ProcessCollectionChangeStream(unit, true, seconds);
                                    _log.WriteLine($"{_syncBackPrefix}[DEBUG] Task.Run completed successfully for collection {collectionName}", LogType.Debug);
                                }
                                catch (Exception ex)
                                {
                                    //_log.WriteLine($"{_syncBackPrefix}[DEBUG] Task.Run caught exception for collection {collectionName}: {ex.GetType().Name}", LogType.Debug);
                                    _log.WriteLine($"{_syncBackPrefix}Unhandled exception in Task.Run for collection {collectionName}: {ex}", LogType.Error);
                                    throw; // Re-throw to ensure Task.WhenAll sees the failure
                                }
                            });
                            tasks.Add(task);
                        }
                    }

                    _log.WriteLine($"{_syncBackPrefix}Processing change streams for collections: {string.Join(", ", collectionProcessed)}. Batch Duration {seconds} seconds", LogType.Info);

                    //_log.WriteLine($"{_syncBackPrefix}[DEBUG] About to execute Task.WhenAll for {tasks.Count} tasks. Collections: [{string.Join(", ", collectionProcessed)}]", LogType.Debug);

                    try
                    {
                        _log.WriteLine($"{_syncBackPrefix}[DEBUG] Entering Task.WhenAll execution...", LogType.Debug);
                        await Task.WhenAll(tasks);
                        _log.WriteLine($"{_syncBackPrefix}[DEBUG] Task.WhenAll completed successfully for {tasks.Count} tasks", LogType.Debug);
                    }
                    catch (Exception ex)
                    {
                        //_log.WriteLine($"{_syncBackPrefix}[DEBUG] Task.WhenAll threw exception, analyzing task states...", LogType.Debug);
                        _log.WriteLine($"{_syncBackPrefix}Task.WhenAll threw exception: {ex.Message}", LogType.Error);

                        // Log individual task states
                        for (int i = 0; i < tasks.Count; i++)
                        {
                            var task = tasks[i];
                            _log.WriteLine($"{_syncBackPrefix}[DEBUG] Task {i} (Collection: {(i < collectionProcessed.Count ? collectionProcessed[i] : "Unknown")}) Status: {task.Status}", LogType.Debug);
                            if (task.IsFaulted)
                            {
                                _log.WriteLine($"{_syncBackPrefix}Task {i} FAULTED: {task.Exception?.GetBaseException().Message}", LogType.Error);
                                if (task.Exception?.InnerExceptions != null)
                                {
                                    foreach (var innerEx in task.Exception.InnerExceptions)
                                    {
                                        _log.WriteLine($"{_syncBackPrefix}[DEBUG] Task {i} Inner Exception: {innerEx.GetType().Name}: {innerEx.Message}", LogType.Debug);
                                    }
                                }
                            }
                            else if (task.IsCanceled)
                            {
                                _log.WriteLine($"{_syncBackPrefix}Task {i} CANCELED", LogType.Warning);
                            }
                            else
                            {
                                _log.WriteLine($"{_syncBackPrefix}Task {i} completed successfully", LogType.Info);
                            }
                        }

                        _log.WriteLine($"{_syncBackPrefix}[DEBUG] Task analysis complete, re-throwing exception", LogType.Debug);
                        throw; // Re-throw to let outer exception handler deal with it
                    }

                    _log.WriteLine($"{_syncBackPrefix}Completed processing batch of {collectionProcessed.Count} collection(s). Moving to next batch...", LogType.Debug);
                    //_log.WriteLine($"{_syncBackPrefix}[DEBUG] Batch completion verified, updating index from {index} to {index + _concurrentProcessors}", LogType.Debug);

                    index += _concurrentProcessors;

                    //_log.WriteLine($"{_syncBackPrefix}[DEBUG] Index updated to {index}, total keys: {totalKeys}, will continue: {index < totalKeys}", LogType.Debug);

                    // Pause briefly before next batch
                    // _log.WriteLine($"{_syncBackPrefix}[DEBUG] Sleeping 100ms before next batch...", LogType.Debug);
                    Thread.Sleep(100);
                    //_log.WriteLine($"{_syncBackPrefix}[DEBUG] Sleep completed, ready for next batch", LogType.Debug);
                }

                _log.WriteLine($"{_syncBackPrefix}Completed round {loops + 1} of change stream processing for all {totalKeys} collection(s). Re-sorting by load and starting new round...", LogType.Debug); index = 0;
                // Sort the dictionary after all processing is complete
                sortedKeys = _migrationUnitsToProcess
                    .OrderByDescending(kvp => kvp.Value.CSNormalizedUpdatesInLastBatch)
                    .Select(kvp => kvp.Key)
                    .ToList();

                loops++;
                // every 4 loops, check for oplog count, doesn't work on vcore
                if (loops % 4 == 0 && oplogSuccess && !isVCore && !_syncBack)
                {
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
                                    _log.WriteLine($"{_syncBackPrefix}[DEBUG] Oplog count Task.Run starting for {unit.Key}", LogType.Debug);
                                    oplogSuccess = MongoHelper.GetPendingOplogCountAsync(_log, _sourceClient, secondsSinceEpoch, unit.Key);
                                    _log.WriteLine($"{_syncBackPrefix}[DEBUG] Oplog count Task.Run completed for {unit.Key}: success={oplogSuccess}", LogType.Debug);
                                }
                                catch (Exception ex)
                                {
                                    //_log.WriteLine($"{_syncBackPrefix}[DEBUG] Oplog count Task.Run caught exception for {unit.Key}: {ex.GetType().Name}", LogType.Debug);
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
            _log.WriteLine($"{_syncBackPrefix}[DEBUG] ProcessCollectionChangeStream started for {collectionKey}, IsCSProcessingRun: {IsCSProcessingRun}, seconds: {seconds}", LogType.Debug);

            try
            {
                string databaseName = mu.DatabaseName;
                string collectionName = mu.CollectionName;

                _log.WriteLine($"{_syncBackPrefix}[DEBUG] Initializing database and collection references for {collectionKey}", LogType.Debug);

                IMongoDatabase sourceDb;
                IMongoDatabase targetDb;

                IMongoCollection<BsonDocument>? sourceCollection = null;
                IMongoCollection<BsonDocument>? targetCollection = null;

                if (!_syncBack)
                {
                    _log.WriteLine($"{_syncBackPrefix}[DEBUG] Setting up normal (non-syncback) database connections for {collectionKey}", LogType.Debug);
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
                    _log.WriteLine($"{_syncBackPrefix}[DEBUG] Setting up syncback database connections for {collectionKey}", LogType.Debug);
                    // For sync back, we use the source collection as the target and vice versa
                    targetDb = _sourceClient.GetDatabase(databaseName);
                    targetCollection = targetDb.GetCollection<BsonDocument>(collectionName);

                    sourceDb = _targetClient.GetDatabase(databaseName);
                    sourceCollection = sourceDb.GetCollection<BsonDocument>(collectionName);
                }

                _log.WriteLine($"{_syncBackPrefix}[DEBUG] Database connections established for {collectionKey}, proceeding to change stream options", LogType.Debug);

                try
                {
                    // Calculate seconds first so we can use it in ChangeStreamOptions
                    if (seconds == 0)
                        seconds = GetBatchDurationInSeconds(.5f); //get seconds from config or use default

                    _log.WriteLine($"{_syncBackPrefix}[DEBUG] Calculated batch duration: {seconds} seconds for {collectionKey}", LogType.Debug);

                    // MaxAwaitTime should be shorter than the cancellation timeout to allow proper cleanup
                    // Use 80% of the total duration to ensure cursor returns before cancellation
                    int maxAwaitSeconds = Math.Max(5, (int)(seconds * 0.8));

                    _log.WriteLine($"{_syncBackPrefix}[DEBUG] MaxAwaitTime set to {maxAwaitSeconds} seconds for {collectionKey}", LogType.Debug);

                    // Default options; will be overridden based on resume strategy
                    ChangeStreamOptions options = new ChangeStreamOptions { BatchSize = 500, FullDocument = ChangeStreamFullDocumentOption.UpdateLookup, MaxAwaitTime = TimeSpan.FromSeconds(maxAwaitSeconds) };

                    DateTime startedOn;
                    DateTime timeStamp;
                    string resumeToken = string.Empty;
                    string? version = string.Empty;

                    _log.WriteLine($"{_syncBackPrefix}[DEBUG] Setting up resume token and timestamp for {collectionKey}, syncBack: {_syncBack}", LogType.Debug);

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

                    _log.WriteLine($"{_syncBackPrefix}[DEBUG] Resume token info for {collectionKey} - timestamp: {timeStamp}, token length: {resumeToken.Length}, version: {version}, startedOn: {startedOn}", LogType.Debug);

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
                    }
                    else if (!string.IsNullOrEmpty(resumeToken) && !mu.ResetChangeStream) //skip resume token if its reset, both version  having resume token
                    {
                        options = new ChangeStreamOptions { BatchSize = 500, FullDocument = ChangeStreamFullDocumentOption.UpdateLookup, ResumeAfter = BsonDocument.Parse(resumeToken), MaxAwaitTime = TimeSpan.FromSeconds(maxAwaitSeconds) };
                    }
                    else if (string.IsNullOrEmpty(resumeToken) && version.StartsWith("3")) //for Mongo 3.6
                    {
                        options = new ChangeStreamOptions { BatchSize = 500, FullDocument = ChangeStreamFullDocumentOption.UpdateLookup, MaxAwaitTime = TimeSpan.FromSeconds(maxAwaitSeconds) };
                    }
                    else if (startedOn > DateTime.MinValue && !version.StartsWith("3"))  //newer version
                    {
                        var bsonTimestamp = MongoHelper.ConvertToBsonTimestamp((DateTime)startedOn);
                        options = new ChangeStreamOptions { BatchSize = 500, FullDocument = ChangeStreamFullDocumentOption.UpdateLookup, StartAtOperationTime = bsonTimestamp, MaxAwaitTime = TimeSpan.FromSeconds(maxAwaitSeconds) };
                        if (mu.ResetChangeStream)
                        {
                            ResetCounters(mu);
                        }

                        mu.ResetChangeStream = false; //reset the start time after setting resume token
                    }

                    var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(seconds));
                    CancellationToken cancellationToken = cancellationTokenSource.Token;

                    _log.WriteLine($"{_syncBackPrefix}[DEBUG] Created cancellation token for {collectionKey} with {seconds} second timeout", LogType.Debug);
                    _log.ShowInMonitor($"{_syncBackPrefix}Monitoring change stream with new batch for {sourceCollection!.CollectionNamespace}. Batch Duration {seconds} seconds");

                    // In simulated runs, use source collection as a placeholder to avoid null target warnings
                    if (_job.IsSimulatedRun && targetCollection == null)
                    {
                        _log.WriteLine($"{_syncBackPrefix}[DEBUG] Simulated run detected for {collectionKey}, using source as target placeholder", LogType.Debug);
                        targetCollection = sourceCollection;
                    }

                    _log.WriteLine($"{_syncBackPrefix}[DEBUG] About to call WatchCollection for {collectionKey}", LogType.Debug);
                    await WatchCollection(mu, options, sourceCollection!, targetCollection!, cancellationToken, seconds);
                    _log.WriteLine($"{_syncBackPrefix}[DEBUG] WatchCollection completed successfully for {collectionKey}", LogType.Debug);
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
            finally
            {
                _log.WriteLine($"{_syncBackPrefix}[DEBUG] ProcessCollectionChangeStream completed for {collectionKey}", LogType.Debug);
            }
        }

        private async Task WatchCollection(MigrationUnit mu, ChangeStreamOptions options, IMongoCollection<BsonDocument> sourceCollection, IMongoCollection<BsonDocument> targetCollection, CancellationToken cancellationToken, int seconds)
        {
            string collectionKey = $"{mu.DatabaseName}.{mu.CollectionName}";
            _log.WriteLine($"{_syncBackPrefix}[DEBUG] WatchCollection started for {collectionKey}, batch duration: {seconds}s", LogType.Debug);

            bool isVCore = (_syncBack ? _job.TargetEndpoint : _job.SourceEndpoint)
                .Contains("mongocluster.cosmos.azure.com", StringComparison.OrdinalIgnoreCase);

            _log.WriteLine($"{_syncBackPrefix}[DEBUG] VCore detection for {collectionKey}: {isVCore}", LogType.Debug);

            long counter = 0;
            BsonDocument userFilterDoc = new BsonDocument();

            userFilterDoc = MongoHelper.GetFilterDoc(mu.UserFilter);
            _log.WriteLine($"{_syncBackPrefix}[DEBUG] User filter setup for {collectionKey}, filter elements count: {userFilterDoc.Elements.Count()}", LogType.Debug);

            ChangeStreamDocuments changeStreamDocuments = new ChangeStreamDocuments();

            try
            {
                _log.WriteLine($"{_syncBackPrefix}[DEBUG] Setting up pipeline for {collectionKey}, JobType: {_job.JobType}", LogType.Debug);

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
                _log.WriteLine($"{_syncBackPrefix}[DEBUG] Pipeline created for {collectionKey}, pipeline length: {pipelineArray.Length}", LogType.Debug);

                // Wrap Watch() in a timeout to prevent indefinite blocking - use 80% of batch duration like MaxAwaitTime
                int watchTimeoutSeconds = (int)(seconds * 0.8);
                _log.WriteLine($"{_syncBackPrefix}[DEBUG] Creating watch task for {collectionKey} with {watchTimeoutSeconds}s timeout", LogType.Debug);

                var watchTask = Task.Run(() =>
                {
                    try
                    {
                        _log.WriteLine($"{_syncBackPrefix}[DEBUG] Watch Task.Run starting for {collectionKey}", LogType.Debug);
                        var cursor = sourceCollection.Watch<ChangeStreamDocument<BsonDocument>>(pipelineArray, options, cancellationToken);
                        _log.WriteLine($"{_syncBackPrefix}[DEBUG] Watch Task.Run completed successfully for {collectionKey}", LogType.Debug);
                        return cursor;
                    }
                    catch (OperationCanceledException)
                    {
                        // Expected when cancellation token times out - don't log as error
                        throw; // Re-throw to let the timeout handler deal with it
                    }
                    catch (Exception ex)
                    {
                        _log.WriteLine($"{_syncBackPrefix}Exception in Watch Task.Run for {collectionKey}: {ex}", LogType.Error);
                        throw; // Re-throw to let the timeout handler deal with it
                    }
                });
                var watchTimeoutTask = Task.Delay(TimeSpan.FromSeconds(watchTimeoutSeconds));

                _log.WriteLine($"{_syncBackPrefix}[DEBUG] Waiting for watch task or timeout for {collectionKey}", LogType.Debug);
                var completedTask = await Task.WhenAny(watchTask, watchTimeoutTask);

                if (completedTask == watchTimeoutTask)
                {
                    _log.WriteLine($"{_syncBackPrefix}[DEBUG] Watch timeout occurred for {collectionKey}", LogType.Debug);
                    _log.WriteLine($"{_syncBackPrefix}Watch() call timed out after {watchTimeoutSeconds} seconds for {sourceCollection.CollectionNamespace} - skipping this collection in this batch", LogType.Debug);
                    _log.ShowInMonitor($"{_syncBackPrefix}WARNING: Change stream cursor creation timed out for {sourceCollection.CollectionNamespace}");
                    return; // Skip this collection, will retry in next batch
                }

                _log.WriteLine($"{_syncBackPrefix}[DEBUG] Watch task completed for {collectionKey}, getting cursor", LogType.Debug);
                IChangeStreamCursor<ChangeStreamDocument<BsonDocument>> cursor;
                try
                {
                    cursor = await watchTask;
                    _log.WriteLine($"{_syncBackPrefix}[DEBUG] Successfully obtained cursor for {collectionKey}", LogType.Debug);
                }
                catch (Exception ex)
                {
                    _log.WriteLine($"{_syncBackPrefix}Failed to create change stream cursor for {collectionKey}: {ex}", LogType.Error);
                    return; // Skip this collection, will retry in next batch
                }

                using (cursor)
                {
                    string lastProcessedToken = string.Empty;

                    if (_job.SourceServerVersion.StartsWith("3"))
                    {
                        _log.WriteLine($"{_syncBackPrefix}[DEBUG] Using MongoDB 3.x enumeration for {collectionKey}", LogType.Debug);
                        foreach (var change in cursor.ToEnumerable(cancellationToken))
                        {
                            if (cancellationToken.IsCancellationRequested || ExecutionCancelled)
                            {
                                _log.WriteLine($"{_syncBackPrefix}Change stream processing cancelled for {sourceCollection!.CollectionNamespace}", LogType.Info);
                                return;
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

                            if (!ProcessCursor(change, cursor, targetCollection, sourceCollection.CollectionNamespace.ToString(), mu, changeStreamDocuments, ref counter, userFilterDoc))
                                return;

                            // MEMORY SAFETY: Flush accumulated changes periodically to prevent OOM
                            int totalAccumulated = changeStreamDocuments.DocsToBeInserted.Count +
                                                  changeStreamDocuments.DocsToBeUpdated.Count +
                                                  changeStreamDocuments.DocsToBeDeleted.Count;

                            if (totalAccumulated >= 5000)
                            {
                                // BACKPRESSURE: Check global pending writes before adding more load
                                int pending = GetGlobalPendingWriteCount();
                                while (pending >= MAX_GLOBAL_PENDING_WRITES)
                                {
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
                    }
                    else
                    {
                        _log.WriteLine($"{_syncBackPrefix}[DEBUG] Using MongoDB 4.x+ cursor iteration for {collectionKey}", LogType.Debug);
                        // Watchdog: Track when MoveNext was last called to detect hangs
                        DateTime lastMoveNextCall = DateTime.UtcNow;
                        int moveNextTimeoutSeconds = seconds + 10; // Add 10 second buffer beyond batch duration

                        _log.WriteLine($"{_syncBackPrefix}[DEBUG] Starting cursor loop for {collectionKey}, watchdog timeout: {moveNextTimeoutSeconds}s", LogType.Debug);

                        while (true)
                        {
                            if (cancellationToken.IsCancellationRequested || ExecutionCancelled)
                            {
                                //_log.WriteLine($"{_syncBackPrefix}[DEBUG] Cancellation requested for {collectionKey}, breaking loop", LogType.Debug);
                                _log.WriteLine($"{_syncBackPrefix}Change stream processing cancelled for {sourceCollection!.CollectionNamespace}", LogType.Info);
                                return;
                            }

                            // Watchdog: Check if MoveNext is taking too long (stuck)
                            if ((DateTime.UtcNow - lastMoveNextCall).TotalSeconds > moveNextTimeoutSeconds)
                            {
                                //_log.WriteLine($"{_syncBackPrefix}[DEBUG] Watchdog timeout triggered for {collectionKey}", LogType.Debug);
                                _log.WriteLine($"{_syncBackPrefix}Watchdog timeout: MoveNext exceeded {moveNextTimeoutSeconds}s for {sourceCollection!.CollectionNamespace}. Breaking out to prevent hang.", LogType.Warning);
                                break; // Exit the while loop, batch will complete in finally block
                            }

                            bool hasMore;
                            try
                            {
                                _log.WriteLine($"{_syncBackPrefix}[DEBUG] Calling MoveNext for {collectionKey}", LogType.Debug);
                                lastMoveNextCall = DateTime.UtcNow;
                                hasMore = cursor.MoveNext(cancellationToken);
                                _log.WriteLine($"{_syncBackPrefix}[DEBUG] MoveNext completed for {collectionKey}, hasMore: {hasMore}", LogType.Debug);
                            }
                            catch (OperationCanceledException)
                            {
                                _log.WriteLine($"{_syncBackPrefix}[DEBUG] MoveNext cancelled by timeout for {collectionKey}", LogType.Debug);
                                // Expected when cancellationToken times out
                                _log.WriteLine($"{_syncBackPrefix}MoveNext cancelled by timeout for {sourceCollection!.CollectionNamespace} - completing batch", LogType.Debug);
                                break; // Exit loop, process accumulated batch
                            }

                            if (!hasMore)
                            {
                                _log.WriteLine($"{_syncBackPrefix}[DEBUG] No more data available for {collectionKey}, exiting loop", LogType.Debug);
                                // No more data available, exit loop
                                break;
                            }

                            _log.WriteLine($"{_syncBackPrefix}[DEBUG] Processing current batch for {collectionKey}, items count: {cursor.Current?.Count() ?? 0}", LogType.Debug);

                            foreach (var change in cursor.Current)
                            {
                                if (cancellationToken.IsCancellationRequested || ExecutionCancelled)
                                {
                                    _log.WriteLine($"{_syncBackPrefix}Change stream processing cancelled for {sourceCollection!.CollectionNamespace}", LogType.Debug);
                                    return;
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

                                if (!ProcessCursor(change, cursor, targetCollection, sourceCollection.CollectionNamespace.ToString(), mu, changeStreamDocuments, ref counter, userFilterDoc))
                                    return;

                                // MEMORY SAFETY: Flush accumulated changes periodically to prevent OOM
                                // Check every 5000 documents to avoid excessive memory buildup
                                int totalAccumulated = changeStreamDocuments.DocsToBeInserted.Count +
                                                      changeStreamDocuments.DocsToBeUpdated.Count +
                                                      changeStreamDocuments.DocsToBeDeleted.Count;

                                if (totalAccumulated >= 5000)
                                {
                                    _log.WriteLine($"{_syncBackPrefix}[DEBUG] Memory safety threshold reached for {collectionKey}: {totalAccumulated} accumulated changes", LogType.Debug);

                                    // BACKPRESSURE: Check global pending writes before adding more load
                                    int pending = GetGlobalPendingWriteCount();
                                    _log.WriteLine($"{_syncBackPrefix}[DEBUG] Current global pending writes for {collectionKey}: {pending}", LogType.Debug);

                                    while (pending >= MAX_GLOBAL_PENDING_WRITES)
                                    {
                                        _log.WriteLine($"{_syncBackPrefix}[DEBUG] Waiting for backpressure relief for {collectionKey}, pending: {pending}", LogType.Debug);
                                        await WaitWithExponentialBackoffAsync(pending, sourceCollection.CollectionNamespace.ToString());

                                        // Check if memory recovered after wait
                                        if (IsMemoryExhausted(out long currentMB, out long maxMB, out double percent))
                                        {
                                            //_log.WriteLine($"{_syncBackPrefix}[DEBUG] Memory pressure detected for {collectionKey}: {currentMB}MB/{maxMB}MB ({percent:F1}%)", LogType.Debug);
                                            _log.ShowInMonitor($"{_syncBackPrefix}Memory pressure detected: {currentMB}MB / {maxMB}MB ({percent:F1}%)");
                                            await WaitForMemoryRecoveryAsync(sourceCollection.CollectionNamespace.ToString());
                                        }

                                        pending = GetGlobalPendingWriteCount();
                                    }

                                    _log.WriteLine($"{_syncBackPrefix}Memory safety flush: Processing {totalAccumulated} accumulated changes for {sourceCollection.CollectionNamespace}", LogType.Info);

                                    IncrementGlobalPendingWrites();
                                    try
                                    {
                                        _log.WriteLine($"{_syncBackPrefix}[DEBUG] Starting bulk processing for memory safety flush: {collectionKey}", LogType.Debug);
                                        // Flush accumulated changes
                                        await BulkProcessChangesAsync(
                                            mu,
                                            targetCollection,
                                            insertEvents: changeStreamDocuments.DocsToBeInserted,
                                            updateEvents: changeStreamDocuments.DocsToBeUpdated,
                                            deleteEvents: changeStreamDocuments.DocsToBeDeleted);

                                        _log.WriteLine($"{_syncBackPrefix}[DEBUG] Bulk processing completed for memory safety flush: {collectionKey}", LogType.Debug);

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

                                        //_log.WriteLine($"{_syncBackPrefix}[DEBUG] Memory collections cleared for {collectionKey}", LogType.Debug);
                                        _log.WriteLine($"{_syncBackPrefix}Memory safety flush completed, memory released for {sourceCollection.CollectionNamespace}", LogType.Info);
                                    }
                                    finally
                                    {
                                        DecrementGlobalPendingWrites();
                                        _log.WriteLine($"{_syncBackPrefix}[DEBUG] Global pending writes decremented after memory safety flush: {collectionKey}", LogType.Debug);
                                    }
                                }
                            }

                            if (ExecutionCancelled)
                            {
                                return;
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
                _log.WriteLine($"{_syncBackPrefix}[DEBUG] Entering finally block for {collectionKey}", LogType.Debug);

                try
                {
                    int totalChanges = changeStreamDocuments.DocsToBeInserted.Count +
                                      changeStreamDocuments.DocsToBeUpdated.Count +
                                      changeStreamDocuments.DocsToBeDeleted.Count;

                    _log.WriteLine($"{_syncBackPrefix}[DEBUG] Final batch processing for {collectionKey}: {totalChanges} total changes", LogType.Debug);

                    if (totalChanges > 0)
                    {
                        // NO BACKPRESSURE HERE: This finally block REDUCES memory pressure by writing accumulated changes
                        // Blocking writes here would prevent the very thing that frees up memory
                        // Backpressure is applied at read/accumulation points (periodic flushes) only
                        _log.ShowInMonitor($"{_syncBackPrefix}Processing batch for {sourceCollection.CollectionNamespace}: {totalChanges} changes (I:{changeStreamDocuments.DocsToBeInserted.Count}, U:{changeStreamDocuments.DocsToBeUpdated.Count}, D:{changeStreamDocuments.DocsToBeDeleted.Count})");
                    }

                    IncrementGlobalPendingWrites();
                    _log.WriteLine($"{_syncBackPrefix}[DEBUG] Global pending writes incremented for final batch: {collectionKey}", LogType.Debug);

                    try
                    {
                        _log.WriteLine($"{_syncBackPrefix}[DEBUG] Starting final BulkProcessChangesAsync for {collectionKey}", LogType.Debug);
                        // Process all pending changes in the batch - AWAIT properly to ensure completion before next batch
                        await BulkProcessChangesAsync(
                            mu,
                            targetCollection,
                            insertEvents: changeStreamDocuments.DocsToBeInserted,
                            updateEvents: changeStreamDocuments.DocsToBeUpdated,
                            deleteEvents: changeStreamDocuments.DocsToBeDeleted);

                        _log.WriteLine($"{_syncBackPrefix}[DEBUG] Final BulkProcessChangesAsync completed for {collectionKey}", LogType.Debug);

                        if (totalChanges > 0)
                        {
                            _log.ShowInMonitor($"{_syncBackPrefix}Batch processing completed for {sourceCollection.CollectionNamespace}: {totalChanges} changes written successfully");
                        }
                    }
                    finally
                    {
                        DecrementGlobalPendingWrites();
                        _log.WriteLine($"{_syncBackPrefix}[DEBUG] Global pending writes decremented after final batch: {collectionKey}", LogType.Debug);
                    }

                    // CRITICAL: Only update resume token and timestamp AFTER successful batch write
                    // This ensures we can recover from the last successful checkpoint on failure
                    if (!string.IsNullOrEmpty(changeStreamDocuments.LatestResumeToken))
                    {
                        _log.WriteLine($"{_syncBackPrefix}[DEBUG] Updating resume token for {collectionKey}", LogType.Debug);
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

                        _log.WriteLine($"{_syncBackPrefix}Checkpoint updated for {sourceCollection.CollectionNamespace}: Resume token persisted after successful batch write", LogType.Verbose);
                    }

                    mu.CSUpdatesInLastBatch = counter;
                    mu.CSNormalizedUpdatesInLastBatch = (long)(counter / (mu.CSLastBatchDurationSeconds > 0 ? mu.CSLastBatchDurationSeconds : 1));
                    _jobList?.Save();

                    _log.WriteLine($"{_syncBackPrefix}[DEBUG] Migration unit stats updated for {collectionKey}: counter={counter}, normalized={mu.CSNormalizedUpdatesInLastBatch}", LogType.Debug);

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
                    _log.WriteLine($"{_syncBackPrefix}Resume token NOT updated due to batch failure - will resume from last successful checkpoint", LogType.Verbose);
                }

                _log.WriteLine($"{_syncBackPrefix}[DEBUG] WatchCollection finally block completed for {collectionKey}", LogType.Debug);
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

                // Output change details to the console
                if (timeStamp == DateTime.MinValue)
                    _log.ShowInMonitor($"{_syncBackPrefix}{change.OperationType} operation detected in {collNameSpace} for _id: {change.DocumentKey["_id"]}. Sequence in batch #{counter}");
                else
                    _log.ShowInMonitor($"{_syncBackPrefix}{change.OperationType} operation detected in {collNameSpace} for _id: {change.DocumentKey["_id"]} with TS (UTC): {timeStamp}. Sequence in batch #{counter}");

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
                    _log.ShowInMonitor($"{_syncBackPrefix}Change stream max batch size exceeded. Flushing changes for {collNameSpace}", LogType.Debug);

                    // Process the changes in bulk if the batch size exceeds the limit
                    BulkProcessChangesAsync(
                        mu,
                        targetCollection,
                        insertEvents: changeStreamDocuments.DocsToBeInserted,
                        updateEvents: changeStreamDocuments.DocsToBeUpdated,
                        deleteEvents: changeStreamDocuments.DocsToBeDeleted).GetAwaiter().GetResult();

                    // CRITICAL: Only update resume token AFTER successful mid-batch flush
                    // This ensures we can recover from the last successful checkpoint on failure
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
    }
}