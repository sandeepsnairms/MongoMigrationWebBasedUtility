using MongoDB.Bson;
using MongoDB.Driver;
using OnlineMongoMigrationProcessor.Helpers;
using OnlineMongoMigrationProcessor.Models;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using static OnlineMongoMigrationProcessor.MongoHelper;

#pragma warning disable CS8602 // Dereference of a possibly null reference.

namespace OnlineMongoMigrationProcessor
{
    public class CollectionLevelChangeStreamProcessor : ChangeStreamProcessor
    {
        private Dictionary<string, AccumulatedChangesTracker> _accumulatedChangesPerCollection = new Dictionary<string, AccumulatedChangesTracker>();

     

        public CollectionLevelChangeStreamProcessor(Log log, MongoClient sourceClient, MongoClient targetClient, JobList jobList, MigrationJob job, MigrationSettings config, bool syncBack = false)
            : base(log, sourceClient, targetClient, jobList, job, config, syncBack)
        {
        }

        protected override async Task ProcessChangeStreamsAsync(CancellationToken token)
        {
            //_log.ShowInMonitor($"{_syncBackPrefix}ProcessChangeStreamsAsync started. Token cancelled: {token.IsCancellationRequested}, ExecutionCancelled: {ExecutionCancelled}");
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
            
            while (!token.IsCancellationRequested && !ExecutionCancelled)
            {
                var totalKeys = sortedKeys.Count;

                loops++;
                while (index < totalKeys && !token.IsCancellationRequested && !ExecutionCancelled)
                {
                    _log.WriteLine($"{_syncBackPrefix}Starting batch at index {index} (totalKeys: {totalKeys}, round: {loops})", LogType.Debug);
                    
                    var tasks = new List<Task>();
                    var collectionProcessed = new List<string>();

                    // Determine the batch
                    var batchKeys = sortedKeys.Skip(index).Take(_concurrentProcessors).ToList();
                    _log.WriteLine($"{_syncBackPrefix}Batch contains {batchKeys.Count} collection(s): {string.Join(", ", batchKeys)}", LogType.Debug);
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
                            // Initialize accumulated changes tracker if not present
                            if (!_accumulatedChangesPerCollection.ContainsKey(key))                              
                            {
                                _accumulatedChangesPerCollection[key] = new AccumulatedChangesTracker(key);
                            }

                            collectionProcessed.Add(key);
                            unit.CSLastBatchDurationSeconds = seconds; // Store the factor for each unit
                            // Don't pass token to Task.Run - each collection manages its own timeout via CancellationTokenSource inside SetChangeStreamOptionandWatch
                            string collectionName = key; // Capture for closure
                            
                            
                            try
                            {
                                var task = Task.Run(async () =>
                                {
                                    try
                                    {                                        
                                        await SetChangeStreamOptionandWatch(unit, true, seconds);
                                    }
                                    catch (TimeoutException tex)
                                    {
                                        _log.WriteLine($"{_syncBackPrefix}Timeout for {collectionName}: {tex.Message}", LogType.Debug);
                                        // Don't re-throw - let batch continue with other collections
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
                    

                    try
                    {
                        _log.WriteLine($"{_syncBackPrefix}Processing change streams for collections: {string.Join(", ", collectionProcessed)}. Batch Duration {seconds} seconds", LogType.Info);
                        await Task.WhenAll(tasks);
                        _log.WriteLine($"{_syncBackPrefix}Completed processing change streams for collections: {string.Join(", ", collectionProcessed)}. Batch Duration {seconds} seconds", LogType.Debug);
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
                                    _log.WriteLine($"{_syncBackPrefix}Task {i} CANCELED", LogType.Debug);
                                }
                            }
                            catch
                            {
                            }
                        }

                        throw; // Re-throw to let outer exception handler deal with it
                    }

                    index += _concurrentProcessors;
                    _log.WriteLine($"{_syncBackPrefix}Batch completed, index advanced to {index} (totalKeys: {totalKeys})", LogType.Debug);

                    // Increased to 5000ms to address OOM issues and server CPU spikes
                    _log.WriteLine($"{_syncBackPrefix}Sleeping 5 seconds between batches...", LogType.Debug);
                    Thread.Sleep(5000);
                    
                }

                _log.WriteLine($"{_syncBackPrefix}Completed round {loops} of change stream processing for all {totalKeys} collection(s). Re-sorting by load and starting new round...");

                //static collections resume tokens need adjustment
                _log.WriteLine($"{_syncBackPrefix}Adjusting cursor times for static collections...", LogType.Debug);
                AdjustCusrsorTimeForStaticCollections();

                index = 0;
                _log.WriteLine($"{_syncBackPrefix}Index reset to 0 for next round", LogType.Debug);
                // Sort the dictionary after all processing is complete
                sortedKeys = _migrationUnitsToProcess
                    .OrderByDescending(kvp => kvp.Value.CSNormalizedUpdatesInLastBatch)
                    .Select(kvp => kvp.Key)
                    .ToList();
                
            }
        }

        private bool AdjustCusrsorTimeForStaticCollections()
        {
            try
            {
                foreach (var mu in _migrationUnitsToProcess.Values)
                {
                    if (!_syncBack)
                    {
                        TimeSpan gap;
                        if (mu.CursorUtcTimestamp > DateTime.MinValue)
                             gap = DateTime.UtcNow - mu.CursorUtcTimestamp.AddHours(mu.CSAddHours);
                        else
                             gap = DateTime.UtcNow - (mu.ChangeStreamStartedOn ?? DateTime.UtcNow).AddHours(mu.CSAddHours);

                        if (gap.TotalMinutes > (60 * 24) && mu.CSUpdatesInLastBatch == 0)
                        {
                            mu.CSAddHours += 22;
                            mu.ResumeToken = string.Empty; //clear resume token to use timestamp
                            _log.WriteLine($"{_syncBackPrefix}24 hour change stream lag with no updates detected for {mu.DatabaseName}.{mu.CollectionName} - pushed by 22 hours",LogType.Warning);
                        }
                    }
                    else
                    {
                        TimeSpan gap;
                        if (mu.CursorUtcTimestamp > DateTime.MinValue)
                            gap = DateTime.UtcNow - mu.SyncBackCursorUtcTimestamp.AddHours(mu.SyncBackAddHours);
                        else
                            gap = DateTime.UtcNow - (mu.SyncBackChangeStreamStartedOn ?? DateTime.UtcNow).AddHours(mu.SyncBackAddHours);

                        if (gap.TotalMinutes > (60 * 24) && mu.CSUpdatesInLastBatch == 0)
                        {
                            mu.SyncBackAddHours += 22;
                            mu.ResumeToken = string.Empty; //clear resume token to use timestamp
                            _log.WriteLine($"{_syncBackPrefix}24 hour change stream lag with no updates detected for {mu.DatabaseName}.{mu.CollectionName} - pushed by 22 hours", LogType.Warning);
                        }
                    }
                }
                return true;
            }
            catch(Exception ex)
            {
                _log.WriteLine($"{_syncBackPrefix}Error adjusting cursor time for static collections: {ex}", LogType.Error);
                StopProcessing = true;
                return false;
            }

        }

        private async Task SetChangeStreamOptionandWatch(MigrationUnit mu, bool IsCSProcessingRun = false, int seconds = 0)
        {
            string collectionKey = $"{mu.DatabaseName}.{mu.CollectionName}";
            _log.WriteLine($"{_syncBackPrefix}SetChangeStreamOptionandWatch started for {collectionKey} - IsCSProcessingRun: {IsCSProcessingRun}, Seconds: {seconds}", LogType.Verbose);

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
                        timeStamp = mu.CursorUtcTimestamp.AddHours(mu.CSAddHours);//to adjust for expiring rsume tokens
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
                        startedOn = startedOn.AddHours(mu.CSAddHours); //to adjust for expiring rsume tokens
                    }
                    else
                    {
                        timeStamp = mu.SyncBackCursorUtcTimestamp.AddHours(mu.SyncBackAddHours);//to adjust for expiring rsume tokens
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
                        startedOn = startedOn.AddHours(mu.SyncBackAddHours); //to adjust for expiring rsume tokens
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

                    if (timeStamp > DateTime.MinValue && !mu.ResetChangeStream && string.IsNullOrEmpty(resumeToken) && !(_job.JobType == JobType.RUOptimizedCopy && !_job.ProcessingSyncBack)) //skip CursorUtcTimestamp if its reset 
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
                catch (TimeoutException tex)
                {
                    // TimeoutException is expected and handled - don't set StopProcessing
                    // The timeout counter tracking will handle retry logic
                    _log.WriteLine($"{_syncBackPrefix}TimeoutException caught in SetChangeStreamOptionandWatch for {sourceCollection!.CollectionNamespace}: {tex.Message}", LogType.Debug);
                    // Let this propagate to Task.Run wrapper for timeout counter tracking
                    throw;
                }
                catch (OperationCanceledException ex)
                {
                    _log.WriteLine($"{_syncBackPrefix}OperationCanceledException in SetChangeStreamOptionandWatch for {sourceCollection!.CollectionNamespace}: {ex.Message}", LogType.Info);
                }
                catch (MongoCommandException ex) when (ex.ToString().Contains("Resume of change stream was not possible"))
                {
                    // Handle other potential exceptions
                    _log.WriteLine($"{_syncBackPrefix}Oplog is full. Error processing change stream for {sourceCollection.CollectionNamespace}. Details: {ex}", LogType.Error);
                    _log.ShowInMonitor($"{_syncBackPrefix}Oplog is full. Error processing change stream for {sourceCollection.CollectionNamespace}. Details: {ex}");
                    StopProcessing = true;
                }
                catch (MongoCommandException ex) when (ex.Message.Contains("Expired resume token") || ex.Message.Contains("cursor"))
                {
                    _log.WriteLine($"{_syncBackPrefix}Resume token has expired or cursor is invalid for {sourceCollection.CollectionNamespace}.", LogType.Error);
                    _log.ShowInMonitor($"{_syncBackPrefix}Resume token has expired or cursor is invalid for {sourceCollection.CollectionNamespace}.");
                }
            }
            catch (TimeoutException)
            {
                // TimeoutException is expected and already logged - just propagate for timeout counter tracking
                // Do NOT set StopProcessing = true for timeouts
                throw;
            }
            catch (Exception ex)
            {
                _log.WriteLine($"{_syncBackPrefix}Error processing change stream for {mu.DatabaseName}.{mu.CollectionName}. Details: {ex}", LogType.Error);
                StopProcessing = true;
            }

            _log.WriteLine($"{_syncBackPrefix}Exiting  processing change stream for {mu.DatabaseName}.{mu.CollectionName}. ", LogType.Debug);

        }


        private async Task FlushPendingChangesAsync(MigrationUnit mu, IMongoCollection<BsonDocument> targetCollection, AccumulatedChangesTracker accumulatedChangesInColl)
        {
            
            // Flush accumulated changes
            await BulkProcessChangesAsync(
                mu,
                targetCollection,
                insertEvents: accumulatedChangesInColl.DocsToBeInserted,
                updateEvents: accumulatedChangesInColl.DocsToBeUpdated,
                deleteEvents: accumulatedChangesInColl.DocsToBeDeleted);

            // Update resume token after successful flush
            if (accumulatedChangesInColl.CollectionKey != $"{mu.DatabaseName}.{mu.CollectionName}")
            {
                _log.WriteLine($"{_syncBackPrefix}CollectionKey is not maching. Expected '{mu.DatabaseName}.{mu.CollectionName}' Received: '{accumulatedChangesInColl.CollectionKey}'", LogType.Error);
                throw new Exception($"{_syncBackPrefix}CollectionKey is not maching. Expected '{mu.DatabaseName}.{mu.CollectionName}' Received: '{accumulatedChangesInColl.CollectionKey}'");
            }

            if (!string.IsNullOrEmpty(accumulatedChangesInColl.LatestResumeToken))
            {
                if (!_syncBack)
                {
                    // We don't allow going backwards in time
                    if (accumulatedChangesInColl.LatestTimestamp - mu.CursorUtcTimestamp >= TimeSpan.FromSeconds(0))
                    {
                        mu.CursorUtcTimestamp = accumulatedChangesInColl.LatestTimestamp;
                        mu.ResumeToken = accumulatedChangesInColl.LatestResumeToken;
                    }
                    else
                    {
                        string collectionNamespace = $"{mu.DatabaseName}.{mu.CollectionName}";
                        _log.WriteLine($"Old Token:{mu.ResumeToken}, New Token:{accumulatedChangesInColl.LatestResumeToken} for {collectionNamespace}", LogType.Error);
                        throw new Exception($"{_syncBackPrefix} Timestamp mismatch Old Value: {mu.CursorUtcTimestamp} is newer than New Value: {accumulatedChangesInColl.LatestTimestamp} for {collectionNamespace}");
                    }
                }
                else
                {
                    // We don't allow going backwards in time
                    if (accumulatedChangesInColl.LatestTimestamp - mu.SyncBackCursorUtcTimestamp >= TimeSpan.FromSeconds(0))
                    {
                        mu.SyncBackCursorUtcTimestamp = accumulatedChangesInColl.LatestTimestamp;
                        mu.SyncBackResumeToken = accumulatedChangesInColl.LatestResumeToken;
                    }
                    else
                    {
                        string collectionNamespace = $"{mu.DatabaseName}.{mu.CollectionName}";
                        _log.WriteLine($"Old Token:{mu.SyncBackResumeToken}, New Token:{accumulatedChangesInColl.LatestResumeToken} for {collectionNamespace}",LogType.Error);
                        throw new Exception($"{_syncBackPrefix} Timestamp mismatch Old Value: {mu.SyncBackCursorUtcTimestamp} is newer than New Value: {accumulatedChangesInColl.LatestTimestamp} for {collectionNamespace}");
                    }
                }
                _resumeTokenCache[$"{targetCollection.CollectionNamespace}"] = accumulatedChangesInColl.LatestResumeToken;
            }
            //else
            //{
            //    _log.WriteLine($"{_syncBackPrefix}LatestResumeToken is empty for {targetCollection.CollectionNamespace}", LogType.Error);
            //    throw new Exception($"{_syncBackPrefix} LatestResumeToken is empty for {targetCollection.CollectionNamespace}");
            //}

            // Clear collections to free memory
            accumulatedChangesInColl.DocsToBeInserted.Clear();
            accumulatedChangesInColl.DocsToBeUpdated.Clear();
            accumulatedChangesInColl.DocsToBeDeleted.Clear();
            accumulatedChangesInColl.ClearMetadata();
        }

        private void ProcessWatchCollectionException(AccumulatedChangesTracker accumulatedChangesInColl, MigrationUnit mu)
        {
            accumulatedChangesInColl.DocsToBeInserted.Clear();
            accumulatedChangesInColl.DocsToBeUpdated.Clear();
            accumulatedChangesInColl.DocsToBeDeleted.Clear();

            // Update counters before early return
            mu.CSUpdatesInLastBatch = 0;
            mu.CSNormalizedUpdatesInLastBatch = 0;
            _jobList?.Save();

        }

        private async Task WatchCollection(MigrationUnit mu, ChangeStreamOptions options, IMongoCollection<BsonDocument> sourceCollection, IMongoCollection<BsonDocument> targetCollection, CancellationToken cancellationToken, int seconds)
        {
            string collectionKey = $"{mu.DatabaseName}.{mu.CollectionName}";
            _log.WriteLine($"{_syncBackPrefix}WatchCollection started for {collectionKey} - Duration: {seconds}s", LogType.Verbose);

            bool isVCore = (_syncBack ? _job.TargetEndpoint : _job.SourceEndpoint)
                .Contains("mongocluster.cosmos.azure.com", StringComparison.OrdinalIgnoreCase);

            long counter = 0;
            BsonDocument userFilterDoc = MongoHelper.GetFilterDoc(mu.UserFilter);

            var accumulatedChangesInColl = _accumulatedChangesPerCollection[collectionKey];

            bool shouldProcessFinalBatch = true; // Flag to control finally block execution

            try
            {
                var pipelineArray = CreateChangeStreamPipeline();

                try
                {
                    counter = await MongoSafeTaskExecutor.ExecuteAsync(
                    async (ct) =>
                    {
                        _log.WriteLine($"{_syncBackPrefix}Starting cursor creation for {collectionKey}", LogType.Verbose);

                        // 1. Create cursor
                        var cursor = await CreateChangeStreamCursorAsync(
                            sourceCollection,
                            pipelineArray,
                            options,
                            ct,
                            collectionKey
                        );

                        if (cursor == null)
                        {
                            _log.WriteLine($"{_syncBackPrefix}Cursor is null for {collectionKey}", LogType.Debug);
                            return 0;
                        }

                        _log.WriteLine($"{_syncBackPrefix} Cursor created for {collectionKey}. Starting processing...", LogType.Verbose);

                        // 2. Process cursor
                        var result = await ProcessChangeStreamCursorAsync(
                            cursor,
                            mu,
                            sourceCollection,
                            targetCollection,
                            accumulatedChangesInColl,
                            ct,
                            seconds,
                            userFilterDoc,
                            counter
                        );

                        _log.WriteLine($"{_syncBackPrefix} Finished processing for {collectionKey}.", LogType.Verbose);

                        return result;
                    },
                    timeoutSeconds: seconds + 10, // Entire block must finish within seconds+ 10 seconds
                    operationName: $"WatchAndProcess({collectionKey})",
                    logAction: msg => _log.WriteLine($"{_syncBackPrefix}{msg}", LogType.Debug),
                    externalToken: cancellationToken
                );


                }
                catch (TimeoutException tex)
                {
                    // Clear accumulated data to prevent memory leak
                    int clearedCount = accumulatedChangesInColl.DocsToBeInserted.Count + 
                                            accumulatedChangesInColl.DocsToBeUpdated.Count + 
                                                 accumulatedChangesInColl.DocsToBeDeleted.Count;
                    _log.WriteLine($"{_syncBackPrefix}Clearing {clearedCount} accumulated documents due to watch timeout for {collectionKey}", LogType.Verbose);

                    ProcessWatchCollectionException(accumulatedChangesInColl, mu);

                    shouldProcessFinalBatch = false; // Skip finally block processing
                    
                    _log.WriteLine($"{_syncBackPrefix}Watch() timed out for {collectionKey} after {seconds + 10}s - will retry in next round. Exception: {tex.Message}", LogType.Debug);
                    
                    // Re-throw to allow outer handler to track consecutive timeouts
                    throw;
                }
                catch (Exception ex)
                {
                    _log.WriteLine($"{_syncBackPrefix}Failed to create change stream cursor for {collectionKey}: {ex}", LogType.Error);
                    _log.ShowInMonitor($"{_syncBackPrefix}ERROR: Failed to create cursor for {collectionKey}: {ex.Message}");

                    ProcessWatchCollectionException(accumulatedChangesInColl, mu);                    

                    shouldProcessFinalBatch = false; // Skip finally block processing
                    return; // Skip this collection, will retry in next batch
                }

            } // End of main try block
            catch (TimeoutException)
            {
                // TimeoutException is already handled in inner catch block
                // Re-throw to allow Task.Run wrapper to track consecutive timeouts
                throw;
            }
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
                    await ProcessWatchFinallyAsync(mu, sourceCollection, targetCollection, accumulatedChangesInColl, counter, collectionKey);
                }
            }
        }

        private BsonDocument[] CreateChangeStreamPipeline()
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

            return pipeline.ToArray();
        }

        private async Task<IChangeStreamCursor<ChangeStreamDocument<BsonDocument>>> CreateChangeStreamCursorAsync(
            IMongoCollection<BsonDocument> sourceCollection,
            BsonDocument[] pipelineArray,
            ChangeStreamOptions options,
            CancellationToken cancellationToken,
            string collectionKey)
        {
            return await Task.Run(() =>
            {
                _log.WriteLine($"[DEBUG] Starting Watch() for {collectionKey}...", LogType.Debug);
                try
                {
                    var cursor = sourceCollection.Watch<ChangeStreamDocument<BsonDocument>>(pipelineArray, options, cancellationToken);
                    _log.WriteLine($"[DEBUG] Successfully created Watch cursor for {collectionKey}.", LogType.Debug);
                    return cursor;
                }
                catch (OperationCanceledException)
                {
                    _log.WriteLine($"[DEBUG] Watch() cancelled for {collectionKey}.", LogType.Debug);
                    throw;
                }
                catch (Exception ex)
                {
                    _log.WriteLine($"[ERROR] Exception in Watch() for {collectionKey}: {ex}", LogType.Error);
                    throw;
                }
            }, cancellationToken);
        }

        private async Task<long> ProcessChangeStreamCursorAsync(
            IChangeStreamCursor<ChangeStreamDocument<BsonDocument>> cursor,
            MigrationUnit mu,
            IMongoCollection<BsonDocument> sourceCollection,
            IMongoCollection<BsonDocument> targetCollection,
            AccumulatedChangesTracker accumulatedChangesInColl,
            CancellationToken cancellationToken,
            int seconds,
            BsonDocument userFilterDoc,
            long counter)
        {

            string collectionKey = $"{mu.DatabaseName}.{mu.CollectionName}";
  
            using (cursor)
            {
                string lastProcessedToken = string.Empty;
                _log.WriteLine($"{_syncBackPrefix}Change stream cursor created successfully for {collectionKey}, starting enumeration", LogType.Verbose);

                if (_job.SourceServerVersion.StartsWith("3"))
                {
                    counter = await ProcessMongoDB3xChangeStreamAsync(cursor, mu, sourceCollection, targetCollection, accumulatedChangesInColl, cancellationToken, userFilterDoc, collectionKey, counter);
                }
                else
                {
                    counter = await ProcessMongoDB4xChangeStreamAsync(cursor, mu, sourceCollection, targetCollection, accumulatedChangesInColl, cancellationToken, seconds, userFilterDoc, collectionKey, counter);
                }
            }
            
            return counter;
        }

        private async Task<long> ProcessMongoDB3xChangeStreamAsync(
            IChangeStreamCursor<ChangeStreamDocument<BsonDocument>> cursor,
            MigrationUnit mu,
            IMongoCollection<BsonDocument> sourceCollection,
            IMongoCollection<BsonDocument> targetCollection,
            AccumulatedChangesTracker accumulatedChangesInColl,
            CancellationToken cancellationToken,
            BsonDocument userFilterDoc,
            string collectionKey,
            long counter)
        {

            foreach (var change in cursor.ToEnumerable(cancellationToken))
            {
                if (cancellationToken.IsCancellationRequested || ExecutionCancelled)
                {
                    _log.WriteLine($"{_syncBackPrefix}Change stream processing cancelled for {sourceCollection!.CollectionNamespace}", LogType.Info);
                    break; // Exit loop, let finally block handle cleanup
                }

                string lastProcessedToken = string.Empty;
                _resumeTokenCache.TryGetValue($"{sourceCollection!.CollectionNamespace}", out string? token1);
                lastProcessedToken = token1 ?? string.Empty;

                if (lastProcessedToken == change.ResumeToken.ToJson())
                {
                    mu.CSUpdatesInLastBatch = 0;
                    mu.CSNormalizedUpdatesInLastBatch = 0;
                    return counter; // Skip processing if the event has already been processed
                }

                try
                {
                    bool result = ProcessCursor(change, cursor, targetCollection, sourceCollection.CollectionNamespace.ToString(), mu, accumulatedChangesInColl, ref counter, userFilterDoc);
                    if (!result)
                        break; // Exit loop on error, let finally block handle cleanup
                }
                catch (Exception ex)
                {
                    _log.WriteLine($"{_syncBackPrefix}Exception in ProcessCursor for {collectionKey}: {ex.Message}", LogType.Error);
                    break; // Exit loop on exception, let finally block handle cleanup
                }

            }

            //Task.Run(async () =>
            await FlushPendingChangesAsync(mu, targetCollection, accumulatedChangesInColl);
            //);

            return counter;
        }

        private async Task<long> ProcessMongoDB4xChangeStreamAsync(IChangeStreamCursor<ChangeStreamDocument<BsonDocument>> cursor,
            MigrationUnit mu,
            IMongoCollection<BsonDocument> sourceCollection,
            IMongoCollection<BsonDocument> targetCollection,
            AccumulatedChangesTracker accumulatedChangesInColl,
            CancellationToken cancellationToken,
            int seconds,
            BsonDocument userFilterDoc,
            string collectionKey,
            long counter)
        {          

            using (cursor)
            {
                try
                {
                    // Iterate changes detected
                    while (!cancellationToken.IsCancellationRequested)
                    {
                        var hasNext = await cursor.MoveNextAsync(cancellationToken);
                        if (!hasNext)
                        {                            
                            break; // Stream closed or no more data
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

                            string lastProcessedToken = string.Empty;
                            _resumeTokenCache.TryGetValue($"{sourceCollection!.CollectionNamespace}", out string? token2);
                            lastProcessedToken = token2 ?? string.Empty;

                            if (lastProcessedToken == change.ResumeToken.ToJson() && _job.JobType != JobType.RUOptimizedCopy)
                            {
                                mu.CSUpdatesInLastBatch = 0;
                                mu.CSNormalizedUpdatesInLastBatch = 0;
                                return counter; // Skip processing if the event has already been processed
                            }

                            try
                            {
                                bool result = ProcessCursor(change, cursor, targetCollection, sourceCollection.CollectionNamespace.ToString(), mu, accumulatedChangesInColl, ref counter, userFilterDoc);
                                if (!result)
                                    break; // Exit loop on error, let finally block handle cleanup
                            }
                            catch (Exception ex)
                            {
                                _log.WriteLine($"{_syncBackPrefix}Exception in ProcessCursor for {collectionKey}: {ex.Message}", LogType.Error);
                                break; // Exit loop on exception, let finally block handle cleanup
                            }

                        }

                        await FlushPendingChangesAsync(mu, targetCollection, accumulatedChangesInColl);                    

                    }
                }
                catch (OperationCanceledException)
                {
                    // Cancellation requested - exit quietly
                }
                finally
                {

                   await FlushPendingChangesAsync(mu, targetCollection, accumulatedChangesInColl);

                }                
            }
            return counter;
        }

        private async Task ProcessWatchFinallyAsync(
            MigrationUnit mu,
            IMongoCollection<BsonDocument> sourceCollection,
            IMongoCollection<BsonDocument> targetCollection,
            AccumulatedChangesTracker accumulatedChangesInColl,
            long counter,
            string collectionKey)
        {
            try
            {
                int totalChanges = accumulatedChangesInColl.DocsToBeInserted.Count +
                                  accumulatedChangesInColl.DocsToBeUpdated.Count +
                                  accumulatedChangesInColl.DocsToBeDeleted.Count;

                if (totalChanges > 0)
                {
                    _log.ShowInMonitor($"{_syncBackPrefix}Processing batch for {sourceCollection.CollectionNamespace}: {totalChanges} changes (I:{accumulatedChangesInColl.DocsToBeInserted.Count}, U:{accumulatedChangesInColl.DocsToBeUpdated.Count}, D:{accumulatedChangesInColl.DocsToBeDeleted.Count})");
                    _log.WriteLine($"{_syncBackPrefix}Final batch processing - Total: {totalChanges}, Inserts: {accumulatedChangesInColl.DocsToBeInserted.Count}, Updates: {accumulatedChangesInColl.DocsToBeUpdated.Count}, Deletes: {accumulatedChangesInColl.DocsToBeDeleted.Count}", LogType.Debug);
                }

                await FlushPendingChangesAsync(mu, targetCollection, accumulatedChangesInColl);

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

        private bool ProcessCursor(ChangeStreamDocument<BsonDocument> change, IChangeStreamCursor<ChangeStreamDocument<BsonDocument>> cursor, IMongoCollection<BsonDocument> targetCollection, string collNameSpace, MigrationUnit mu, AccumulatedChangesTracker accumulatedChangesInColl, ref long counter, BsonDocument userFilterDoc)
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

                // Throttle UI updates and log change operation details
                bool shouldUpdateUI = ShowInMonitor(change, collNameSpace, timeStamp, counter);
                

                ProcessChange(change, targetCollection, collNameSpace, accumulatedChangesInColl, _job.IsSimulatedRun, mu);

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
                StopProcessing = true;
                return false;
            }
        }

        

        private void ProcessChange(ChangeStreamDocument<BsonDocument> change, IMongoCollection<BsonDocument> targetCollection, string collNameSpace, AccumulatedChangesTracker accumulatedChangesInColl, bool isWriteSimulated, MigrationUnit mu)
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
                            accumulatedChangesInColl.AddInsert(change);
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
                            accumulatedChangesInColl.AddUpdate(change);
                        }
                        break;
                    case ChangeStreamOperationType.Delete:
                        IncrementEventCounter(mu, change.OperationType);
                        // Accumulate deletes even in simulation mode so counters get updated
                        accumulatedChangesInColl.AddDelete(change);
                        break;
                    default:
                        _log.WriteLine($"{_syncBackPrefix}Unhandled operation type: {change.OperationType}", LogType.Error);
                        break;
                }
                
            }
            catch (Exception ex)
            {
                _log.WriteLine($"{_syncBackPrefix}Error processing operation {change.OperationType} on {collNameSpace} with _id {idValue}. Details: {ex}", LogType.Error);
            }
        }       
    }
}
