using MongoDB.Bson;
using MongoDB.Driver;
using OnlineMongoMigrationProcessor.Context;
using OnlineMongoMigrationProcessor.Helpers.JobManagement;
using OnlineMongoMigrationProcessor.Helpers.Mongo;
using OnlineMongoMigrationProcessor.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using static OnlineMongoMigrationProcessor.Helpers.Mongo.MongoHelper;

#pragma warning disable CS8602 // Dereference of a possibly null reference.

namespace OnlineMongoMigrationProcessor
{
    public class CollectionLevelChangeStreamProcessor : ChangeStreamProcessor
    {
        private Dictionary<string, AccumulatedChangesTracker> _accumulatedChangesPerCollection = new Dictionary<string, AccumulatedChangesTracker>();

        public CollectionLevelChangeStreamProcessor(Log log, MongoClient sourceClient, MongoClient targetClient, ActiveMigrationUnitsCache muCache, MigrationSettings config, bool syncBack = false)
            : base(log, sourceClient, targetClient, muCache, config, syncBack)
        {
        }

        protected override async Task ProcessChangeStreamsAsync(CancellationToken token)
        {
            
            _log.WriteLine($"{_syncBackPrefix}Collections to process: {string.Join(", ", _migrationUnitsToProcess.Keys)}", LogType.Verbose);

            bool isVCore = (_syncBack ? MigrationJobContext.CurrentlyActiveJob.TargetEndpoint : MigrationJobContext.CurrentlyActiveJob.SourceEndpoint)
                .Contains("mongocluster.cosmos.azure.com", StringComparison.OrdinalIgnoreCase);
            _log.WriteLine($"{_syncBackPrefix}Environment detection - IsVCore: {isVCore}, SyncBack: {_syncBack}", LogType.Debug);

            int index = 0;

            // Get the latest sorted keys
            var sortedKeys = _migrationUnitsToProcess
                .OrderByDescending(kvp => kvp.Value) //value is CSNormalizedUpdatesInLastBatch
                .Select(kvp => kvp.Key)
                .ToList();

            _log.WriteLine($"{_syncBackPrefix}Starting collection-level change stream processing for {sortedKeys.Count} collection(s). Each round-robin batch will process {Math.Min(_concurrentProcessors, sortedKeys.Count)} collections. Max duration per batch {_processorRunMaxDurationInSec} seconds.", LogType.Info);

            long loops = 0;

            while (!token.IsCancellationRequested && !ExecutionCancelled)
            {
                var totalKeys = sortedKeys.Count;


                while (index < totalKeys && !token.IsCancellationRequested && !ExecutionCancelled)
                {
                    var tasks = new List<Task>();
                    var collectionProcessed = new List<string>();

                    // Determine the batch
                    var batchKeys = sortedKeys.Skip(index).Take(_concurrentProcessors).ToList();


                    //total of  _migrationUnitsToProcess
                    long totalUpdatesInAll = _migrationUnitsToProcess.Sum(kvp => kvp.Value);
                    //filter _migrationUnitsToProcess for batchKeys and calculate total of value.
                    long totalUpdatesInBatch = _migrationUnitsToProcess
                        .Where(kvp => batchKeys.Contains(kvp.Key))
                        .Sum(kvp => kvp.Value);

                    float timeFactor = totalUpdatesInAll > 0 ? (float)totalUpdatesInBatch / totalUpdatesInAll : 1;
                    _log.WriteLine($"{_syncBackPrefix}Batch load calculation - TotalUpdatesInBatch: {totalUpdatesInBatch}, TotalUpdatesInAll: {totalUpdatesInAll}, TimeFactor: {timeFactor:F3}", LogType.Verbose);

                    int seconds = GetBatchDurationInSeconds(timeFactor);
                    _log.WriteLine($"{_syncBackPrefix}Batch duration computed - Seconds: {seconds}, Collections: {batchKeys.Count}", LogType.Debug);


                    foreach (var key in batchKeys)
                    {
                        if (_migrationUnitsToProcess.ContainsKey(key))
                        {
                            var mu = MigrationJobContext.MigrationUnitsCache.GetMigrationUnit(key);
                            
                            var collectionKey = $"{mu.DatabaseName}.{mu.CollectionName}";
                            
                            // Check if resume token setup is still pending - if so, skip this collection
                            if (WaitForResumeTokenTaskDelegate != null)
                            {
                                // Create a completed task to check if resume token is ready without waiting
                                var checkTask = WaitForResumeTokenTaskDelegate(collectionKey);
                                if (!checkTask.IsCompleted)
                                {
                                    _log.ShowInMonitor($"{_syncBackPrefix}Skipping collection {collectionKey} - resume token not yet ready");
                                    continue; // Skip this collection and move to the next one
                                }
                                // If task is completed, await it to ensure any exceptions are observed
                                await checkTask;
                            }
                            
                            collectionProcessed.Add(collectionKey);
                            // Initialize accumulated changes tracker if not present
                            if (!_accumulatedChangesPerCollection.ContainsKey(key))
                            {
                                _accumulatedChangesPerCollection[key] = new AccumulatedChangesTracker(key);
                            }

                            mu.CSLastBatchDurationSeconds = seconds; // Store the factor for each mu

                           
                            // Don't pass token to Task.Run - each collection manages its own timeout via CancellationTokenSource inside SetChangeStreamOptionandWatch
                            string collectionName = key; // Capture for closure


                            try
                            {
                                var task = Task.Run(async () =>
                                {
                                    try
                                    {
                                        _log.WriteLine($"{_syncBackPrefix}Starting SetChangeStreamOptionandWatch for {collectionKey}", LogType.Verbose);
                                        await SetChangeStreamOptionandWatch(mu, true, seconds);
                                    }
                                    catch (Exception ex) when (ex is TimeoutException)
                                    {
                                        // Handle timeout exceptions separately if needed
                                        _log.WriteLine($"{_syncBackPrefix}TimeoutException in Task.Run for collection {collectionKey}: {ex.Message}", LogType.Debug);
                                    }
                                    catch (Exception ex)
                                    {
                                        _log.WriteLine($"{_syncBackPrefix}Unhandled exception in Task.Run for collection {collectionKey}: {ex}", LogType.Error);
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

                    try
                    {
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
                                    if(baseEx is TimeoutException)
                                    {
                                        _log.WriteLine($"{_syncBackPrefix}Task {i} {baseEx?.Message}", LogType.Verbose);
                                    }
                                    else
                                        _log.WriteLine($"{_syncBackPrefix}Task {i} FAULTED: {baseEx?.Message}", LogType.Debug);
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
                    //cleanup the processes migrationUnits from cache
                    foreach (var key in batchKeys)
                    {
                        MigrationJobContext.MigrationUnitsCache.RemoveMigrationUnit(key);
                    }
                    index += _concurrentProcessors;

                    // Pause between batches to allow memory recovery and reduce CPU spikes
                    // Increased to 5000ms to address OOM issues and server CPU spikes
                    Thread.Sleep(5000);
                }
                loops++;
                _log.WriteLine($"{_syncBackPrefix}Completed round {loops} of change stream processing for all {totalKeys} collection(s). Starting a new round; collections are sorted by their previous batch change counts.");

                //static collections resume tokens need adjustment
                AdjustCusrsorTimeForStaticCollections();

                index = 0;
                // Sort the dictionary after all processing is complete
                sortedKeys = _migrationUnitsToProcess
                    .OrderByDescending(kvp => kvp.Value)
                    .Select(kvp => kvp.Key)
                    .ToList();

            }
        }

        private bool AdjustCusrsorTimeForStaticCollections()
        {
            try
            {
                foreach (var unitId in _migrationUnitsToProcess.Keys)
                {
                    var mu = MigrationJobContext.GetMigrationUnit(MigrationJobContext.CurrentlyActiveJob.Id, unitId);
                    if (mu == null)
                        continue;
                    
                    mu.ParentJob = MigrationJobContext.CurrentlyActiveJob;
                    if (!_syncBack)
                    {
                        TimeSpan gap;
                        if (mu.CursorUtcTimestamp > DateTime.MinValue)
                            gap = DateTime.UtcNow - mu.CursorUtcTimestamp.AddHours(mu.CSAddHours);
                        else if (mu.ChangeStreamStartedOn.HasValue)
                            gap = DateTime.UtcNow - mu.ChangeStreamStartedOn.Value.AddHours(mu.CSAddHours);
                        else
                            continue;

                        if (gap.TotalMinutes > (60 * 24) && mu.CSUpdatesInLastBatch == 0)
                        {
                            mu.CSAddHours += 22;
                            mu.ResumeToken = string.Empty; //clear resume token to use timestamp
                            _log.WriteLine($"{_syncBackPrefix}24 hour change stream lag with no updates detected for {mu.DatabaseName}.{mu.CollectionName} - pushed by 22 hours", LogType.Warning);
                        }
                    }
                    else
                    {
                        TimeSpan gap;
                        if (mu.CursorUtcTimestamp > DateTime.MinValue)
                            gap = DateTime.UtcNow - mu.SyncBackCursorUtcTimestamp.AddHours(mu.SyncBackAddHours);
                        else if (mu.SyncBackChangeStreamStartedOn.HasValue)
                            gap = DateTime.UtcNow - mu.SyncBackChangeStreamStartedOn.Value.AddHours(mu.SyncBackAddHours);
                        else
                            continue;

                        if (gap.TotalMinutes > (60 * 24) && mu.CSUpdatesInLastBatch == 0)
                        {
                            mu.SyncBackAddHours += 22;
                            mu.ResumeToken = string.Empty; //clear resume token to use timestamp
                            _log.WriteLine($"{_syncBackPrefix}24 hour change stream lag with no updates detected for {mu.DatabaseName}.{mu.CollectionName} - pushed by 22 hours", LogType.Warning);
                        }
                    }

                    MigrationJobContext.SaveMigrationUnit(mu,false);
                }
                return true;
            }
            catch (Exception ex)
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

                    if (!MigrationJobContext.CurrentlyActiveJob.IsSimulatedRun)
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
                        version = MigrationJobContext.CurrentlyActiveJob.SourceServerVersion;
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

                    if (!mu.InitialDocumenReplayed && !MigrationJobContext.CurrentlyActiveJob.IsSimulatedRun && MigrationJobContext.CurrentlyActiveJob.ChangeStreamMode != ChangeStreamMode.Aggressive)
                    {
                        _log.WriteLine($"{_syncBackPrefix}Auto-replaying first change for {collectionKey} - ResumeDocId: {mu.ResumeDocumentId}, Operation: {mu.ResumeTokenOperation}", LogType.Verbose);
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
                            MigrationJobContext.SaveMigrationUnit(mu,false);
                            _log.WriteLine($"{_syncBackPrefix}Auto-replay successful for {collectionKey}, proceeding with change stream", LogType.Verbose);
                        }
                        else
                        {
                            _log.WriteLine($"{_syncBackPrefix}Failed to replay the first change for {sourceCollection!.CollectionNamespace}. Skipping change stream processing for this collection.", LogType.Error);
                            throw new Exception($"Failed to replay the first change for {sourceCollection!.CollectionNamespace}. Skipping change stream processing for this collection.");
                        }
                    }
                    else
                    {
                        _log.WriteLine($"{_syncBackPrefix}Skipping auto-replay for {collectionKey} - InitialDocReplayed: {mu.InitialDocumenReplayed}, IsSimulated: {MigrationJobContext.CurrentlyActiveJob.IsSimulatedRun}, ChangeStreamMode: {MigrationJobContext.CurrentlyActiveJob.ChangeStreamMode}", LogType.Verbose);
                    }

                    if (timeStamp > DateTime.MinValue && !mu.ResetChangeStream && string.IsNullOrEmpty(resumeToken) && !(MigrationJobContext.CurrentlyActiveJob.JobType == JobType.RUOptimizedCopy && !MigrationJobContext.CurrentlyActiveJob.ProcessingSyncBack)) //skip CursorUtcTimestamp if its reset 
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
                    else if (startedOn > DateTime.MinValue && !version.StartsWith("3") && !(MigrationJobContext.CurrentlyActiveJob.JobType == JobType.RUOptimizedCopy && !MigrationJobContext.CurrentlyActiveJob.ProcessingSyncBack))  //newer version
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

                        MigrationJobContext.SaveMigrationUnit(mu,true);
                    }

                    using var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(seconds));
                    CancellationToken cancellationToken = cancellationTokenSource.Token;                    

                    // In simulated runs, use source collection as a placeholder to avoid null target warnings
                    if (MigrationJobContext.CurrentlyActiveJob.IsSimulatedRun && targetCollection == null)
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
        }

        private async Task FlushPendingChangesAsync(MigrationUnit mu, IMongoCollection<BsonDocument> targetCollection, AccumulatedChangesTracker accumulatedChangesInColl, bool isFinalFlush)
        {            

            // Flush accumulated changes - convert Dictionary.Values to List for BulkProcessChangesAsync
            await BulkProcessChangesAsync(
                mu,
                targetCollection,
                insertEvents: accumulatedChangesInColl.DocsToBeInserted.Values.ToList(),
                updateEvents: accumulatedChangesInColl.DocsToBeUpdated.Values.ToList(),
                deleteEvents: accumulatedChangesInColl.DocsToBeDeleted.Values.ToList(),
                accumulatedChangesInColl: accumulatedChangesInColl,
                batchSize: 500);

            // Update resume token after successful flush
            if (!string.IsNullOrEmpty(accumulatedChangesInColl.LatestResumeToken))
            {
            	
                if (!_syncBack)
                {
                    // We don't allow going backwards in time
                    if (accumulatedChangesInColl.LatestTimestamp - mu.CursorUtcTimestamp >= TimeSpan.FromSeconds(0))
                    {
                        mu.CursorUtcTimestamp = accumulatedChangesInColl.LatestTimestamp;
                        mu.ResumeToken = accumulatedChangesInColl.LatestResumeToken;
                        
                        MigrationJobContext.SaveMigrationUnit(mu,true);
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
                        
                        MigrationJobContext.SaveMigrationUnit(mu,true);
                    }
                    else
                    {
                        string collectionNamespace = $"{mu.DatabaseName}.{mu.CollectionName}";
                        _log.WriteLine($"Old Token:{mu.SyncBackResumeToken}, New Token:{accumulatedChangesInColl.LatestResumeToken} for {collectionNamespace}", LogType.Error);
                        throw new Exception($"{_syncBackPrefix} Timestamp mismatch Old Value: {mu.SyncBackCursorUtcTimestamp} is newer than New Value: {accumulatedChangesInColl.LatestTimestamp} for {collectionNamespace}");
                    }
                }
                _resumeTokenCache[$"{targetCollection.CollectionNamespace}"] = accumulatedChangesInColl.LatestResumeToken;
            }

            // Clear collections to free memory
            accumulatedChangesInColl.Reset(isFinalFlush);
        }

        

        private async Task WatchCollection(MigrationUnit mu, ChangeStreamOptions options, IMongoCollection<BsonDocument> sourceCollection, IMongoCollection<BsonDocument> targetCollection, CancellationToken cancellationToken, int seconds)
        {
            string collectionKey = $"{mu.DatabaseName}.{mu.CollectionName}";
            _log.WriteLine($"{_syncBackPrefix}WatchCollection started for {collectionKey} - Duration: {seconds}s, ResumeToken: {(!string.IsNullOrEmpty(mu.ResumeToken) ? "SET" : "NOT SET")}", LogType.Verbose);

            bool isVCore = (_syncBack ? MigrationJobContext.CurrentlyActiveJob.TargetEndpoint : MigrationJobContext.CurrentlyActiveJob.SourceEndpoint)
                .Contains("mongocluster.cosmos.azure.com", StringComparison.OrdinalIgnoreCase);

            //long counter = 0;
            BsonDocument userFilterDoc = MongoHelper.GetFilterDoc(mu.UserFilter);

            if (!_accumulatedChangesPerCollection.ContainsKey(collectionKey))
            {
                _accumulatedChangesPerCollection[collectionKey] = new AccumulatedChangesTracker(collectionKey);
            }

            var accumulatedChangesInColl = _accumulatedChangesPerCollection[collectionKey];
            bool shouldProcessFinalBatch = true; // Flag to control finally block execution

            //reset latency counters
            accumulatedChangesInColl.CSTotalReadDurationInMS = 0;
            accumulatedChangesInColl.CSTotaWriteDurationInMS = 0;
            accumulatedChangesInColl.Reset();

            // creating the watch cursor
            System.Diagnostics.Stopwatch readStopwatch = new System.Diagnostics.Stopwatch();

            try
            {
                var pipelineArray = CreateChangeStreamPipeline();

                try
				{
                    var sucess = await MongoSafeTaskExecutor.ExecuteAsync(
                    async (ct) =>
                    {
                        _log.WriteLine($"{_syncBackPrefix}Starting cursor creation for {collectionKey}", LogType.Verbose);

                        readStopwatch.Start();

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
                            return false;
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
                            readStopwatch
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
                catch (Exception ex) when (ex is TimeoutException || ex is OperationCanceledException)
                {
                    // Clear accumulated data to prevent memory leak
                    //int clearedCount = accumulatedChangesInColl.DocsToBeInserted.Count +
                    //                        accumulatedChangesInColl.DocsToBeUpdated.Count +
                    //                             accumulatedChangesInColl.DocsToBeDeleted.Count;
                    //_log.WriteLine($"{_syncBackPrefix}Clearing {accumulatedChangesInColl} accumulated documents due to watch timeout for {collectionKey}", LogType.Debug);

                    //await ProcessWatchFinallyAsync(mu, sourceCollection, targetCollection, accumulatedChangesInColl, collectionKey,false);

                    //shouldProcessFinalBatch = false; // Skip finally block processing

                    _log.WriteLine($"{_syncBackPrefix}Watch() timed out for {collectionKey} - will retry in next batch", LogType.Debug);
                    //return; // Skip this collection, will retry in next batch
					throw;
                }
                catch(Exception ex) when (ex.Message.Contains("CollectionScan died due to position in capped collection being deleted"))
                {
                    _log.WriteLine($"{_syncBackPrefix}Failed to create change stream cursor for {collectionKey}: CollectionScan died due to position in capped collection being deleted ", LogType.Warning);
                    _log.WriteLine($"{_syncBackPrefix}Failed to create change stream cursor for {collectionKey}: {ex}", LogType.Debug);
                    //ProcessWatchCollectionException(accumulatedChangesInColl, mu);

                    //shouldProcessFinalBatch = false; // Skip finally block processing
                    //return; // Skip this collection, will retry in next batch
                }
                catch (Exception ex)
                {
                    _log.WriteLine($"{_syncBackPrefix}Failed to create change stream cursor for {collectionKey}: {ex}", LogType.Debug);

                    //ProcessWatchCollectionException(accumulatedChangesInColl, mu);

                    //shouldProcessFinalBatch = false; // Skip finally block processing
                    //return; // Skip this collection, will retry in next batch
                }

			}
            catch (TimeoutException)
            {
                // TimeoutException is already handled in inner catch block
                // Re-throw to allow Task.Run wrapper to track consecutive timeouts
                //await ProcessWatchFinallyAsync(mu, sourceCollection, targetCollection, accumulatedChangesInColl, collectionKey, false);
                throw;
            }
            catch (OperationCanceledException ex)
            {
                _log.WriteLine($"{_syncBackPrefix}OperationCanceledException in WatchCollection for {sourceCollection!.CollectionNamespace}: {ex.Message}", LogType.Debug);
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
                    // Note: readStopwatch time is already accumulated in ProcessMongoDB3x/4xChangeStreamAsync
                    // No need to accumulate here to avoid double-counting
                    await ProcessWatchFinallyAsync(mu, sourceCollection, targetCollection, accumulatedChangesInColl, collectionKey,true);
                }
            }
        }

        private BsonDocument[] CreateChangeStreamPipeline()
        {
            List<BsonDocument> pipeline;
            if (MigrationJobContext.CurrentlyActiveJob.JobType == JobType.RUOptimizedCopy)
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
                _log.WriteLine($"Starting Watch() for {collectionKey}...", LogType.Debug);
                try
                {
                    var cursor = sourceCollection.Watch<ChangeStreamDocument<BsonDocument>>(pipelineArray, options, cancellationToken);
                    _log.WriteLine($"Successfully created Watch cursor for {collectionKey}.", LogType.Debug);
                    return cursor;
                }
                catch(Exception ex) when (ex is OperationCanceledException || ex is TimeoutException)
                {
                    _log.WriteLine($"Watch() cancelled for {collectionKey}.", LogType.Debug);
                    throw;
                }
                catch (Exception ex)
                {
                    _log.WriteLine($"Exception in Watch() for {collectionKey}: {ex}", LogType.Error);
                    throw;
                }
            }, cancellationToken);
        }

        private async Task<bool> ProcessChangeStreamCursorAsync(
            IChangeStreamCursor<ChangeStreamDocument<BsonDocument>> cursor,
            MigrationUnit mu,
            IMongoCollection<BsonDocument> sourceCollection,
            IMongoCollection<BsonDocument> targetCollection,
            AccumulatedChangesTracker accumulatedChangesInColl,
            CancellationToken cancellationToken,
            int seconds,
            BsonDocument userFilterDoc,
            System.Diagnostics.Stopwatch readStopwatch)
        {

            string collectionKey = $"{mu.DatabaseName}.{mu.CollectionName}";
            var sucess = false;
            using (cursor)
            {
                string lastProcessedToken = string.Empty;
                _log.WriteLine($"{_syncBackPrefix}Change stream cursor created successfully for {collectionKey}, starting enumeration", LogType.Verbose);

                if (MigrationJobContext.CurrentlyActiveJob.SourceServerVersion.StartsWith("3"))
                {
                    sucess = await ProcessMongoDB3xChangeStreamAsync(cursor, mu, sourceCollection, targetCollection, accumulatedChangesInColl, cancellationToken, userFilterDoc, collectionKey, readStopwatch);
                }
                else
                {
                    sucess = await ProcessMongoDB4xChangeStreamAsync(cursor, mu, sourceCollection, targetCollection, accumulatedChangesInColl, cancellationToken, seconds, userFilterDoc, collectionKey, readStopwatch);
                }
            }

            return sucess;
        }

        private async Task<bool> ProcessMongoDB3xChangeStreamAsync(
            IChangeStreamCursor<ChangeStreamDocument<BsonDocument>> cursor,
            MigrationUnit mu,
            IMongoCollection<BsonDocument> sourceCollection,
            IMongoCollection<BsonDocument> targetCollection,
            AccumulatedChangesTracker accumulatedChangesInColl,
            CancellationToken cancellationToken,
            BsonDocument userFilterDoc,
            string collectionKey,
            System.Diagnostics.Stopwatch readStopwatch)
        {

            long flushedCount = 0;
            foreach (var change in cursor.ToEnumerable(cancellationToken))
            {
               
                // Stop the read stopwatch immediately after getting the change from source
                readStopwatch.Stop();
                accumulatedChangesInColl.CSTotalReadDurationInMS += readStopwatch.ElapsedMilliseconds;
                
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
                    _log.ShowInMonitor($"{_syncBackPrefix}Skipping already processed change for {sourceCollection!.CollectionNamespace}");
                    //mu.CSUpdatesInLastBatch = 0;
                    //mu.CSNormalizedUpdatesInLastBatch = 0;

                    //MigrationJobContext.SaveMigrationUnit(mu,true);

                    return true; // Skip processing if the event has already been processed
                }

                try
                {
                    bool result = ProcessCursor(change, cursor, targetCollection, sourceCollection.CollectionNamespace.ToString(), mu, accumulatedChangesInColl, userFilterDoc);
                    if (!result)
                        break; // Exit loop on error, let finally block handle cleanup
                }
                catch (Exception ex)
                {
                    _log.WriteLine($"{_syncBackPrefix}Exception in ProcessCursor for {collectionKey}: {ex.Message}", LogType.Error);
                    break; // Exit loop on exception, let finally block handle cleanup
                }

                if((accumulatedChangesInColl.TotalChangesCount - flushedCount) > _config.ChangeStreamMaxDocsInBatch)
                {
                    flushedCount = flushedCount + accumulatedChangesInColl.TotalChangesCount;
                    _log.WriteLine($"{_syncBackPrefix}Flushing accumulated changes - Count: {accumulatedChangesInColl.TotalChangesCount} exceeds max: {_config.ChangeStreamMaxDocsInBatch} for {collectionKey}", LogType.Debug);
                    await FlushPendingChangesAsync(mu, targetCollection, accumulatedChangesInColl,false);
                    //changeCount = 0; // Reset change count after flush  
                }

                // Restart stopwatch for next read iteration
                readStopwatch.Restart();
            }

            readStopwatch.Stop();

            return true;
        }       

        private async Task<bool> ProcessMongoDB4xChangeStreamAsync(IChangeStreamCursor<ChangeStreamDocument<BsonDocument>> cursor,
            MigrationUnit mu,
            IMongoCollection<BsonDocument> sourceCollection,
            IMongoCollection<BsonDocument> targetCollection,
            AccumulatedChangesTracker accumulatedChangesInColl,
            CancellationToken cancellationToken,
            int seconds,
            BsonDocument userFilterDoc,
            string collectionKey,
            System.Diagnostics.Stopwatch readStopwatch)
        {

            using (cursor)
            {
                try
                {
                    long flushedCount = 0;
                    //int changeCount = 0;
                    // Iterate changes detected
                    while (!cancellationToken.IsCancellationRequested)
                    {
                        var hasNext = await cursor.MoveNextAsync(cancellationToken);
                        if (!hasNext)
                        {
                            readStopwatch.Stop();
                            break; // Stream closed or no more data
                        }
                        
                        // Stop the read stopwatch after reading from source is complete
                        // Only accumulate read time when we actually got data (hasNext == true)
                        readStopwatch.Stop();
                        accumulatedChangesInColl.CSTotalReadDurationInMS += readStopwatch.ElapsedMilliseconds;
                        
                        
                        foreach (var change in cursor.Current)
                        {
                            //changeCount++;

                            if (cancellationToken.IsCancellationRequested || ExecutionCancelled)
                            {
                                _log.WriteLine($"{_syncBackPrefix}Change stream processing cancelled for {sourceCollection!.CollectionNamespace}", LogType.Debug);
                                break; // Exit inner loop, outer loop will also break
                            }

                            string lastProcessedToken = string.Empty;
                            _resumeTokenCache.TryGetValue($"{sourceCollection!.CollectionNamespace}", out string? token2);
                            lastProcessedToken = token2 ?? string.Empty;

                            if (lastProcessedToken == change.ResumeToken.ToJson() && MigrationJobContext.CurrentlyActiveJob.JobType != JobType.RUOptimizedCopy)
                            {
                                //mu.CSUpdatesInLastBatch = 0;
                                //mu.CSNormalizedUpdatesInLastBatch = 0;

                                //MigrationJobContext.SaveMigrationUnit(mu,true);

                                return true; // Skip processing if the event has already been processed
                            }

                            try
                            {
                                bool result = ProcessCursor(change, cursor, targetCollection, sourceCollection.CollectionNamespace.ToString(), mu, accumulatedChangesInColl, userFilterDoc);
                                if (!result)
                                    break; // Exit loop on error, let finally block handle cleanup
                            }
                            catch (Exception ex)
                            {
                                _log.WriteLine($"{_syncBackPrefix}Exception in ProcessCursor for {collectionKey}: {ex.Message}", LogType.Error);
                                break; // Exit loop on exception, let finally block handle cleanup
                            }

                            // Check if we need to flush accumulated changes to prevent memory buildup
                            if ((accumulatedChangesInColl.TotalChangesCount - flushedCount) > _config.ChangeStreamMaxDocsInBatch)
                            {
                                flushedCount = flushedCount + accumulatedChangesInColl.TotalChangesCount;
                                _log.WriteLine($"{_syncBackPrefix}Flushing accumulated changes - Count: {accumulatedChangesInColl.TotalChangesCount} exceeds max: {_config.ChangeStreamMaxDocsInBatch} for {collectionKey}", LogType.Debug);
                                await FlushPendingChangesAsync(mu, targetCollection, accumulatedChangesInColl,false);
                                //changeCount = 0; // Reset change count after flush  
                            }

                        }
                        
                        
                        
                        // Restart the stopwatch for the next read iteration
                        readStopwatch.Restart();

                    }
                    
                    readStopwatch.Stop();
                }
                catch (OperationCanceledException)
                {
                    // Cancellation requested - exit quietly
                }
                finally
                {
                    readStopwatch.Stop();
                    await FlushPendingChangesAsync(mu, targetCollection, accumulatedChangesInColl,false);
                }
            }
            return true;
        }

        private async Task ProcessWatchFinallyAsync(
            MigrationUnit mu,
            IMongoCollection<BsonDocument> sourceCollection,
            IMongoCollection<BsonDocument> targetCollection,
            AccumulatedChangesTracker accumulatedChangesInColl,
            string collectionKey,
            bool isFinalFlush)
        {
            try
            {

                long eventCounter = accumulatedChangesInColl.TotalEventCount;// TotalEventCount will get reset in FlushPendingChangesAsync
                if (eventCounter > 0)
                {
                    _log.ShowInMonitor($"{_syncBackPrefix}Processing batch for {sourceCollection.CollectionNamespace}:{eventCounter} events, {accumulatedChangesInColl.TotalChangesCount} changes (I:{accumulatedChangesInColl.DocsToBeInserted.Count}, U:{accumulatedChangesInColl.DocsToBeUpdated.Count}, D:{accumulatedChangesInColl.DocsToBeDeleted.Count})");
                    _log.WriteLine($"{_syncBackPrefix}Final batch processing - Events: {eventCounter} Total: {accumulatedChangesInColl.TotalChangesCount}, Inserts: {accumulatedChangesInColl.DocsToBeInserted.Count}, Updates: {accumulatedChangesInColl.DocsToBeUpdated.Count}, Deletes: {accumulatedChangesInColl.DocsToBeDeleted.Count}", LogType.Debug);
                }

                await FlushPendingChangesAsync(mu, targetCollection, accumulatedChangesInColl, isFinalFlush);

                mu.CSUpdatesInLastBatch = eventCounter; 
                mu.CSNormalizedUpdatesInLastBatch = (long)(eventCounter / (mu.CSLastBatchDurationSeconds > 0 ? mu.CSLastBatchDurationSeconds : 1));


                // Transfer latency metrics from accumulatedChangesInColl to mu
                if (eventCounter > 0)
                {
                    mu.CSAvgReadLatencyInMS = Math.Round((double)accumulatedChangesInColl.CSTotalReadDurationInMS / eventCounter,2);
                    mu.CSAvgWriteLatencyInMS = Math.Round((double)accumulatedChangesInColl.CSTotaWriteDurationInMS / eventCounter,2);
                }

                MigrationJobContext.SaveMigrationUnit(mu,true);
                _log.WriteLine($"{_syncBackPrefix}Batch counters updated - CSUpdatesInLastBatch: {eventCounter}, CSNormalizedUpdatesInLastBatch: {mu.CSNormalizedUpdatesInLastBatch} for {collectionKey}", LogType.Verbose);
                

                if (eventCounter > 0)
                {
                    _log.ShowInMonitor($"{_syncBackPrefix}Watch cycle completed for {sourceCollection.CollectionNamespace}: {eventCounter} events processed in batch. Avg Read Latency: {mu.CSAvgReadLatencyInMS} ms | Avg Write Latency: {mu.CSAvgWriteLatencyInMS} ms");
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
                _log.WriteLine($"Auto replay is empty for {sourceCollection.CollectionNamespace}.", LogType.Debug);
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

        private bool ProcessCursor(ChangeStreamDocument<BsonDocument> change, IChangeStreamCursor<ChangeStreamDocument<BsonDocument>> cursor, IMongoCollection<BsonDocument> targetCollection, string collNameSpace, MigrationUnit mu, AccumulatedChangesTracker accumulatedChangesInColl, BsonDocument userFilterDoc)
        {
            try
            {
                _log.WriteLine($"{_syncBackPrefix}ProcessCursor - OperationType: {change.OperationType}, DocumentKey: {change.DocumentKey?.ToJson() ?? "null"}, Counter: {accumulatedChangesInColl.TotalEventCount + 1}", LogType.Verbose);
                //check if user filter condition is met
                if (change.OperationType != ChangeStreamOperationType.Delete)
                {
                    if (userFilterDoc.Elements.Count() > 0
                        && !MongoHelper.CheckForUserFilterMatch(change.FullDocument, userFilterDoc))
                        return true;
                }

                //counter++;

                DateTime timeStamp = DateTime.MinValue;

                if (!MigrationJobContext.CurrentlyActiveJob.SourceServerVersion.StartsWith("3") && change.ClusterTime != null)
                {
                    timeStamp = MongoHelper.BsonTimestampToUtcDateTime(change.ClusterTime); // Convert BsonTimestamp to DateTime
                }
                else if (!MigrationJobContext.CurrentlyActiveJob.SourceServerVersion.StartsWith("3") && change.WallTime != null) //for 4.0 and above
                {
                    timeStamp = change.WallTime.Value; // Use WallTime for 4.0 and above
                }

                // Throttle UI updates and log change operation details
                // Copy counter to local variable to avoid CS1628 (cannot use ref parameter in lambda)
                //long currentCounter = counter;
                bool shouldUpdateUI = Task.Run(() => ShowInMonitor(change, collNameSpace, timeStamp, accumulatedChangesInColl.TotalEventCount+1)).Result;


                ProcessChange(change, targetCollection, collNameSpace, accumulatedChangesInColl, MigrationJobContext.CurrentlyActiveJob.IsSimulatedRun, mu);

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
                                _log.WriteLine($"{_syncBackPrefix}Processing {change.OperationType} operation for {collNameSpace} with _id {idValue}. No document found on source.", LogType.Info);
                                //var deleteTTLFilter = Builders<BsonDocument>.Filter.Eq("_id", idValue);
                                //try
                                //{
                                //    targetCollection.DeleteOne(deleteTTLFilter);
                                //    IncrementEventCounter(mu, ChangeStreamOperationType.Delete);
                                //}
                                //catch
                                //{ }
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
