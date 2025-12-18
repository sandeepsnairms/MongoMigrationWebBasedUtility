using MongoDB.Bson;
using MongoDB.Driver;
using OnlineMongoMigrationProcessor.Context;
using OnlineMongoMigrationProcessor.Helpers.JobManagement;
using OnlineMongoMigrationProcessor.Helpers.Mongo;
using OnlineMongoMigrationProcessor.Models;
using OnlineMongoMigrationProcessor.Workers;
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

        private MongoClient _changeStreamMongoClient;

        public CollectionLevelChangeStreamProcessor(Log log, MongoClient sourceClient, MongoClient targetClient, ActiveMigrationUnitsCache muCache, MigrationSettings config, bool syncBack = false, MigrationWorker? migrationWorker = null)
            : base(log, sourceClient, targetClient, muCache, config, syncBack, migrationWorker)
        {
            MigrationJobContext.AddVerboseLog($"CollectionLevelChangeStreamProcessor: Constructor called, syncBack={syncBack}");
            // Initialize the change stream client based on syncBack mode
            var mj = MigrationJobContext.CurrentlyActiveJob;
            string connectionString = _syncBack
                ? MigrationJobContext.TargetConnectionString[mj.Id]
                : MigrationJobContext.SourceConnectionString[mj.Id];

            _changeStreamMongoClient = MongoClientFactory.Create(
                _log,
                connectionString,
                false,
                _syncBack ? null : _config.CACertContentsForSourceServer);
        }

        protected override async Task ProcessChangeStreamsAsync(CancellationToken token)
        {
            MigrationJobContext.AddVerboseLog("CollectionLevelChangeStreamProcessor.ProcessChangeStreamsAsync: starting");
            WriteBasicLog();

            int index = 0;
            var sortedKeys = GetSortedCollectionKeys();

            LogProcessingConfiguration(sortedKeys.Count);

            long loops = 0;
            long emptyLoops = 0;
            DateTime lastResumeTokenCheck = DateTime.MinValue;

            while (!token.IsCancellationRequested && !ExecutionCancelled)
            {
                var totalKeys = sortedKeys.Count;

                // Handle empty sortedKeys case
                if (totalKeys == 0)
                {
                    var result = await HandleEmptyCollectionKeys(emptyLoops, lastResumeTokenCheck, token);
                    sortedKeys = result.sortedKeys;
                    emptyLoops = result.emptyLoops;
                    lastResumeTokenCheck = result.lastResumeTokenCheck;
                    continue;
                }

                // Reset empty loops counter when we have collections to process
                emptyLoops = ResetEmptyLoopsCounterIfNeeded(emptyLoops, totalKeys);

                while (index < totalKeys && !token.IsCancellationRequested && !ExecutionCancelled)
                {
                    var batchKeys = sortedKeys.Skip(index).Take(_concurrentProcessors).ToList();
                    int seconds = CalculateBatchDuration(batchKeys);

                    var (tasks, collectionProcessed) = await PrepareCollectionTasks(batchKeys, seconds, token);

                    await ExecuteBatchTasks(tasks, collectionProcessed, seconds);

                    CleanupProcessedCollections(batchKeys);

                    index += _concurrentProcessors;

                    // Pause between batches to allow memory recovery and reduce CPU spikes
                    // Increased to 5000ms to address OOM issues and server CPU spikes
                    Thread.Sleep(5000);
                    
                }
                
                loops++;
                LogRoundCompletion(loops, totalKeys);

                
                // Initialize resume tokens for migration units without them, after 1st loop and  then every 4 loops
                if (loops==1||loops % 4 == 0)
                {
                    _ = InitializeResumeTokensForUnsetUnitsAsync(token);
                    lastResumeTokenCheck = DateTime.UtcNow;
                }

                //static collections resume tokens need adjustment
                AdjustCusrsorTimeForStaticCollections();


                index = 0;
                sortedKeys = GetSortedCollectionKeys();
            }
        }

        private async Task<(List<string> sortedKeys, long emptyLoops, DateTime lastResumeTokenCheck)> HandleEmptyCollectionKeys(long emptyLoops, DateTime lastResumeTokenCheck, CancellationToken token)
        {
            emptyLoops++;

            var loopDurationSec=Math.Max(60, _config.ChangeStreamBatchDurationMin);
            _log.ShowInMonitor($"{_syncBackPrefix}No collections with resume tokens found (empty loop #{emptyLoops}). Waiting {loopDurationSec} seconds before rechecking.");

            _ = InitializeResumeTokensForUnsetUnitsAsync(token);
            //lastResumeTokenCheck = await CheckAndInitializeResumeTokensIfNeeded(emptyLoops, lastResumeTokenCheck, token);

            // Wait for loopDurationSec before checking again
            await Task.Delay(loopDurationSec * 1000, token);
            
            // Recheck for collections with resume tokens
            var sortedKeys = GetSortedCollectionKeys();
            return (sortedKeys, emptyLoops, lastResumeTokenCheck);
        }

        //private async Task<DateTime> CheckAndInitializeResumeTokensIfNeeded(long emptyLoops, DateTime lastResumeTokenCheck, CancellationToken token)
        //{
        //    // Check if we should initialize resume tokens (every 10 loops or every 10 minutes)
        //    TimeSpan timeSinceLastCheck = DateTime.UtcNow - lastResumeTokenCheck;
        //    if (emptyLoops % 10 == 0 || timeSinceLastCheck.TotalMinutes >= 10)
        //    {                
        //        _= InitializeResumeTokensForUnsetUnitsAsync(token);
        //        return DateTime.UtcNow;
        //    }
        //    return lastResumeTokenCheck;
        //}

        private long ResetEmptyLoopsCounterIfNeeded(long emptyLoops, int totalKeys)
        {
            if (emptyLoops > 0)
            {
                _log.WriteLine($"{_syncBackPrefix}Resuming processing with {totalKeys} collection(s) after {emptyLoops} empty loops", LogType.Info);
                return 0;
            }
            return emptyLoops;
        }
       


        private void WriteBasicLog()
        {

            bool isVCore = (_syncBack ? MigrationJobContext.CurrentlyActiveJob.TargetEndpoint : MigrationJobContext.CurrentlyActiveJob.SourceEndpoint)
                .Contains("mongocluster.cosmos.azure.com", StringComparison.OrdinalIgnoreCase);

            _log.WriteLine($"{_syncBackPrefix}Environment detection - IsVCore: {isVCore}, SyncBack: {_syncBack}", LogType.Debug);
        }

        private List<string> GetSortedCollectionKeys()
        {
            return _migrationUnitsToProcess
                .Where(kvp =>
                {
                    var mu = MigrationJobContext.CurrentlyActiveJob.MigrationUnitBasics.FirstOrDefault(m => m.Id == kvp.Key);
                    if (mu == null)
                        return false;                    
                   
                    
                    // Check cursor timestamp based on syncBack mode
                    bool hasCursorTimestamp = _syncBack 
                        ? mu.SyncBackCursorUtcTimestamp > DateTime.MinValue 
                        : mu.CursorUtcTimestamp > DateTime.MinValue;
                    
                    return hasCursorTimestamp;
                })
                .OrderByDescending(kvp => kvp.Value) //value is CSNormalizedUpdatesInLastBatch
                .Select(kvp => kvp.Key)
                .ToList();
        }

        private void LogProcessingConfiguration(int collectionCount)
        {
            _log.WriteLine($"{_syncBackPrefix}Starting collection-level change stream processing for {collectionCount} collection(s). Each round-robin batch will process {Math.Min(_concurrentProcessors, collectionCount)} collections. Max duration per batch {_processorRunMaxDurationInSec} seconds. Collections without a resume token will be skipped and rechecked every 4 rounds.", LogType.Info);
        }

        private int CalculateBatchDuration(List<string> batchKeys)
        {
            MigrationJobContext.AddVerboseLog($"CollectionLevelChangeStreamProcessor.CalculateBatchDuration: batchKeys.Count={batchKeys.Count}");
            long totalUpdatesInAll = _migrationUnitsToProcess.Sum(kvp => kvp.Value);
            long totalUpdatesInBatch = _migrationUnitsToProcess
                .Where(kvp => batchKeys.Contains(kvp.Key))
                .Sum(kvp => kvp.Value);

            float timeFactor = totalUpdatesInAll > 0 ? (float)totalUpdatesInBatch / totalUpdatesInAll : 1;
            
            int seconds = GetBatchDurationInSeconds(timeFactor);

            return seconds;
        }

        private async Task<(List<Task> tasks, List<string> collectionProcessed)> PrepareCollectionTasks(List<string> batchKeys, int seconds, CancellationToken token)
        {
            MigrationJobContext.AddVerboseLog($"CollectionLevelChangeStreamProcessor.PrepareCollectionTasks: batchKeys.Count={batchKeys.Count}, seconds={seconds}");
            var tasks = new List<Task>();
            var collectionProcessed = new List<string>();

            foreach (var key in batchKeys)
            {
                if (_migrationUnitsToProcess.ContainsKey(key))
                {
                    var mu = MigrationJobContext.MigrationUnitsCache.GetMigrationUnit(key);
                    var collectionKey = $"{mu.DatabaseName}.{mu.CollectionName}";

                    // Check if resume token setup is still pending - if so, skip this collection
                    if (!await IsResumeTokenReady(collectionKey))
                    {
                        continue;
                    }

                    collectionProcessed.Add(collectionKey);
                    InitializeAccumulatedChangesTracker(collectionKey);

                    mu.CSLastBatchDurationSeconds = seconds;

                    var task = CreateCollectionProcessingTask(mu, collectionKey, seconds);
                    tasks.Add(task);
                }
            }

            return (tasks, collectionProcessed);
        }

        private async Task<bool> IsResumeTokenReady(string collectionKey)
        {
            MigrationJobContext.AddVerboseLog($"CollectionLevelChangeStreamProcessor.IsResumeTokenReady: collectionKey={collectionKey}");
            if (WaitForResumeTokenTaskDelegate != null)
            {
                var checkTask = WaitForResumeTokenTaskDelegate(collectionKey);
                if (!checkTask.IsCompleted)
                {
                    _log.ShowInMonitor($"{_syncBackPrefix}Skipping collection {collectionKey} - resume token not yet ready");
                    return false;
                }
                await checkTask;
            }
            return true;
        }

        private void InitializeAccumulatedChangesTracker(string collectionKey)
        {
            MigrationJobContext.AddVerboseLog($"CollectionLevelChangeStreamProcessor.InitializeAccumulatedChangesTracker: collectionKey={collectionKey}");
            lock (_accumulatedChangesPerCollection)
            {
                if (!_accumulatedChangesPerCollection.ContainsKey(collectionKey))
                {
                    _accumulatedChangesPerCollection[collectionKey] = new AccumulatedChangesTracker(collectionKey);
                }
            }
        }

        private Task CreateCollectionProcessingTask(MigrationUnit mu, string collectionKey, int seconds)
        {
            MigrationJobContext.AddVerboseLog($"CollectionLevelChangeStreamProcessor.CreateCollectionProcessingTask: collectionKey={collectionKey}, seconds={seconds}");
            return Task.Run(async () =>
            {
                try
                {
                      await SetChangeStreamOptionandWatch(mu, true, seconds);
                }
                catch (Exception ex) when (ex is TimeoutException)
                {
                    _log.WriteLine($"{_syncBackPrefix}TimeoutException in Task.Run for collection {collectionKey}: {ex.Message}", LogType.Debug);
                }
                catch (Exception ex)
                {
                    _log.WriteLine($"{_syncBackPrefix}Unhandled exception in Task.Run for collection {collectionKey}: {ex}", LogType.Error);
                    throw;
                }
            });
        }

        private async Task ExecuteBatchTasks(List<Task> tasks, List<string> collectionProcessed, int seconds)
        {
            _log.WriteLine($"{_syncBackPrefix}Processing change streams for {collectionProcessed.Count} collections: {string.Join(", ", collectionProcessed)}, collections without a resume token have been skipped. Batch Duration {seconds} seconds", LogType.Info);

            try
            {
                await Task.WhenAll(tasks);
                _log.WriteLine($"{_syncBackPrefix}Completed processing change streams for collections: {string.Join(", ", collectionProcessed)}. Batch Duration {seconds} seconds", LogType.Debug);
            }
            catch (Exception ex)
            {
                _log.WriteLine($"{_syncBackPrefix}Task.WhenAll threw exception: {ex.Message}", LogType.Error);
                LogTaskStates(tasks);
                throw;
            }
        }

        private void LogTaskStates(List<Task> tasks)
        {
            for (int i = 0; i < tasks.Count; i++)
            {
                try
                {
                    var task = tasks[i];

                    if (task.IsFaulted)
                    {
                        var baseEx = task.Exception?.GetBaseException();
                        if (baseEx is TimeoutException)
                        {
                            _log.WriteLine($"{_syncBackPrefix}Task {i} {baseEx?.Message}", LogType.Debug);
                        }
                        else
                        {
                            _log.WriteLine($"{_syncBackPrefix}Task {i} FAULTED: {baseEx?.Message}", LogType.Debug);
                        }
                    }
                    else if (task.IsCanceled)
                    {
                        _log.WriteLine($"{_syncBackPrefix}Task {i} CANCELED", LogType.Warning);
                    }
                }
                catch
                {
                    // Ignore exceptions during logging
                }
            }
        }

        private void CleanupProcessedCollections(List<string> batchKeys)
        {
            MigrationJobContext.AddVerboseLog($"CollectionLevelChangeStreamProcessor.CleanupProcessedCollections: batchKeys.Count={batchKeys.Count}");
            foreach (var key in batchKeys)
            {
                MigrationJobContext.MigrationUnitsCache.RemoveMigrationUnit(key);
            }
        }

        private void LogRoundCompletion(long loops, int totalKeys)
        {
            MigrationJobContext.AddVerboseLog($"CollectionLevelChangeStreamProcessor.LogRoundCompletion: loops={loops}, totalKeys={totalKeys}");
            _log.WriteLine($"{_syncBackPrefix}Completed round {loops} of change stream processing for all {totalKeys} collection(s). Starting a new round; collections are sorted by their previous batch change counts. Collections without a resume token will be skipped.");
        }

        private bool AdjustCusrsorTimeCollection(MigrationUnit mu, bool force=false)
        {
            MigrationJobContext.AddVerboseLog($"CollectionLevelChangeStreamProcessor.AdjustCusrsorTimeCollection: muId={mu?.Id}, collection={mu?.DatabaseName}.{mu?.CollectionName}, force={force}");
            try
            {                
                if (mu == null)
                    return false;

                mu.ParentJob = MigrationJobContext.CurrentlyActiveJob;
                if (!_syncBack)
                {
                    TimeSpan gap;
                    if (mu.CursorUtcTimestamp > DateTime.MinValue)
                        gap = DateTime.UtcNow - mu.CursorUtcTimestamp.AddHours(mu.CSAddHours);
                    else if (mu.ChangeStreamStartedOn.HasValue)
                        gap = DateTime.UtcNow - mu.ChangeStreamStartedOn.Value.AddHours(mu.CSAddHours);
                    else
                        return false;

                    if (gap.TotalMinutes > (60 * 24) && mu.CSUpdatesInLastBatch == 0)
                    {
                        mu.CSAddHours += 22;
                        mu.ResumeToken = string.Empty; //clear resume token to use timestamp
                        _log.WriteLine($"{_syncBackPrefix}24 hour change stream lag with no updates detected for {mu.DatabaseName}.{mu.CollectionName} - pushed by 22 hours", LogType.Warning);
                    }

                    if (force && gap.TotalMinutes < (60 *24) && mu.CSUpdatesInLastBatch == 0)
                    {
                        mu.CSAddHours += 22; ;
                        mu.ResumeToken = string.Empty; //clear resume token to use timestamp
                        _log.WriteLine($"{_syncBackPrefix}Force reset of CSAddHours for {mu.DatabaseName}.{mu.CollectionName}", LogType.Warning);
                    }
                    else
                    {
                        _log.WriteLine($"{_syncBackPrefix}No adjustment possible for {mu.DatabaseName}.{mu.CollectionName} - Gap: {gap.TotalHours:F2} hours", LogType.Debug);
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
                        return false;

                    if (gap.TotalMinutes > (60 * 24) && mu.CSUpdatesInLastBatch == 0)
                    {
                        mu.SyncBackAddHours += 22;
                        mu.ResumeToken = string.Empty; //clear resume token to use timestamp
                        _log.WriteLine($"{_syncBackPrefix}24 hour change stream lag with no updates detected for {mu.DatabaseName}.{mu.CollectionName} - pushed by 22 hours", LogType.Warning);
                    }
                    if (force && gap.TotalMinutes < (60 * 24) && mu.CSUpdatesInLastBatch == 0)
                    {
                        mu.SyncBackAddHours += 22;
                        mu.ResumeToken = string.Empty; //clear resume token to use timestamp
                        _log.WriteLine($"{_syncBackPrefix}Force reset of SyncBackAddHours for {mu.DatabaseName}.{mu.CollectionName}", LogType.Warning);
                    }
                    else
                    {
                        _log.WriteLine($"{_syncBackPrefix}No adjustment possible for {mu.DatabaseName}.{mu.CollectionName} - Gap: {gap.TotalHours:F2} hours", LogType.Debug);
                    }
                }

                MigrationJobContext.SaveMigrationUnit(mu, false);
                return true;
            }
            catch (Exception ex)
            {
                _log.WriteLine($"{_syncBackPrefix}Error adjusting cursor time for static collection {mu.DatabaseName}.{mu.CollectionName}: {ex}", LogType.Error);
                StopProcessing = true;
                return false;
            }
        }

        private async Task InitializeResumeTokensForUnsetUnitsAsync(CancellationToken token)
        {
            MigrationJobContext.AddVerboseLog($"CollectionLevelChangeStreamProcessor.InitializeResumeTokensForUnsetUnitsAsync: starting, unitsToProcess={_migrationUnitsToProcess.Count}");
            try
            {
                bool shownlog = false;
                foreach (var unitId in _migrationUnitsToProcess.Keys)
                {
                    if (token.IsCancellationRequested || ExecutionCancelled)
                        break;

                    var mu = MigrationJobContext.MigrationUnitsCache.GetMigrationUnit(unitId);
                    if (mu == null)
                        continue;

                    // Check if both ResumeToken and OriginalResumeToken are not set
                    if (string.IsNullOrEmpty(mu.ResumeToken) && string.IsNullOrEmpty(mu.OriginalResumeToken))
                    {
                        if(shownlog==false)
                        {
                            _log.WriteLine($"{_syncBackPrefix}Rechecking collections without a resume token; these collections were previously skipped.", LogType.Info);
                            shownlog = true;
                        }

                        MigrationJobContext.AddVerboseLog(($"{_syncBackPrefix}Setting resume token for {mu.DatabaseName}.{mu.CollectionName} (no tokens set)"));
                        
                        try
                        {
                            await MongoHelper.SetChangeStreamResumeTokenAsync(
                                _log,
                                _syncBack ? _targetClient : _sourceClient,
                                MigrationJobContext.CurrentlyActiveJob,
                                mu,
                                30,
                                token,
                                true);
                        }
                        catch (Exception ex)
                        {
                           // do nothing
                        }
                    }

                    //remove from cache
                    MigrationJobContext.MigrationUnitsCache.RemoveMigrationUnit(mu.Id);
                }
                
            }
            catch (Exception ex)
            {
                _log.WriteLine($"{_syncBackPrefix}Error in InitializeResumeTokensForUnsetUnitsAsync: {ex.Message}", LogType.Error);
            }
        }

        private bool AdjustCusrsorTimeForStaticCollections()
        {
            MigrationJobContext.AddVerboseLog($"CollectionLevelChangeStreamProcessor.AdjustCusrsorTimeForStaticCollections: unitsToProcess={_migrationUnitsToProcess.Count}");
            try
            {
                foreach (var unitId in _migrationUnitsToProcess.Keys)
                {
                    var mu = MigrationJobContext.GetMigrationUnit(MigrationJobContext.CurrentlyActiveJob.Id, unitId);
                    AdjustCusrsorTimeCollection(mu);
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
            _log.WriteLine($"{_syncBackPrefix}SetChangeStreamOptionandWatch started for {collectionKey} - IsCSProcessingRun: {IsCSProcessingRun}, Seconds: {seconds}", LogType.Debug);

            try
            {
                var (changeStreamCollection, targetCollection) = GetCollectionsForChangeStream(mu);
                
                try
                {
                    seconds = CalculateBatchDuration(seconds, collectionKey);
                    var options = await ConfigureChangeStreamOptionsAsync(mu, seconds, collectionKey, changeStreamCollection, targetCollection);
                    
                    using var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(seconds));
                    CancellationToken cancellationToken = cancellationTokenSource.Token;

                    await WatchCollection(mu, options, changeStreamCollection!, targetCollection!, cancellationToken, seconds);
                }
				catch (TimeoutException tex)
                {
                    _log.WriteLine($"{_syncBackPrefix}TimeoutException caught in SetChangeStreamOptionandWatch for {collectionKey}: {tex.Message}", LogType.Debug);
                    throw;
                }
                catch (OperationCanceledException ex)
                {
                    _log.WriteLine($"{_syncBackPrefix}OperationCanceledException in SetChangeStreamOptionandWatch for {collectionKey}: {ex.Message}", LogType.Info);
                }
                catch (MongoCommandException ex) when (ex.ToString().Contains("Resume of change stream was not possible"))
                {
                    _log.WriteLine($"{_syncBackPrefix}Oplog is full. Error processing change stream for {collectionKey}. Details: {ex}", LogType.Error);
                    _log.ShowInMonitor($"{_syncBackPrefix}Oplog is full. Error processing change stream for {collectionKey}. Details: {ex}");
                    StopProcessing = true;
                }
                catch (MongoCommandException ex) when (ex.Message.Contains("Expired resume token") || ex.Message.Contains("cursor"))
                {
                    _log.WriteLine($"{_syncBackPrefix}Resume token has expired or cursor is invalid for {collectionKey}.", LogType.Error);
                    _log.ShowInMonitor($"{_syncBackPrefix}Resume token has expired or cursor is invalid for {collectionKey}.");
                }
            }
            catch (TimeoutException)
            {
                throw;
            }
            catch (Exception ex)
            {
                _log.WriteLine($"{_syncBackPrefix}Error processing change stream for {mu.DatabaseName}.{mu.CollectionName}. Details: {ex}", LogType.Error);
                StopProcessing = true;
            }
        }

        private (IMongoCollection<BsonDocument>? changeStreamCollection, IMongoCollection<BsonDocument>? targetCollection) GetCollectionsForChangeStream(MigrationUnit mu)
        {
            string databaseName = mu.DatabaseName;
            string collectionName = mu.CollectionName;

            IMongoDatabase targetDb;
            IMongoDatabase changeStreamDb;
            IMongoCollection<BsonDocument>? targetCollection = null;
            IMongoCollection<BsonDocument>? changeStreamCollection = null;

            if (!_syncBack)
            {
                changeStreamDb = _changeStreamMongoClient.GetDatabase(databaseName);
                changeStreamCollection = changeStreamDb.GetCollection<BsonDocument>(collectionName);

                if (!MigrationJobContext.CurrentlyActiveJob.IsSimulatedRun)
                {
                    targetDb = _targetClient.GetDatabase(databaseName);
                    targetCollection = targetDb.GetCollection<BsonDocument>(collectionName);
                }
            }
            else
            {
                targetDb = _sourceClient.GetDatabase(databaseName);
                targetCollection = targetDb.GetCollection<BsonDocument>(collectionName);
                
                changeStreamDb = _changeStreamMongoClient.GetDatabase(databaseName);
                changeStreamCollection = changeStreamDb.GetCollection<BsonDocument>(collectionName);
            }

            return (changeStreamCollection, targetCollection);
        }

        private int CalculateBatchDuration(int seconds, string collectionKey)
        {
            if (seconds == 0)
                seconds = GetBatchDurationInSeconds(.5f);

            int maxAwaitSeconds = Math.Max(5, (int)(seconds * 0.8));
            _log.WriteLine($"{_syncBackPrefix}ChangeStream timing - TotalDuration: {seconds}s, MaxAwaitTime: {maxAwaitSeconds}s for {collectionKey}", LogType.Debug);

            return seconds;
        }

        private async Task<ChangeStreamOptions> ConfigureChangeStreamOptionsAsync(MigrationUnit mu, int seconds, string collectionKey, IMongoCollection<BsonDocument> changeStreamCollection, IMongoCollection<BsonDocument>? targetCollection)
        {
            MigrationJobContext.AddVerboseLog($"CollectionLevelChangeStreamProcessor.ConfigureChangeStreamOptionsAsync: collectionKey={collectionKey}, seconds={seconds}");
            int maxAwaitSeconds = Math.Max(5, (int)(seconds * 0.8));
            ChangeStreamOptions options = new ChangeStreamOptions { BatchSize = 500, FullDocument = ChangeStreamFullDocumentOption.UpdateLookup, MaxAwaitTime = TimeSpan.FromSeconds(maxAwaitSeconds) };

            var (timeStamp, resumeToken, version, startedOn) = GetResumeParameters(mu);

            await HandleAutoReplayIfNeeded(mu, collectionKey, targetCollection);

            options = DetermineResumeStrategy(mu, timeStamp, resumeToken, version, startedOn, maxAwaitSeconds, collectionKey);

            // In simulated runs, use change stream collection as a placeholder
            if (MigrationJobContext.CurrentlyActiveJob.IsSimulatedRun && targetCollection == null)
            {
                targetCollection = changeStreamCollection;
            }

            return options;
        }

        private (DateTime timeStamp, string resumeToken, string version, DateTime startedOn) GetResumeParameters(MigrationUnit mu)
        {
            DateTime timeStamp;
            string resumeToken;
            string? version;
            DateTime startedOn;

            if (!_syncBack)
            {
                timeStamp = mu.CursorUtcTimestamp.AddHours(mu.CSAddHours);
                resumeToken = mu.ResumeToken ?? string.Empty;
                version = MigrationJobContext.CurrentlyActiveJob.SourceServerVersion;
                startedOn = mu.ChangeStreamStartedOn.HasValue 
                    ? mu.ChangeStreamStartedOn.Value.AddHours(mu.CSAddHours) 
                    : DateTime.MinValue;
            }
            else
            {
                timeStamp = mu.SyncBackCursorUtcTimestamp.AddHours(mu.SyncBackAddHours);
                resumeToken = mu.SyncBackResumeToken ?? string.Empty;
                version = "8";
                startedOn = mu.SyncBackChangeStreamStartedOn.HasValue 
                    ? mu.SyncBackChangeStreamStartedOn.Value.AddHours(mu.SyncBackAddHours) 
                    : DateTime.MinValue;
            }

            return (timeStamp, resumeToken, version!, startedOn);
        }

        private async Task HandleAutoReplayIfNeeded(MigrationUnit mu, string collectionKey, IMongoCollection<BsonDocument>? targetCollection)
        {

            if (!mu.InitialDocumenReplayed && 
                !MigrationJobContext.CurrentlyActiveJob.IsSimulatedRun && 
                MigrationJobContext.CurrentlyActiveJob.ChangeStreamMode != ChangeStreamMode.Aggressive)
            {
                _log.WriteLine($"{_syncBackPrefix}Auto-replaying first change for {collectionKey} - ResumeDocId: {mu.ResumeDocumentId}, Operation: {mu.ResumeTokenOperation}", LogType.Debug);
                
                if (targetCollection == null)
                {
                    var targetDb2 = _targetClient.GetDatabase(mu.DatabaseName);
                    targetCollection = targetDb2.GetCollection<BsonDocument>(mu.CollectionName);
                }
                
                var replaySourceClient = _syncBack ? _targetClient : _sourceClient;
                var replaySourceDb = replaySourceClient.GetDatabase(mu.DatabaseName);
                var replaySourceCollection = replaySourceDb.GetCollection<BsonDocument>(mu.CollectionName);
                
                if (AutoReplayFirstChangeInResumeToken(mu.ResumeDocumentId, mu.ResumeTokenOperation, replaySourceCollection, targetCollection!, mu))
                {
                    mu.InitialDocumenReplayed = true;
                    MigrationJobContext.SaveMigrationUnit(mu, false);
                    _log.WriteLine($"{_syncBackPrefix}Auto-replay successful for {collectionKey}, proceeding with change stream", LogType.Debug);
                }
                else
                {
                    _log.WriteLine($"{_syncBackPrefix}Failed to replay the first change for {collectionKey}. Skipping change stream processing for this collection.", LogType.Error);
                    throw new Exception($"Failed to replay the first change for {collectionKey}. Skipping change stream processing for this collection.");
                }
            }
            else
            {
                _log.WriteLine($"{_syncBackPrefix}Skipping auto-replay for {collectionKey} - InitialDocReplayed: {mu.InitialDocumenReplayed}, IsSimulated: {MigrationJobContext.CurrentlyActiveJob.IsSimulatedRun}, ChangeStreamMode: {MigrationJobContext.CurrentlyActiveJob.ChangeStreamMode}", LogType.Debug);
            }
        }

        private ChangeStreamOptions DetermineResumeStrategy(MigrationUnit mu, DateTime timeStamp, string resumeToken, string version, DateTime startedOn, int maxAwaitSeconds, string collectionKey)
        {
            ChangeStreamOptions options;

            if (timeStamp > DateTime.MinValue && !mu.ResetChangeStream && string.IsNullOrEmpty(resumeToken) && 
                !(MigrationJobContext.CurrentlyActiveJob.JobType == JobType.RUOptimizedCopy && !MigrationJobContext.CurrentlyActiveJob.ProcessingSyncBack))
            {
                var bsonTimestamp = MongoHelper.ConvertToBsonTimestamp(timeStamp.ToLocalTime());
                options = new ChangeStreamOptions { BatchSize = 500, FullDocument = ChangeStreamFullDocumentOption.UpdateLookup, StartAtOperationTime = bsonTimestamp, MaxAwaitTime = TimeSpan.FromSeconds(maxAwaitSeconds) };
                _log.WriteLine($"{_syncBackPrefix}Resume strategy: StartAtOperationTime - Timestamp: {timeStamp} for {collectionKey}", LogType.Debug);
            }
            else if (!string.IsNullOrEmpty(resumeToken) && !mu.ResetChangeStream)
            {
                options = new ChangeStreamOptions { BatchSize = 500, FullDocument = ChangeStreamFullDocumentOption.UpdateLookup, ResumeAfter = BsonDocument.Parse(resumeToken), MaxAwaitTime = TimeSpan.FromSeconds(maxAwaitSeconds) };
                _log.WriteLine($"{_syncBackPrefix}Resume strategy: ResumeAfter token for {collectionKey}", LogType.Debug);
            }
            else if (string.IsNullOrEmpty(resumeToken) && version.StartsWith("3"))
            {
                options = new ChangeStreamOptions { BatchSize = 500, FullDocument = ChangeStreamFullDocumentOption.UpdateLookup, MaxAwaitTime = TimeSpan.FromSeconds(maxAwaitSeconds) };
                _log.WriteLine($"{_syncBackPrefix}Resume strategy: No resume (MongoDB 3.x) for {collectionKey}", LogType.Debug);
            }
            else if (startedOn > DateTime.MinValue && !version.StartsWith("3") && 
                     !(MigrationJobContext.CurrentlyActiveJob.JobType == JobType.RUOptimizedCopy && !MigrationJobContext.CurrentlyActiveJob.ProcessingSyncBack))
            {
                var bsonTimestamp = MongoHelper.ConvertToBsonTimestamp(startedOn);
                options = new ChangeStreamOptions { BatchSize = 500, FullDocument = ChangeStreamFullDocumentOption.UpdateLookup, StartAtOperationTime = bsonTimestamp, MaxAwaitTime = TimeSpan.FromSeconds(maxAwaitSeconds) };
                _log.WriteLine($"{_syncBackPrefix}Resume strategy: StartAtOperationTime from ChangeStreamStartedOn - StartedOn: {startedOn} for {collectionKey}", LogType.Debug);
                
                if (mu.ResetChangeStream)
                {
                    ResetCounters(mu);
                    _log.WriteLine($"{_syncBackPrefix}Counters reset for {collectionKey}", LogType.Debug);
                }

                mu.ResetChangeStream = false;
                MigrationJobContext.SaveMigrationUnit(mu, true);
            }
            else
            {
                options = new ChangeStreamOptions { BatchSize = 500, FullDocument = ChangeStreamFullDocumentOption.UpdateLookup, MaxAwaitTime = TimeSpan.FromSeconds(maxAwaitSeconds) };
            }

            return options;
        }

        private async Task FlushPendingChangesAsync(MigrationUnit mu, IMongoCollection<BsonDocument> targetCollection, AccumulatedChangesTracker accumulatedChangesInColl, bool isFinalFlush)
        {
            MigrationJobContext.AddVerboseLog($"CollectionLevelChangeStreamProcessor.FlushPendingChangesAsync: collection={mu.DatabaseName}.{mu.CollectionName}, totalChanges={accumulatedChangesInColl.TotalChangesCount}, isFinalFlush={isFinalFlush}");
            
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

        

        private async Task WatchCollection(MigrationUnit mu, ChangeStreamOptions options, IMongoCollection<BsonDocument> changeStreamCollection, IMongoCollection<BsonDocument> targetCollection, CancellationToken cancellationToken, int seconds)
        {
            string collectionKey = $"{mu.DatabaseName}.{mu.CollectionName}";
            _log.WriteLine($"{_syncBackPrefix}WatchCollection started for {collectionKey} - Duration: {seconds}s, ResumeToken: {(!string.IsNullOrEmpty(mu.ResumeToken) ? "SET" : "NOT SET")}", LogType.Debug);

            bool isVCore = (_syncBack ? MigrationJobContext.CurrentlyActiveJob.TargetEndpoint : MigrationJobContext.CurrentlyActiveJob.SourceEndpoint)
                .Contains("mongocluster.cosmos.azure.com", StringComparison.OrdinalIgnoreCase);

            //long counter = 0;
            BsonDocument userFilterDoc = MongoHelper.GetFilterDoc(mu.UserFilter);

            // Thread-safe initialization and retrieval
            AccumulatedChangesTracker accumulatedChangesInColl;
            lock (_accumulatedChangesPerCollection)
            {
                if (!_accumulatedChangesPerCollection.ContainsKey(collectionKey))
                {
                    _accumulatedChangesPerCollection[collectionKey] = new AccumulatedChangesTracker(collectionKey);
                }
                accumulatedChangesInColl = _accumulatedChangesPerCollection[collectionKey];
            }
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
                    //last time chnage stream was checked
                    mu.CSLastChecked=DateTime.UtcNow;

                    var sucess = await MongoSafeTaskExecutor.ExecuteAsync(
                    async (ct) =>
                    {
                        MigrationJobContext.AddVerboseLog($"{_syncBackPrefix}Starting cursor creation for {collectionKey}");

                        readStopwatch.Start();

                        IChangeStreamCursor<ChangeStreamDocument<BsonDocument>>? cursor = null;
                        try
                        {
                            // 1. Create cursor
                            cursor = await CreateChangeStreamCursorAsync(
                                changeStreamCollection,
                                pipelineArray,
                                options,
                                ct,
                                collectionKey
                            );
                            if (cursor == null)
                            {
                                MigrationJobContext.AddVerboseLog($"{_syncBackPrefix}Cursor is null for {collectionKey}");
                                return false;
                            }

                            MigrationJobContext.AddVerboseLog($"{_syncBackPrefix} Cursor created for {collectionKey}. Starting processing...");

                            // 2. Process cursor
                            var result = await ProcessChangeStreamCursorAsync(
                                cursor,
                                mu,
                                changeStreamCollection,
                                targetCollection,
                                accumulatedChangesInColl,
                                ct,
                                seconds,
                                userFilterDoc,
                                readStopwatch
                            );

                            MigrationJobContext.AddVerboseLog(($"{_syncBackPrefix} Finished processing for {collectionKey}."));

                            return result;
                        }
                        finally
                        {
                            // Ensure cursor is disposed even on timeout/cancellation
                            if (cursor != null)
                            {
                                try
                                {
                                    cursor.Dispose();                                    
                                }
                                catch (Exception ex)
                                {
                                    //do nothing
                                }
                            }
                        }
                    },
                    timeoutSeconds: seconds + 10, // Entire block must finish within seconds+ 10 seconds
                    operationName: $"WatchAndProcess({collectionKey})",
                    logAction: msg => MigrationJobContext.AddVerboseLog(($"{_syncBackPrefix}{msg}")),
                    externalToken: cancellationToken
                    ); 
                }
                catch (Exception ex) when (ex is TimeoutException || ex is OperationCanceledException)
                {
                    MigrationJobContext.AddVerboseLog(($"{_syncBackPrefix}Watch() timed out for {collectionKey} - will retry in next batch"));
					throw;
                }
                catch(Exception ex) when (ex.Message.Contains("CollectionScan died due to position in capped collection being deleted"))
                {
                    _log.ShowInMonitor($"{_syncBackPrefix}Change stream position invalidated for {collectionKey} - oplog position was deleted. Will push and retry in next batch.");
                    
                    AdjustCusrsorTimeCollection(mu,true); 
                }
                catch (Exception ex)
                {
                    _log.WriteLine($"{_syncBackPrefix}Failed to create change stream cursor for {collectionKey}: {ex}", LogType.Debug);

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
                MigrationJobContext.AddVerboseLog("{_syncBackPrefix}OperationCanceledException in WatchCollection for {changeStreamCollection!.CollectionNamespace}: {ex.Message}");
            }
            catch (Exception ex)
            {
                _log.WriteLine($"{_syncBackPrefix}Exception in WatchCollection for {changeStreamCollection!.CollectionNamespace}: {ex.Message}", LogType.Error);
                throw;
            }
            finally
            {
                MigrationJobContext.AddVerboseLog($"{_syncBackPrefix}WatchCollection finally block - ShouldProcessFinalBatch: {shouldProcessFinalBatch} for {collectionKey}");
                // Only process final batch if we didn't exit early (e.g., due to timeout or cursor creation failure)
                if (shouldProcessFinalBatch)
                {
                    // Note: readStopwatch time is already accumulated in ProcessMongoDB3x/4xChangeStreamAsync
                    // No need to accumulate here to avoid double-counting
                    await ProcessWatchFinallyAsync(mu, changeStreamCollection, targetCollection, accumulatedChangesInColl, collectionKey,true);
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
            IMongoCollection<BsonDocument> changeStreamCollection,
            BsonDocument[] pipelineArray,
            ChangeStreamOptions options,
            CancellationToken cancellationToken,
            string collectionKey)
        {

            return await Task.Run(() =>
            {
                MigrationJobContext.AddVerboseLog(($"Starting Watch() for {collectionKey}..."));
                try
                {
                    var cursor = changeStreamCollection.Watch<ChangeStreamDocument<BsonDocument>>(pipelineArray, options, cancellationToken);
                    return cursor;
                }
                catch(Exception ex) when (ex is OperationCanceledException || ex is TimeoutException)
                {
                    _log.WriteLine($"Watch() cancelled for {collectionKey}.", LogType.Debug);
                    throw;
                }
                catch (Exception ex) when (ex.Message.Contains("CollectionScan died due to position in capped collection being deleted"))
                {
                    // Don't log here - let outer catch handle it to avoid double logging
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
            IMongoCollection<BsonDocument> changeStreamCollection,
            IMongoCollection<BsonDocument> targetCollection,
            AccumulatedChangesTracker accumulatedChangesInColl,
            CancellationToken cancellationToken,
            int seconds,
            BsonDocument userFilterDoc,
            System.Diagnostics.Stopwatch readStopwatch)
        {

            string collectionKey = $"{mu.DatabaseName}.{mu.CollectionName}";
            var sucess = false;
            // Note: cursor disposal is handled in ProcessMongoDB3x/4xChangeStreamAsync methods
            string lastProcessedToken = string.Empty;


            if (MigrationJobContext.CurrentlyActiveJob.SourceServerVersion.StartsWith("3"))
            {
                sucess = await ProcessMongoDB3xChangeStreamAsync(cursor, mu, changeStreamCollection, targetCollection, accumulatedChangesInColl, cancellationToken, userFilterDoc, collectionKey, readStopwatch);
            }
            else
            {
                sucess = await ProcessMongoDB4xChangeStreamAsync(cursor, mu, changeStreamCollection, targetCollection, accumulatedChangesInColl, cancellationToken, seconds, userFilterDoc, collectionKey, readStopwatch);
            }

            return sucess;
        }

        private async Task<bool> ProcessMongoDB3xChangeStreamAsync(
            IChangeStreamCursor<ChangeStreamDocument<BsonDocument>> cursor,
            MigrationUnit mu,
            IMongoCollection<BsonDocument> changeStreamCollection,
            IMongoCollection<BsonDocument> targetCollection,
            AccumulatedChangesTracker accumulatedChangesInColl,
            CancellationToken cancellationToken,
            BsonDocument userFilterDoc,
            string collectionKey,
            System.Diagnostics.Stopwatch readStopwatch)
        {
            MigrationJobContext.AddVerboseLog($"CollectionLevelChangeStreamProcessor.ProcessMongoDB3xChangeStreamAsync: collectionKey={collectionKey}");

            long flushedCount = 0;
            using (cursor)
            {
                foreach (var change in cursor.ToEnumerable(cancellationToken))
                {
                    // Stop the read stopwatch immediately after getting the change from source
                    readStopwatch.Stop();
                    accumulatedChangesInColl.CSTotalReadDurationInMS += readStopwatch.ElapsedMilliseconds;
                    
                    if (cancellationToken.IsCancellationRequested || ExecutionCancelled)
                    {
                        _log.WriteLine($"{_syncBackPrefix}Change stream processing cancelled for {changeStreamCollection!.CollectionNamespace}", LogType.Info);
                        break; // Exit loop, let finally block handle cleanup
                    }

                    string lastProcessedToken = string.Empty;
                    _resumeTokenCache.TryGetValue($"{changeStreamCollection!.CollectionNamespace}", out string? token1);
                    lastProcessedToken = token1 ?? string.Empty;

                    if (lastProcessedToken == change.ResumeToken.ToJson())
                    {
                        _log.ShowInMonitor($"{_syncBackPrefix}Skipping already processed change for {changeStreamCollection!.CollectionNamespace}");

                        return true; // Skip processing if the event has already been processed
                    }

                    try
                    {
                        bool result = ProcessCursor(change, cursor, targetCollection, collectionKey, mu, accumulatedChangesInColl, userFilterDoc);
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
                        MigrationJobContext.AddVerboseLog($"{_syncBackPrefix}Flushing accumulated changes - Count: {accumulatedChangesInColl.TotalChangesCount} exceeds max: {_config.ChangeStreamMaxDocsInBatch} for {collectionKey}");
                        
                        try
                        {
                            await FlushPendingChangesAsync(mu, targetCollection, accumulatedChangesInColl, false);
                        }
                        catch (InvalidOperationException ex) when (ex.Message.Contains("CRITICAL"))
                        {
                            _log.WriteLine($"{_syncBackPrefix}CRITICAL error during flush for {collectionKey}: {ex.Message}", LogType.Error);
                            StopJob($"CRITICAL error during flush: {ex.Message}");
                            throw; // Re-throw to stop processing
                        }

                    }

                    // Restart stopwatch for next read iteration
                    readStopwatch.Restart();
                }

                readStopwatch.Stop();
            } 

            return true;
        }       

        private async Task<bool> ProcessMongoDB4xChangeStreamAsync(IChangeStreamCursor<ChangeStreamDocument<BsonDocument>> cursor,
            MigrationUnit mu,
            IMongoCollection<BsonDocument> changeStreamCollection,
            IMongoCollection<BsonDocument> targetCollection,
            AccumulatedChangesTracker accumulatedChangesInColl,
            CancellationToken cancellationToken,
            int seconds,
            BsonDocument userFilterDoc,
            string collectionKey,
            System.Diagnostics.Stopwatch readStopwatch)
        {
            MigrationJobContext.AddVerboseLog($"CollectionLevelChangeStreamProcessor.ProcessMongoDB4xChangeStreamAsync: collectionKey={collectionKey}, seconds={seconds}");

            using (cursor)
            {
                try
                {
                    long flushedCount = 0;

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

                            if (cancellationToken.IsCancellationRequested || ExecutionCancelled)
                            {
                                MigrationJobContext.AddVerboseLog($"{_syncBackPrefix}Change stream processing cancelled for {changeStreamCollection!.CollectionNamespace}");
                                break; // Exit inner loop, outer loop will also break
                            }

                            string lastProcessedToken = string.Empty;
                            _resumeTokenCache.TryGetValue($"{changeStreamCollection!.CollectionNamespace}", out string? token2);
                            lastProcessedToken = token2 ?? string.Empty;

                            if (lastProcessedToken == change.ResumeToken.ToJson() && MigrationJobContext.CurrentlyActiveJob.JobType != JobType.RUOptimizedCopy)
                                return true; // Skip processing if the event has already been processed                            
                        

                        try
                        {
                            bool result = ProcessCursor(change, cursor, targetCollection, collectionKey, mu, accumulatedChangesInColl, userFilterDoc);
                            if (!result)
                                break; // Exit loop on error, let finally block cleanup
                        }
                        catch (Exception ex)
                        {
                            _log.WriteLine($"{_syncBackPrefix}Exception in ProcessCursor for {collectionKey}: {ex.Message}", LogType.Error);
                            break; // Exit loop on exception, let finally block handle cleanup
                        }                            // Check if we need to flush accumulated changes to prevent memory buildup
                            if ((accumulatedChangesInColl.TotalChangesCount - flushedCount) > _config.ChangeStreamMaxDocsInBatch)
                            {
                                flushedCount = flushedCount + accumulatedChangesInColl.TotalChangesCount;
                                MigrationJobContext.AddVerboseLog($"{_syncBackPrefix}Flushing accumulated changes - Count: {accumulatedChangesInColl.TotalChangesCount} exceeds max: {_config.ChangeStreamMaxDocsInBatch} for {collectionKey}");
                                
                                try
                                {
                                    await FlushPendingChangesAsync(mu, targetCollection, accumulatedChangesInColl, false);
                                }
                                catch (InvalidOperationException ex) when (ex.Message.Contains("CRITICAL"))
                                {
                                    _log.WriteLine($"{_syncBackPrefix}CRITICAL error during flush for {collectionKey}: {ex.Message}", LogType.Error);
                                    StopJob($"CRITICAL error during flush: {ex.Message}");
                                    throw; // Re-throw to stop processing
                                }
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
                    
                    try
                    {
                        await FlushPendingChangesAsync(mu, targetCollection, accumulatedChangesInColl, false);
                    }
                    catch (InvalidOperationException ex) when (ex.Message.Contains("CRITICAL"))
                    {
                        _log.WriteLine($"{_syncBackPrefix}CRITICAL error during final flush for {collectionKey}: {ex.Message}", LogType.Error);
                        StopJob($"CRITICAL error during final flush: {ex.Message}");
                        throw; // Re-throw to stop processing
                    }
                    catch (Exception ex)
                    {
                        _log.WriteLine($"{_syncBackPrefix}Error during final flush for {collectionKey}: {ex.Message}", LogType.Error);
                        // Don't throw non-critical errors from finally block
                    }
                }
            }
            return true;
        }

        private async Task ProcessWatchFinallyAsync(
            MigrationUnit mu,
            IMongoCollection<BsonDocument> changeStreamCollection,
            IMongoCollection<BsonDocument> targetCollection,
            AccumulatedChangesTracker accumulatedChangesInColl,
            string collectionKey,
            bool isFinalFlush)
        {
            MigrationJobContext.AddVerboseLog($"CollectionLevelChangeStreamProcessor.ProcessWatchFinallyAsync: collectionKey={collectionKey}, isFinalFlush={isFinalFlush}");
            try
            {

                long eventCounter = accumulatedChangesInColl.TotalEventCount;// TotalEventCount will get reset in FlushPendingChangesAsync
                if (eventCounter > 0)
                {
                    _log.ShowInMonitor($"{_syncBackPrefix}Processing batch for {changeStreamCollection.CollectionNamespace}:{eventCounter} events, {accumulatedChangesInColl.TotalChangesCount} changes (I:{accumulatedChangesInColl.DocsToBeInserted.Count}, U:{accumulatedChangesInColl.DocsToBeUpdated.Count}, D:{accumulatedChangesInColl.DocsToBeDeleted.Count})");
                    MigrationJobContext.AddVerboseLog($"{_syncBackPrefix}Final batch processing - Events: {eventCounter} Total: {accumulatedChangesInColl.TotalChangesCount}, Inserts: {accumulatedChangesInColl.DocsToBeInserted.Count}, Updates: {accumulatedChangesInColl.DocsToBeUpdated.Count}, Deletes: {accumulatedChangesInColl.DocsToBeDeleted.Count}");
                }

                try
                {
                    await FlushPendingChangesAsync(mu, targetCollection, accumulatedChangesInColl, isFinalFlush);
                }
                catch (InvalidOperationException ex) when (ex.Message.Contains("CRITICAL"))
                {
                    _log.WriteLine($"{_syncBackPrefix}CRITICAL error during flush in ProcessWatchFinallyAsync for {collectionKey}: {ex.Message}", LogType.Error);
                    StopJob($"CRITICAL error in ProcessWatchFinallyAsync: {ex.Message}");
                    throw; // Re-throw to stop processing
                }

                mu.CSUpdatesInLastBatch = eventCounter; 
                mu.CSNormalizedUpdatesInLastBatch = (long)(eventCounter / (mu.CSLastBatchDurationSeconds > 0 ? mu.CSLastBatchDurationSeconds : 1));


                // Transfer latency metrics from accumulatedChangesInColl to mu
                if (eventCounter > 0)
                {
                    mu.CSAvgReadLatencyInMS = Math.Round((double)accumulatedChangesInColl.CSTotalReadDurationInMS / eventCounter,2);
                    mu.CSAvgWriteLatencyInMS = Math.Round((double)accumulatedChangesInColl.CSTotaWriteDurationInMS / eventCounter,2);
                }

                MigrationJobContext.SaveMigrationUnit(mu,true);
                
                // Update the dictionary with the latest CSNormalizedUpdatesInLastBatch for accurate sorting
                if (_migrationUnitsToProcess.ContainsKey(mu.Id))
                {
                    _migrationUnitsToProcess[mu.Id] = mu.CSNormalizedUpdatesInLastBatch;
                }

                MigrationJobContext.AddVerboseLog($"{_syncBackPrefix}Batch counters updated - CSUpdatesInLastBatch: {eventCounter}, CSNormalizedUpdatesInLastBatch: {mu.CSNormalizedUpdatesInLastBatch} for {collectionKey}");
                

                if (eventCounter > 0)
                {
                    _log.ShowInMonitor($"{_syncBackPrefix}Watch cycle completed for {changeStreamCollection.CollectionNamespace}: {eventCounter} events processed in batch. Avg Read Latency: {mu.CSAvgReadLatencyInMS} ms | Avg Write Latency: {mu.CSAvgWriteLatencyInMS} ms");
                }
            }
            catch (Exception ex)
            {
                _log.ShowInMonitor($"{_syncBackPrefix}ERROR processing batch for {changeStreamCollection.CollectionNamespace}: {ex.Message}");
                _log.WriteLine($"{_syncBackPrefix}Error processing changes in batch for {changeStreamCollection.CollectionNamespace}. Details: {ex}", LogType.Error);
                // On failure, resume token is NOT updated - we will resume from the last successful checkpoint
            }
        }

        // This method retrieves the event associated with the ResumeToken
        private bool AutoReplayFirstChangeInResumeToken(string? documentId, ChangeStreamOperationType opType, IMongoCollection<BsonDocument> sourceCollection, IMongoCollection<BsonDocument> targetCollection, MigrationUnit mu)
        {
            MigrationJobContext.AddVerboseLog($"CollectionLevelChangeStreamProcessor.AutoReplayFirstChangeInResumeToken: documentId={documentId}, opType={opType}, collection={sourceCollection.CollectionNamespace}");
            if (documentId == null || string.IsNullOrEmpty(documentId))
            {
                _log.WriteLine($"Auto replay is empty for {sourceCollection.CollectionNamespace}.", LogType.Debug);
                return true; // Skip if no document ID is provided
            }
            else
            {
                _log.ShowInMonitor($"Auto replay for {opType} operation with _id {documentId} in {sourceCollection.CollectionNamespace}.");
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
                //check if user filter condition is met
                if (change.OperationType != ChangeStreamOperationType.Delete)
                {
                    if (userFilterDoc.Elements.Count() > 0
                        && !MongoHelper.CheckForUserFilterMatch(change.FullDocument, userFilterDoc))
                        return true;
                }


                DateTime timeStamp = DateTime.MinValue;

                if (!MigrationJobContext.CurrentlyActiveJob.SourceServerVersion.StartsWith("3") && change.ClusterTime != null)
                {
                    timeStamp = MongoHelper.BsonTimestampToUtcDateTime(change.ClusterTime); // Convert BsonTimestamp to DateTime
                }
                else if (!MigrationJobContext.CurrentlyActiveJob.SourceServerVersion.StartsWith("3") && change.WallTime != null) //for 4.0 and above
                {
                    timeStamp = change.WallTime.Value; // Use WallTime for 4.0 and above
                }


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
