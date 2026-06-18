using MongoDB.Bson;
using MongoDB.Bson.Serialization.Conventions;
using MongoDB.Driver;
using OnlineMongoMigrationProcessor.Context;
using OnlineMongoMigrationProcessor.Helpers.JobManagement;
using OnlineMongoMigrationProcessor.Helpers.Mongo;
using OnlineMongoMigrationProcessor.Models;
using OnlineMongoMigrationProcessor.Workers;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace OnlineMongoMigrationProcessor.Processors
{
    public abstract class MigrationProcessor
    {
     
        protected MongoClient? _sourceClient;
        protected MongoClient? _targetClient;
        protected IMongoCollection<BsonDocument>? _sourceCollection;
        protected IMongoCollection<BsonDocument>? _targetCollection;
        protected MigrationSettings _config;
        protected CancellationTokenSource _cts;
#if !LEGACY_MONGODB_DRIVER
        protected MongoChangeStreamProcessor? _changeStreamProcessor;
#endif
                
        protected Log _log;
        protected MigrationWorker? _migrationWorker;

        public bool ProcessRunning { get; set; }
        // Add this property to the MigrationProcessor class
        public string? MongoToolsFolder { get; set; }

        public bool IsChangeStreamRunning = false;

#if !LEGACY_MONGODB_DRIVER
        // Expose WaitForResumeTokenTaskDelegate from the change stream processor
        public Func<string, Task>? WaitForResumeTokenTaskDelegate
        {
            get => _changeStreamProcessor?.WaitForResumeTokenTaskDelegate;
            set
            {
                if (_changeStreamProcessor != null)
                    _changeStreamProcessor.WaitForResumeTokenTaskDelegate = value;
            }
        }
#endif

        protected MigrationProcessor(Log log, MongoClient sourceClient, MigrationSettings config, MigrationWorker? migrationWorker = null)
        {
            _log = log;
            _sourceClient = sourceClient;
            _targetClient = null;
            _config = config;
            _cts = new CancellationTokenSource();
            _migrationWorker = migrationWorker;
        }

        public virtual void StopProcessing(bool updateStatus = true)
        {
            MigrationJobContext.AddVerboseLog($"MigrationProcessor.StopProcessing: updateStatus={updateStatus}");

            if (MigrationJobContext.CurrentlyActiveJob != null)
            {
                MigrationJobContext.CurrentlyActiveJob.IsStarted = false;
            }

            MigrationJobContext.SaveMigrationJob(MigrationJobContext.CurrentlyActiveJob);

            if (updateStatus)
                ProcessRunning = false;


            _cts?.Cancel();

#if !LEGACY_MONGODB_DRIVER
            if (_changeStreamProcessor != null)
                _changeStreamProcessor.ExecutionCancelled = true;
#endif
        }

        /// <summary>
        /// Signals processor to stop accepting new work but complete current tasks
        /// </summary>

        /// <summary>
        /// Stops only the change stream processor, leaving offline workers running.
        /// </summary>
        public void StopChangeStreamProcessor()
        {
#if !LEGACY_MONGODB_DRIVER
            if (_changeStreamProcessor != null)
            {
                _changeStreamProcessor.ExecutionCancelled = true;
                MigrationJobContext.AddVerboseLog("MigrationProcessor.StopChangeStreamProcessor: ExecutionCancelled set to true");
            }
#endif
        }

        /// <summary>
        /// Removes in-memory state for the given migration unit id from any active change-stream
        /// processor. Called by <see cref="MigrationJobContext.PurgeMigrationUnit"/> when a
        /// collection is removed from a running job so a later re-add with the same id starts
        /// from a clean slate.
        /// </summary>
        public virtual void RemoveMigrationUnit(string migrationUnitId)
        {
            if (string.IsNullOrEmpty(migrationUnitId))
                return;

#if !LEGACY_MONGODB_DRIVER
            _changeStreamProcessor?.RemoveMigrationUnit(migrationUnitId);
#endif
        }


        protected ProcessorContext SetProcessorContext(MigrationUnit mu, string sourceConnectionString, string targetConnectionString)
        {
            var databaseName = mu.DatabaseName;
            var collectionName = mu.CollectionName;
            var targetDatabaseName = mu.GetEffectiveTargetDatabaseName();
            var targetCollectionName = mu.GetEffectiveTargetCollectionName();
            var database = _sourceClient?.GetDatabase(databaseName);
            var collection = database?.GetCollection<BsonDocument>(collectionName);

            var context = new ProcessorContext
            {
                MigrationUnitId = mu.Id,
                SourceConnectionString = sourceConnectionString,
                TargetConnectionString = targetConnectionString,
                JobId = MigrationJobContext.CurrentlyActiveJob?.Id ?? string.Empty,
                DatabaseName = databaseName,
                CollectionName = collectionName,
                TargetDatabaseName = targetDatabaseName,
                TargetCollectionName = targetCollectionName,
                Database = database!,
                Collection = collection!,
            };

            return context;
        }

        public bool AddCollectionToChangeStreamQueue(MigrationUnit mu)
        {
            MigrationJobContext.AddVerboseLog($"MigrationProcessor.AddCollectionToChangeStreamQueue: migrationUnitId={mu.Id}");

#if LEGACY_MONGODB_DRIVER
            _log.WriteLine($"Online migration (change streams) is not supported with the legacy MongoDB driver. Skipping {mu.DatabaseName}.{mu.CollectionName}", LogType.Warning);
            return false;
#else
            if (!Helper.IsOnline(MigrationJobContext.CurrentlyActiveJob))
                return false;
            
            if (_targetClient == null)
                _targetClient = MongoClientFactory.Create(_log, MigrationJobContext.TargetConnectionString[MigrationJobContext.CurrentlyActiveJob.Id]);

            // Ensure _sourceClient is not null before using it
            if (_changeStreamProcessor == null && _sourceClient != null)
                _changeStreamProcessor = new MongoChangeStreamProcessor(_log, _sourceClient, _targetClient!, MigrationJobContext.MigrationUnitsCache, _config);


            _log.WriteLine($"Adding {mu.DatabaseName}.{mu.CollectionName} to Change Stream processing queue", LogType.Debug);
            _changeStreamProcessor?.AddCollectionsToProcess(mu.Id, _cts);

            return true;
#endif
        }


        public bool RunChangeStreamProcessorForAllCollections()
        {
            MigrationJobContext.AddVerboseLog("MigrationProcessor.RunChangeStreamProcessorForAllCollections: called");

#if LEGACY_MONGODB_DRIVER
            _log.WriteLine("Online migration (change streams) is not supported with the legacy MongoDB driver.", LogType.Warning);
            return false;
        }
#else
            //only once allowed per job
            if(IsChangeStreamRunning)
                return false;

            if (!Helper.IsOnline(MigrationJobContext.CurrentlyActiveJob))
                return false;

            //for delayed mode only
            if (!Helper.IsOfflineJobCompleted(MigrationJobContext.CurrentlyActiveJob) && MigrationJobContext.CurrentlyActiveJob.ChangeStreamMode == ChangeStreamMode.Delayed)
                return false;

            //for delayed mode only, at the start no collections are valid, hence IsOfflineJobCompleted gives false positive
            if (!Helper.AnyValidCollection(MigrationJobContext.CurrentlyActiveJob) && MigrationJobContext.CurrentlyActiveJob.ChangeStreamMode == ChangeStreamMode.Delayed)
                return false;

            //for server-level change streams, wait for all collections to complete offline migration before starting CS
            if (!Helper.IsOfflineJobCompleted(MigrationJobContext.CurrentlyActiveJob) && MigrationJobContext.CurrentlyActiveJob.ChangeStreamLevel == ChangeStreamLevel.Server)
                return false;            


            //only once allowed per job, checking again
            if (IsChangeStreamRunning)
                return false;

            IsChangeStreamRunning = true; // Set flag to indicate post-upload CS processing is in progress

            string targetConnStr = MigrationJobContext.TargetConnectionString[MigrationJobContext.CurrentlyActiveJob.Id];
            if (_targetClient == null && !MigrationJobContext.CurrentlyActiveJob.IsSimulatedRun)
                _targetClient = MongoClientFactory.Create(_log, targetConnStr);

            // Ensure _sourceClient is not null before using it
            if (_changeStreamProcessor == null && _sourceClient != null)
#pragma warning disable CS8604 // Possible null reference argument.
                _changeStreamProcessor = new MongoChangeStreamProcessor(_log, _sourceClient, _targetClient, MigrationJobContext.MigrationUnitsCache, _config, false, _migrationWorker);
#pragma warning restore CS8604 // Possible null reference argument.

            if (_changeStreamProcessor != null)
            {
                var result = _changeStreamProcessor.RunChangeStreamProcessorForAllCollections(_cts);
            }
            return true;            
        }
#endif

        
        /// <summary>
        /// Builds non-unique indexes on the target after offline data copy completes.
        /// For blocking mode: waits for index builds to complete, monitoring via currentOp.
        /// For non-blocking mode: starts index builds and monitors progress asynchronously.
        /// Updates mu.IndexPercent and mu.IndexBuildComplete accordingly.
        /// Returns true if change stream can proceed immediately (non-blocking or no indexes needed).
        /// </summary>
        public async Task<bool> BuildNonUniqueIndexesAfterCopyAsync(MigrationUnit mu)
        {
            if (_cts.Token.IsCancellationRequested)
                return false;

            var activeJob = MigrationJobContext.CurrentlyActiveJob;
            if (activeJob == null)
            {
                _log.WriteLine($"Skipping non-unique index build for {mu.DatabaseName}.{mu.CollectionName}: active job context unavailable", LogType.Debug);
                return false;
            }

            // Skip if indexes are not being migrated
            if (!mu.IndexingStrategy.HasValue || mu.IndexingStrategy.Value == IndexingStrategy.DontIndex)
            {
                mu.IndexPercent = 100;
                mu.IndexBuildComplete = true;
                MigrationJobContext.SaveMigrationUnit(mu, true);
                return true;
            }

            // Skip if already complete
            if (mu.IndexBuildComplete)
                return true;

            if (!MigrationJobContext.TargetConnectionString.TryGetValue(activeJob.Id, out var targetConnStr) || string.IsNullOrWhiteSpace(targetConnStr))
            {
                _log.WriteLine($"Skipping non-unique index build for {mu.DatabaseName}.{mu.CollectionName}: target connection string unavailable", LogType.Warning);
                return false;
            }
            var sourceDatabase = _sourceClient!.GetDatabase(mu.DatabaseName);
            var sourceCollection = sourceDatabase.GetCollection<BsonDocument>(mu.CollectionName);
            var targetDatabaseName = mu.GetEffectiveTargetDatabaseName();
            var targetCollectionName = mu.GetEffectiveTargetCollectionName();
            var namespaceForLog = Log.FormatNamespaceForLog(mu.DatabaseName, mu.CollectionName, targetDatabaseName, targetCollectionName);

            bool isBlocking = mu.IndexingStrategy.Value == IndexingStrategy.SameAsSourceBlocking;

            // Authoritative resume check: if the target already has all expected non-unique
            // indexes built, the previous run finished — mark complete and skip re-issuing.
            // This avoids both (a) the previous bug where currentOp-zero was treated as done
            // before builds actually started, and (b) double-counting from re-issuing createIndexes.
            if (mu.IndexesExpected > 0)
            {
                try
                {
                    var verifyClient = MongoClientFactory.Create(_log, targetConnStr);
                    int builtOnTarget = await Helpers.Mongo.IndexCopier.CountNonUniqueIndexesOnTargetAsync(verifyClient, targetDatabaseName, targetCollectionName, _log);
                    _log.WriteLine($"[temp] BuildNonUniqueIndexesAfterCopy verify {namespaceForLog}: builtOnTarget={builtOnTarget}, expected={mu.IndexesExpected}, migrated={mu.IndexesMigrated}, blocking={isBlocking}", LogType.Info);
                    if (builtOnTarget >= mu.IndexesExpected)
                    {
                        mu.IndexesMigrated = mu.IndexesExpected;
                        mu.IndexPercent = 100;
                        mu.IndexBuildComplete = true;
                        MigrationJobContext.SaveMigrationUnit(mu, true);
                        _log.WriteLine($"Non-unique index builds already complete on target for {namespaceForLog} ({builtOnTarget}/{mu.IndexesExpected})");
                        return true;
                    }
                }
                catch (Exception ex)
                {
                    _log.WriteLine($"Target index verification failed for {namespaceForLog}: {ex.Message}", LogType.Warning);
                }
            }

            // Resume path: a previous run already submitted createIndexes for all expected indexes
            // (IndexesMigrated >= IndexesExpected) but server-side builds didn't finish before the
            // pause. Skip re-issuing createIndexes and go straight to monitoring/waiting so we
            // don't double-count IndexesMigrated or hammer the target unnecessarily.
            if (mu.IndexesExpected > 0 && mu.IndexesMigrated >= mu.IndexesExpected)
            {
                _log.WriteLine($"Resuming index-build monitoring for {namespaceForLog} ({(isBlocking ? "blocking" : "non-blocking")}, IndexesMigrated={mu.IndexesMigrated}/{mu.IndexesExpected})");
                if (isBlocking)
                {
                    if (_cts.Token.IsCancellationRequested)
                        return false;
                    return await WaitForIndexBuildsAsync(mu, targetConnStr, targetDatabaseName, targetCollectionName);
                }
                else
                {
                    _ = Task.Run(() => MonitorIndexBuildsAsync(mu, targetConnStr, targetDatabaseName, targetCollectionName));
                    return true;
                }
            }

            // Reset transient progress at the start of a new index-build cycle.
            // This avoids showing stale values (for example 99%) from a previous run.
            mu.IndexPercent = 0;
            mu.IndexBuildComplete = false;
            MigrationJobContext.SaveMigrationUnit(mu, true);

            _log.WriteLine($"Starting non-unique index build ({(isBlocking ? "blocking" : "non-blocking")}) for {namespaceForLog}");

            // Pre-count source non-unique indexes so the UI immediately shows build progress
            // instead of staying on "Pending" while a long synchronous build runs.
            int expectedNonUniqueCount = 0;
            int originalIndexesMigrated = mu.IndexesMigrated;
            CancellationTokenSource? preBuildMonitorCts = null;
            Task? preBuildMonitorTask = null;
            try
            {
                var preCountCopier = new Helpers.Mongo.IndexCopier();
                expectedNonUniqueCount = await preCountCopier.CountNonUniqueIndexesAsync(sourceCollection, _log);
                if (expectedNonUniqueCount > 0)
                {
                    mu.IndexesExpected = expectedNonUniqueCount;
                    mu.IndexPercent = 0;
                    MigrationJobContext.SaveMigrationUnit(mu, true);
                    _log.WriteLine($"Expecting {expectedNonUniqueCount} non-unique indexes for {namespaceForLog}");

                    // Some targets keep createIndexes call blocked until server-side build finishes.
                    // Start a side monitor so progress can advance while the call is in-flight.
                    preBuildMonitorCts = new CancellationTokenSource();
                    preBuildMonitorTask = MonitorIndexBuildsDuringCreateIndexesAsync(
                        mu,
                        targetConnStr,
                        targetDatabaseName,
                        targetCollectionName,
                        expectedNonUniqueCount,
                        preBuildMonitorCts.Token);
                }
            }
            catch { /* non-fatal; UI falls back to "Pending" if this fails */ }

            // Build non-unique indexes
            int count;
            try
            {
                if (_cts.Token.IsCancellationRequested)
                    return false;

                count = await MongoHelper.BuildNonUniqueIndexesAsync(_log, mu, targetConnStr, sourceCollection, isBlocking);
            }
            finally
            {
                if (preBuildMonitorCts != null)
                {
                    preBuildMonitorCts.Cancel();
                    if (preBuildMonitorTask != null)
                    {
                        try { await preBuildMonitorTask; } catch { }
                    }
                    preBuildMonitorCts.Dispose();
                }
            }

            // BuildNonUniqueIndexesAsync sets IndexesMigrated += actualCount.
            // If the build failed (count < 0), restore the original count.
            if (count < 0)
            {
                mu.IndexesMigrated = originalIndexesMigrated;
                mu.IndexesExpected = 0;
            }

            if (count < 0)
            {
                _log.WriteLine($"Failed to build non-unique indexes for {namespaceForLog}", LogType.Error);
                return !isBlocking; // In non-blocking mode, don't block change stream on failure
            }

            if (count == 0)
            {
                // No non-unique indexes to build
                mu.IndexesExpected = 0;
                mu.IndexPercent = 100;
                mu.IndexBuildComplete = true;
                MigrationJobContext.SaveMigrationUnit(mu, true);
                _log.WriteLine($"No non-unique indexes to build for {namespaceForLog}");
                return true;
            }

            if (isBlocking)
            {
                if (_cts.Token.IsCancellationRequested)
                    return false;

                // Wait for all index builds to complete by polling currentOp
                _log.WriteLine($"Waiting for blocking index builds to complete on {namespaceForLog}");
                return await WaitForIndexBuildsAsync(mu, targetConnStr, targetDatabaseName, targetCollectionName);
            }
            else
            {
                // Non-blocking: start monitoring in background, don't block change stream
                _ = Task.Run(() => MonitorIndexBuildsAsync(mu, targetConnStr, targetDatabaseName, targetCollectionName));
                return true;
            }
        }

        /// <summary>
        /// Monitors index progress while createIndexes call is executing synchronously.
        /// Uses the same throttling policy as other index polling paths.
        /// </summary>
        private async Task MonitorIndexBuildsDuringCreateIndexesAsync(
            MigrationUnit mu,
            string targetConnStr,
            string databaseName,
            string collectionName,
            int expectedTotalBuilds,
            CancellationToken token)
        {
            const int pollIntervalMs = 60000;

            try
            {
                bool seenAnyBuild = false;
                while (!token.IsCancellationRequested)
                {
                    var (activeBuilds, progress) = await MongoHelper.CheckIndexBuildProgressAsync(
                        _log,
                        targetConnStr,
                        databaseName,
                        collectionName,
                        mu.IndexesExpected > 0 ? mu.IndexesExpected : expectedTotalBuilds);

                    if (activeBuilds > 0)
                    {
                        seenAnyBuild = true;
                    }

                    // currentOp can briefly report zero in-flight ops before queued/building entries appear.
                    // During that warm-up window, keep the floor and avoid jumping to 99/100.
                    if (activeBuilds > 0 || seenAnyBuild)
                    {
                        mu.IndexPercent = Math.Max(1, Math.Min(99, Math.Max(0, progress)));
                        MigrationJobContext.SaveMigrationUnit(mu, true);
                        _log.WriteLine($"[temp] PreBuildMonitor {databaseName}.{collectionName}: activeBuilds={activeBuilds}, rawProgress={progress:F1}, seenAnyBuild={seenAnyBuild}, appliedIndexPercent={mu.IndexPercent:F1}", LogType.Info);
                    }
                    else
                    {
                        _log.WriteLine($"[temp] PreBuildMonitor {databaseName}.{collectionName}: activeBuilds=0, rawProgress={progress:F1}, seenAnyBuild=false, indexPercent unchanged ({mu.IndexPercent:F1}) (warm-up)", LogType.Info);
                    }

                    await Task.Delay(pollIntervalMs, token);
                }
            }
            catch (OperationCanceledException)
            {
                // Expected when createIndexes finishes.
            }
            catch
            {
                // Non-fatal: main build flow will still complete and set final status.
            }
        }

        /// <summary>
        /// Polls currentOp until all index builds on the collection complete. (Blocking mode)
        /// </summary>
        private async Task<bool> WaitForIndexBuildsAsync(MigrationUnit mu, string targetConnStr, string databaseName, string collectionName)
        {
            var namespaceForLog = Log.FormatNamespaceForLog(mu.DatabaseName, mu.CollectionName, databaseName, collectionName);
            const int pollIntervalMs = 60000;
            const int maxAttempts = 8640; // ~12 hours at 5s intervals
            int consecutiveZeroPolls = 0;

            try
            {
                for (int attempt = 0; attempt < maxAttempts; attempt++)
                {
                    if (_cts.Token.IsCancellationRequested)
                        return false;

                    var (activeBuilds, progress) = await MongoHelper.CheckIndexBuildProgressAsync(_log, targetConnStr, databaseName, collectionName, mu.IndexesMigrated);

                    mu.IndexPercent = Math.Min(100, Math.Max(0, progress));
                    MigrationJobContext.SaveMigrationUnit(mu, true);
                    _log.WriteLine($"[temp] BlockingWait {namespaceForLog} attempt={attempt}: activeBuilds={activeBuilds}, rawProgress={progress:F1}, appliedIndexPercent={mu.IndexPercent:F1}, expected={mu.IndexesExpected}, migrated={mu.IndexesMigrated}, consecutiveZeroPolls={consecutiveZeroPolls}", LogType.Info);

                    if (activeBuilds == 0)
                    {
                        consecutiveZeroPolls++;
                        if (consecutiveZeroPolls >= 2)
                        {
                            // Verify with target listIndexes before declaring done. currentOp can
                            // report zero in-flight ops while builds are still queued / not yet scheduled
                            // (especially right after resume), which previously caused premature completion.
                            int builtOnTarget = -1;
                            try
                            {
                                var verifyClient = MongoClientFactory.Create(_log, targetConnStr);
                                builtOnTarget = await Helpers.Mongo.IndexCopier.CountNonUniqueIndexesOnTargetAsync(verifyClient, databaseName, collectionName, _log);
                            }
                            catch { }
                            _log.WriteLine($"[temp] BlockingWait verify {namespaceForLog}: builtOnTarget={builtOnTarget}, expected={mu.IndexesExpected}", LogType.Info);
                            if (mu.IndexesExpected <= 0 || (builtOnTarget >= 0 && builtOnTarget >= mu.IndexesExpected))
                            {
                                mu.IndexPercent = 100;
                                mu.IndexBuildComplete = true;
                                MigrationJobContext.SaveMigrationUnit(mu, true);
                                _log.WriteLine($"Blocking index builds completed for {namespaceForLog}");
                                return true;
                            }
                            // Target does not yet have all indexes — reset the zero-poll counter and
                            // keep waiting. Reflect partial progress from target count.
                            consecutiveZeroPolls = 0;
                            if (builtOnTarget >= 0 && mu.IndexesExpected > 0)
                            {
                                mu.IndexPercent = Math.Max(mu.IndexPercent, Math.Min(99, builtOnTarget * 100.0 / mu.IndexesExpected));
                                MigrationJobContext.SaveMigrationUnit(mu, true);
                            }
                        }
                    }
                    else
                    {
                        consecutiveZeroPolls = 0;
                    }

                    if (attempt % 12 == 0) // Log every ~60 seconds
                        _log.WriteLine($"Index build in progress for {namespaceForLog}: {activeBuilds} active, {progress:F1}% complete", LogType.Debug);

                    await Task.Delay(pollIntervalMs, _cts.Token);
                }

                _log.WriteLine($"Index build monitoring timed out for {namespaceForLog}", LogType.Warning);
                return false;
            }
            catch (OperationCanceledException)
            {
                // Expected when pause/stop cancels processor token.
                return false;
            }
        }

        /// <summary>
        /// Monitors index builds asynchronously and updates IndexPercent. (Non-blocking mode)
        /// </summary>
        private async Task MonitorIndexBuildsAsync(MigrationUnit mu, string targetConnStr, string databaseName, string collectionName)
        {
            var namespaceForLog = Log.FormatNamespaceForLog(mu.DatabaseName, mu.CollectionName, databaseName, collectionName);
            const int pollIntervalMs = 60000;
            const int maxAttempts = 4320; // ~12 hours at 10s intervals

            try
            {
                for (int attempt = 0; attempt < maxAttempts; attempt++)
                {
                    if (_cts.Token.IsCancellationRequested)
                        return;

                    var (activeBuilds, progress) = await MongoHelper.CheckIndexBuildProgressAsync(_log, targetConnStr, databaseName, collectionName, mu.IndexesMigrated);

                    mu.IndexPercent = Math.Min(100, Math.Max(0, progress));
                    MigrationJobContext.SaveMigrationUnit(mu, true);
                    _log.WriteLine($"[temp] NonBlockingMonitor {namespaceForLog} attempt={attempt}: activeBuilds={activeBuilds}, rawProgress={progress:F1}, appliedIndexPercent={mu.IndexPercent:F1}, expected={mu.IndexesExpected}, migrated={mu.IndexesMigrated}", LogType.Info);

                    if (activeBuilds == 0)
                    {
                        // Verify with target listIndexes before flipping complete. Without this,
                        // a brief warm-up window where currentOp shows no ops would incorrectly
                        // declare the build done.
                        int builtOnTarget = -1;
                        try
                        {
                            var verifyClient = MongoClientFactory.Create(_log, targetConnStr);
                            builtOnTarget = await Helpers.Mongo.IndexCopier.CountNonUniqueIndexesOnTargetAsync(verifyClient, databaseName, collectionName, _log);
                        }
                        catch { }
                        _log.WriteLine($"[temp] NonBlockingMonitor verify {namespaceForLog}: builtOnTarget={builtOnTarget}, expected={mu.IndexesExpected}", LogType.Info);
                        if (mu.IndexesExpected <= 0 || (builtOnTarget >= 0 && builtOnTarget >= mu.IndexesExpected))
                        {
                            mu.IndexPercent = 100;
                            mu.IndexBuildComplete = true;
                            MigrationJobContext.SaveMigrationUnit(mu, true);
                            _log.WriteLine($"Non-blocking index builds completed for {namespaceForLog}");
                            return;
                        }
                        if (builtOnTarget >= 0 && mu.IndexesExpected > 0)
                        {
                            mu.IndexPercent = Math.Max(mu.IndexPercent, Math.Min(99, builtOnTarget * 100.0 / mu.IndexesExpected));
                            MigrationJobContext.SaveMigrationUnit(mu, true);
                        }
                    }

                    if (attempt % 6 == 0) // Log every ~60 seconds
                        _log.WriteLine($"Index build in progress (non-blocking) for {namespaceForLog}: {activeBuilds} active, {progress:F1}% complete", LogType.Debug);

                    await Task.Delay(pollIntervalMs, _cts.Token);
                }

                _log.WriteLine($"Non-blocking index build monitoring timed out for {namespaceForLog}", LogType.Warning);
            }
            catch (OperationCanceledException)
            {
                // Expected on shutdown
            }
            catch (Exception ex)
            {
                _log.WriteLine($"Error monitoring index builds for {namespaceForLog}: {ex.Message}", LogType.Warning);
            }
        }

        public void StopOfflineOrInvokeChangeStreams()
        {
            // Handle offline completion and post-upload CS logic

            if (!Helper.IsOnline(MigrationJobContext.CurrentlyActiveJob) && Helper.IsOfflineJobCompleted(MigrationJobContext.CurrentlyActiveJob))
            {
                // Don't mark as completed if this is a controlled pause
                if (!MigrationJobContext.ControlledPauseRequested)
                {
                    _log.WriteLine($"Job {MigrationJobContext.CurrentlyActiveJob.Id} Completed from StopOfflineOrInvokeChangeStreams");
                    MigrationJobContext.CurrentlyActiveJob.IsCompleted = true;
                    MigrationJobContext.SaveMigrationJob(MigrationJobContext.CurrentlyActiveJob);
                }
                StopProcessing();
            }
            else
            {
                if (!MigrationJobContext.ControlledPauseRequested)
                {
                    _log.WriteLine($"Invoke RunChangeStreamProcessorForAllCollections.", LogType.Debug);

                    RunChangeStreamProcessorForAllCollections();
                }
            }

        }
        public virtual Task<TaskResult> StartProcessAsync(string migrationUnitId, string sourceConnectionString, string targetConnectionString)
        { return Task.FromResult(TaskResult.Success); }
    }
}
