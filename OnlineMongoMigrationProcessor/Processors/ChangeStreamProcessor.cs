using MongoDB.Bson;
using MongoDB.Driver;
using OnlineMongoMigrationProcessor.Helpers;
using OnlineMongoMigrationProcessor.Models;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using static OnlineMongoMigrationProcessor.MongoHelper;

#pragma warning disable CS8602 // Dereference of a possibly null reference.

namespace OnlineMongoMigrationProcessor
{
    public abstract class ChangeStreamProcessor : IDisposable
    {
        protected int _concurrentProcessors;
        protected int _processorRunMaxDurationInSec;
        protected int _processorRunMinDurationInSec;

        protected MongoClient _sourceClient;
        protected MongoClient _targetClient;
        protected JobList? _jobList;
        protected MigrationJob? _job;
        protected MigrationSettings? _config;
        protected bool _syncBack = false;
        protected string _syncBackPrefix = string.Empty;
        protected bool _isCSProcessing = false;
        protected Log _log;

        // Resume token cache - used by collection-level processors to track individual collection resume tokens
        // Server-level processors don't need this as they use MigrationJob properties directly for global tokens
        protected virtual bool UseResumeTokenCache => true; // Override in server-level processor to return false
        protected ConcurrentDictionary<string, string> _resumeTokenCache = new ConcurrentDictionary<string, string>();
        protected ConcurrentDictionary<string, MigrationUnit> _migrationUnitsToProcess = new ConcurrentDictionary<string, MigrationUnit>();

        // Tracking for aggressive cleanup to prevent duplicate executions
        protected ConcurrentDictionary<string, bool> _aggressiveCleanupProcessed = new ConcurrentDictionary<string, bool>();
        protected bool _finalCleanupExecuted = false;

        // Critical failure tracking - shared across server-level and collection-level processors
        protected readonly ConcurrentBag<Task> _backgroundProcessingTasks = new();
        protected volatile bool _criticalFailureDetected = false;
        protected Exception? _criticalFailureException = null;

        protected static readonly object _processingLock = new object();
        protected static readonly object _cleanupLock = new object();

        // Global backpressure tracking across ALL collections/processors to prevent OOM
        protected static readonly object _pendingWritesLock = new object();


        // UI update throttling to prevent Blazor rendering OOM (max 1 update per second per collection)
        const int GLOBAL_UI_UPDATE_INTERVAL_MS = 500; // 500ms global throttling across all collections
        protected readonly ConcurrentDictionary<string, DateTime> _lastUIUpdateTime = new ConcurrentDictionary<string, DateTime>();

        protected bool StopProcessing = false;

        private bool _disposed = false;
        protected DateTime _lastGlobalUIUpdate = DateTime.MinValue; // Track last global UI update time for 500ms throttling

        public bool ExecutionCancelled { 
            get => _executionCancelled; 
            set 
            {
                if (_executionCancelled != value)
                {
                    _log.WriteLine($"{_syncBackPrefix}ExecutionCancelled state change - From: {_executionCancelled}, To: {value}", LogType.Debug);
                    _executionCancelled = value;
                }
            }
        }
        private bool _executionCancelled = false;

        public ChangeStreamProcessor(Log log, MongoClient sourceClient, MongoClient targetClient, JobList jobList, MigrationJob job, MigrationSettings config, bool syncBack = false)
        {
            _log = log;
            _sourceClient = sourceClient;
            _targetClient = targetClient;
            _jobList = jobList;
            _job = job;
            _config = config;
            _syncBack = syncBack;
            if (_syncBack)
                _syncBackPrefix = "SyncBack: ";

            _concurrentProcessors = _config?.ChangeStreamMaxCollsInBatch ?? 5;
            _processorRunMaxDurationInSec = _config?.ChangeStreamBatchDuration ?? 120;
            _processorRunMinDurationInSec = _config?.ChangeStreamBatchDurationMin ?? 30;
            StopProcessing = false;

        }

        public bool AddCollectionsToProcess(MigrationUnit mu, CancellationTokenSource cts)
        {
            string key = $"{mu.DatabaseName}.{mu.CollectionName}";
            if (!Helper.IsMigrationUnitValid(mu)|| ((mu.DumpComplete != true || mu.RestoreComplete != true) && !_job.AggresiveChangeStream))
            {
                //_log.WriteLine($"{_syncBackPrefix}Cannot add {key} to change streams for processing.", LogType.Error);
                return false;
            }
            if (!_migrationUnitsToProcess.ContainsKey(key))
            {
                _migrationUnitsToProcess.TryAdd(key, mu);
                _log.WriteLine($"{_syncBackPrefix}Collection added to change stream queue - Key: {key}, DumpComplete: {mu.DumpComplete}, RestoreComplete: {mu.RestoreComplete}", LogType.Verbose);

                _ = RunCSPostProcessingAsync(cts); // fire-and-forget by design
                return true;
            }
            else
            {
                return false;
            }
        }

        public async Task RunCSPostProcessingAsync(CancellationTokenSource cts)
        {
            lock (_processingLock)
            {
                if (_isCSProcessing)
                {
                    return; //already processing    
                }
                _isCSProcessing = true;
            }

            try
            {
                cts = new CancellationTokenSource();
                var token = cts.Token;

                _migrationUnitsToProcess.Clear();
                foreach (var migrationUnit in _job.MigrationUnits)
                {
                    if (Helper.IsMigrationUnitValid(migrationUnit) && ((migrationUnit.DumpComplete == true && migrationUnit.RestoreComplete == true) || _job.AggresiveChangeStream))
                    {
                        _migrationUnitsToProcess[$"{migrationUnit.DatabaseName}.{migrationUnit.CollectionName}"] = migrationUnit;

                        // For aggressive change stream, trigger cleanup for completed collections (only if not already processed)
                        if (_job.AggresiveChangeStream && migrationUnit.RestoreComplete && !_syncBack && !migrationUnit.AggressiveCacheDeleted)
                        {
                            string collKey = $"{migrationUnit.DatabaseName}.{migrationUnit.CollectionName}";
                            _log.WriteLine($"{_syncBackPrefix}Aggressive cleanup queued for {collKey}", LogType.Verbose);
                            
                            try
                            {
                                _ = Task.Run(async () => 
                                {
                                    try
                                    {
                                        await CleanupAggressiveCSAsync(migrationUnit);
                                    }
                                    catch (Exception ex)
                                    {
                                        _log.WriteLine($"{_syncBackPrefix}Exception in aggressive cleanup Task.Run for {migrationUnit.DatabaseName}.{migrationUnit.CollectionName}: {ex}", LogType.Error);
                                        // Don't re-throw for fire-and-forget tasks
                                    }
                                });
                            }
                            catch
                            {
                            }
                        }
                    }
                }

                _job.CSPostProcessingStarted = true;
                _jobList?.Save(); // persist state

                if (_migrationUnitsToProcess.Count == 0)
                {
                    _log.WriteLine($"{_syncBackPrefix}No change streams to process.");

                    _isCSProcessing = false;
                    return;
                }

                await ProcessChangeStreamsAsync(token);

                _log.WriteLine($"{_syncBackPrefix}Change stream processing completed or paused.");
                _jobList?.Save();

            }
            catch (OperationCanceledException)
            {
                _log.WriteLine($"{_syncBackPrefix}Change stream processing was paused.");
            }
            catch (Exception ex)
            {
                _log.WriteLine($"{_syncBackPrefix}Error during change stream processing: {ex}", LogType.Error);
            }
            finally
            {
                lock (_processingLock)
                {
                    _isCSProcessing = false;
                }
            }
        }

        protected abstract Task ProcessChangeStreamsAsync(CancellationToken token);

        #region Global Backpressure Methods - Shared by All Processors

        //protected int GetGlobalPendingWriteCount()
        //{
        //    lock (_pendingWritesLock)
        //    {
        //        return _globalPendingWrites;
        //    }
        //}

        //protected void IncrementGlobalPendingWrites()
        //{
        //    lock (_pendingWritesLock)
        //    {
        //        _globalPendingWrites++;
        //        _log.WriteLine($"{_syncBackPrefix}Global pending writes incremented to {_globalPendingWrites}", LogType.Verbose);
        //    }
        //}

        //protected void DecrementGlobalPendingWrites()
        //{
        //    lock (_pendingWritesLock)
        //    {
        //        _globalPendingWrites = Math.Max(0, _globalPendingWrites - 1);
        //        _log.WriteLine($"{_syncBackPrefix}Global pending writes decremented to {_globalPendingWrites}", LogType.Verbose);
        //    }
        //}

        protected bool IsReadyForFlush(AccumulatedChangesTracker accumulatedChangesInColl, out int totalAccumulated)
        {
            totalAccumulated = accumulatedChangesInColl.DocsToBeInserted.Count +
                                  accumulatedChangesInColl.DocsToBeUpdated.Count +
                                  accumulatedChangesInColl.DocsToBeDeleted.Count;
            if (totalAccumulated >= _config?.ChangeStreamMaxCollsInBatch)
                return true;
            else
                return false;
        }
        protected bool ShowInMonitor(ChangeStreamDocument<BsonDocument> change, string collNameSpace, DateTime timeStamp, long counter)
        {
            DateTime now = DateTime.UtcNow;
            bool shouldUpdateUI = false;            

            // Check if enough time has passed since last global UI update (500ms throttling across all collections)
            if (_lastGlobalUIUpdate == DateTime.MinValue ||
                (now - _lastGlobalUIUpdate).TotalMilliseconds >= GLOBAL_UI_UPDATE_INTERVAL_MS)
            {
                // Update the global last UI update time and allow UI update
                _lastGlobalUIUpdate = now;
                shouldUpdateUI = true;
            }

            // Show on monitor only if global UI update is allowed (once per 500ms), but always log to file
            if (shouldUpdateUI)
            {
                if (timeStamp == DateTime.MinValue)
                {
                    _log.ShowInMonitor($"{_syncBackPrefix}{change.OperationType} operation detected in {collNameSpace} for _id: {change.DocumentKey["_id"]}. Sequence in batch #{counter}");
                }
                else
                {
                    _log.ShowInMonitor($"{_syncBackPrefix}{change.OperationType} operation detected in {collNameSpace} for _id: {change.DocumentKey["_id"]} with TS (UTC): {timeStamp}. Sequence in batch #{counter}");
                }
            }

            return shouldUpdateUI;
        }
        protected async Task WaitForPendingChnagesAsync(Dictionary<string, AccumulatedChangesTracker> accumulatedChangesPerCollection)
        {
            //count pending changes in accumulated changes tracker across keys in batch
            long pendingChanges = 0;
            bool loop = true;
            int counter = 1;
            while (loop)
            {
                foreach (var c in accumulatedChangesPerCollection.Values)
                {
                    var count = c.DocsToBeDeleted.Count + c.DocsToBeInserted.Count + c.DocsToBeUpdated.Count;
                    pendingChanges += count;
                }
                if (pendingChanges > _config.ChangeStreamMaxDocsInBatch * 5)
                {
                    _log.WriteLine($"{_syncBackPrefix}Pending changes exceeded limit -  Round {counter} pausing for {counter}seconds,  PendingChanges: {pendingChanges}, Limit: {_config.ChangeStreamMaxDocsInBatch * 5}", LogType.Warning);
                    Thread.Sleep(1000 * counter);//sleep for some time
                    loop = true;
                    counter++;
                }
                else
                {
                    loop = false;
                }
            }         

        }



        //protected bool IsMemoryExhausted(out long currentMB, out long maxMB, out double percent)
        //{
        //    GCMemoryInfo gcInfo = GC.GetGCMemoryInfo();
        //    maxMB = gcInfo.TotalAvailableMemoryBytes / (1024 * 1024);
        //    currentMB = GC.GetTotalMemory(false) / (1024 * 1024);
        //    percent = (double)currentMB / maxMB * 100;

        //    return percent >= MEMORY_THRESHOLD_PERCENT;
        //}

        //protected async Task WaitForMemoryRecoveryAsync(string collectionName)
        //{
        //    const int MAX_WAIT_SECONDS = 60;
        //    DateTime startWait = DateTime.UtcNow;

        //    while (IsMemoryExhausted(out long currentMB, out long maxMB, out double percent))
        //    {
        //        if ((DateTime.UtcNow - startWait).TotalSeconds > MAX_WAIT_SECONDS)
        //        {
        //            // Memory stayed high for 60 seconds - STOP JOB
        //            string errorMsg = $"CRITICAL: Memory exhausted for 60+ seconds ({currentMB}MB / {maxMB}MB = {percent:F1}%). Stopping job to prevent crash.";
        //            _log.ShowInMonitor($"{_syncBackPrefix}ERROR: {errorMsg}");
        //            _log.WriteLine($"{_syncBackPrefix}{errorMsg}", LogType.Error);
        //            throw new OutOfMemoryException(errorMsg);
        //        }

        //        _log.WriteLine($"{_syncBackPrefix}Waiting for memory recovery: {currentMB}MB / {maxMB}MB ({percent:F1}%), {GetGlobalPendingWriteCount()} pending writes", LogType.Warning);
        //        await Task.Delay(5000); // Check every 5 seconds
        //    }

        //    _log.WriteLine($"{_syncBackPrefix}Memory recovered for {collectionName}", LogType.Info);
        //}

        #endregion

        protected IMongoCollection<BsonDocument> GetTargetCollection(string databaseName, string collectionName)
        {
            if (!_syncBack)
            {
                if (!_job.IsSimulatedRun)
                {
                    var targetDb = _targetClient.GetDatabase(databaseName);
                    return targetDb.GetCollection<BsonDocument>(collectionName);
                }
                else
                {
                    // In simulated runs, use source collection as placeholder
                    var sourceDb = _sourceClient.GetDatabase(databaseName);
                    return sourceDb.GetCollection<BsonDocument>(collectionName);
                }
            }
            else
            {
                // For sync back, target is the source
                var targetDb = _sourceClient.GetDatabase(databaseName);
                return targetDb.GetCollection<BsonDocument>(collectionName);
            }
        }

        protected int GetBatchDurationInSeconds(float timeFactor = 1)
        {
            // Calculate batch duration with time factor
            int seconds = (int)(_processorRunMaxDurationInSec * timeFactor);
            if (seconds < _processorRunMinDurationInSec)
                seconds = _processorRunMinDurationInSec; // Ensure at least minimum duration
            return seconds;
        }

        protected void IncrementFailureCounter(MigrationUnit mu, int incrementBy = 1)
        {
            if (!_syncBack)
                mu.CSErrors = mu.CSErrors + incrementBy;
            else
                mu.SyncBackErrors = mu.SyncBackErrors + incrementBy;
        }

        protected void IncrementSkippedCounter(MigrationUnit mu, int incrementBy = 1)
        {
            if (!_syncBack)
                mu.CSDuplicateDocsSkipped = mu.CSDuplicateDocsSkipped + incrementBy;
            else
                mu.SyncBackDuplicateDocsSkipped = mu.SyncBackDuplicateDocsSkipped + incrementBy;
        }

        protected void IncrementDocCounter(MigrationUnit mu, ChangeStreamOperationType op, int incrementBy = 1)
        {
            if (op == ChangeStreamOperationType.Insert)
            {
                if (!_syncBack)
                    mu.CSDocsInserted = mu.CSDocsInserted + incrementBy;
                else
                    mu.SyncBackDocsInserted = mu.SyncBackDocsInserted + incrementBy;
            }
            else if (op == ChangeStreamOperationType.Update || op == ChangeStreamOperationType.Replace)
            {
                if (!_syncBack)
                    mu.CSDocsUpdated = mu.CSDocsUpdated + incrementBy;
                else
                    mu.SyncBackDocsUpdated = mu.SyncBackDocsUpdated + incrementBy;
            }
            else if (op == ChangeStreamOperationType.Delete)
            {
                if (!_syncBack)
                    mu.CSDocsDeleted = mu.CSDocsDeleted + incrementBy;
                else
                    mu.SyncBackDocsDeleted = mu.SyncBackDocsDeleted + incrementBy;
            }
        }

        protected void IncrementEventCounter(MigrationUnit mu, ChangeStreamOperationType op)
        {
            if (op == ChangeStreamOperationType.Insert)
            {
                if (!_syncBack)
                    mu.CSDInsertEvents++;
                else
                    mu.SyncBackInsertEvents++;
            }
            else if (op == ChangeStreamOperationType.Update || op == ChangeStreamOperationType.Replace)
            {
                if (!_syncBack)
                    mu.CSUpdateEvents++;
                else
                    mu.SyncBackUpdateEvents++;
            }
            else if (op == ChangeStreamOperationType.Delete)
            {
                if (!_syncBack)
                    mu.CSDeleteEvents++;
                else
                    mu.SyncBackDeleteEvents++;
            }
        }

        protected void ResetCounters(MigrationUnit mu)
        {
            if (!_syncBack)
            {
                mu.CSDocsUpdated = 0;
                mu.CSDocsInserted = 0;
                mu.CSDocsDeleted = 0;
                mu.CSDuplicateDocsSkipped = 0;

                mu.CSDInsertEvents = 0;
                mu.CSDeleteEvents = 0;
                mu.CSUpdateEvents = 0;
            }
            else
            {
                mu.SyncBackDocsUpdated = 0;
                mu.SyncBackDocsInserted = 0;
                mu.SyncBackDocsDeleted = 0;
                mu.SyncBackDuplicateDocsSkipped = 0;

                mu.SyncBackInsertEvents = 0;
                mu.SyncBackDeleteEvents = 0;
                mu.SyncBackUpdateEvents = 0;
            }
        }

        protected async Task BulkProcessChangesAsync(
            MigrationUnit mu,
            IMongoCollection<BsonDocument> collection,
            List<ChangeStreamDocument<BsonDocument>> insertEvents,
            List<ChangeStreamDocument<BsonDocument>> updateEvents,
            List<ChangeStreamDocument<BsonDocument>> deleteEvents,
            int batchSize = 50)
        {
            string collectionKey = $"{mu.DatabaseName}.{mu.CollectionName}";
            _log.WriteLine($"{_syncBackPrefix}BulkProcessChangesAsync started - Collection: {collectionKey}, Inserts: {insertEvents.Count}, Updates: {updateEvents.Count}, Deletes: {deleteEvents.Count}, BatchSize: {batchSize}", LogType.Debug);
            
            CounterDelegate<MigrationUnit> counterDelegate = (migrationUnit, counterType, operationType, count) =>
            {
                switch (counterType)
                {
                    case CounterType.Processed:
                        if (operationType.HasValue)
                        {
                            IncrementDocCounter(migrationUnit, operationType.Value, count);
                        }
                        break;
                    case CounterType.Skipped:
                        IncrementSkippedCounter(migrationUnit, count);
                        break;
                }
            };

            try
            {
                // Get context for aggressive change stream functionality
                bool isAggressive = _job.AggresiveChangeStream;
                bool isAggressiveComplete = mu.AggressiveCacheDeleted;
                string jobId = _job.Id ?? string.Empty;
                bool isSimulatedRun = _job.IsSimulatedRun;
                
                _log.WriteLine($"{_syncBackPrefix}Processing context - Aggressive: {isAggressive}, AggressiveComplete: {isAggressiveComplete}, Simulated: {isSimulatedRun} for {collectionKey}", LogType.Verbose);

                // Use ParallelWriteProcessor for improved performance with retry logic
                var parallelProcessor = new ParallelWriteProcessor(_log, _syncBackPrefix);
                
                var result = await parallelProcessor.ProcessWritesAsync(
                    mu,
                    collection,
                    insertEvents,
                    updateEvents,
                    deleteEvents,
                    counterDelegate,
                    batchSize,
                    isAggressive,
                    isAggressiveComplete,
                    jobId,
                    _targetClient,
                    isSimulatedRun);

                _log.WriteLine($"{_syncBackPrefix}ParallelWriteProcessor completed - Success: {result.Success}, TotalFailures: {result.TotalFailures} for {collectionKey}", LogType.Debug);

                if (!result.Success)
                {
                    IncrementFailureCounter(mu, result.TotalFailures);
                    _log.WriteLine($"{_syncBackPrefix}Bulk processing had {result.TotalFailures} failures for {collectionKey}", LogType.Debug);
                    
                    // If there were critical errors that would cause data loss, stop the job
                    if (result.Errors.Any(e => e.Contains("CRITICAL")))
                    {
                        var criticalError = result.Errors.First(e => e.Contains("CRITICAL"));
                        _log.WriteLine($"{_syncBackPrefix}Stopping job due to critical error: {criticalError}", LogType.Error);
                        throw new InvalidOperationException(criticalError);
                    }
                }
                else if (result.TotalFailures > 0)
                {
                    IncrementFailureCounter(mu, result.TotalFailures);
                    _log.WriteLine($"{_syncBackPrefix}Bulk processing had {result.TotalFailures} non-critical failures for {collectionKey}", LogType.Verbose);
                }
                
                _log.WriteLine($"{_syncBackPrefix}BulkProcessChangesAsync completed successfully for {collectionKey}", LogType.Verbose);
            }
            catch (InvalidOperationException ex) when (ex.Message.Contains("CRITICAL") && ex.Message.Contains("persistent deadlock"))
            {
                // Critical deadlock failure - re-throw to stop the job and prevent data loss
                _log.WriteLine($"{_syncBackPrefix}Stopping job due to persistent deadlock that would cause data loss. Details: {ex.Message}", LogType.Error);
                throw; // Re-throw to stop the entire migration job
            }
            catch (InvalidOperationException ex) when (ex.Message.Contains("CRITICAL"))
            {
                // Critical error that would cause data loss - re-throw to stop the job
                _log.WriteLine($"{_syncBackPrefix}Stopping job due to critical error that would cause data loss. Details: {ex.Message}", LogType.Error);
                throw; // Re-throw to stop the entire migration job
            }
            catch (Exception ex)
            {
                _log.WriteLine($"{_syncBackPrefix}Error processing operations for {collection.CollectionNamespace.FullName}. Details: {ex}", LogType.Error);
                throw; // Re-throw all exceptions to ensure they are handled upstream
            }
        }

        protected async Task CleanupAggressiveCSAsync(MigrationUnit mu)
        {
            if (!_job.AggresiveChangeStream || !mu.RestoreComplete || _job.IsSimulatedRun)
            {
                return;
            }

            string collectionKey = $"{mu.DatabaseName}.{mu.CollectionName}";

            // Check if cleanup has already been processed for this collection
            lock (_cleanupLock)
            {
                if (mu.AggressiveCacheDeleted || _aggressiveCleanupProcessed.ContainsKey(collectionKey))
                {
                    _log.WriteLine($"Aggressive change stream cleanup already processed for {collectionKey}", LogType.Verbose);
                    return;
                }

                // Mark as being processed to prevent concurrent execution
                _aggressiveCleanupProcessed.TryAdd(collectionKey, true);
            }

            try
            {
                var aggressiveHelper = new AggressiveChangeStreamHelper(_targetClient, _log, _job.Id ?? string.Empty);

                _log.WriteLine($"Processing aggressive change stream cleanup for {mu.DatabaseName}.{mu.CollectionName}");

                long deletedCount = await aggressiveHelper.DeleteStoredDocsAsync(mu.DatabaseName, mu.CollectionName);

                // Mark cleanup as completed
                lock (_cleanupLock)
                {
                    mu.AggressiveCacheDeleted = true;
                    mu.AggressiveCacheDeletedOn = DateTime.UtcNow;
                }

                // retry deletion in case some documents were added during the first deletion pass
                deletedCount += await aggressiveHelper.DeleteStoredDocsAsync(mu.DatabaseName, mu.CollectionName);

                if (deletedCount > 0)
                {
                    // Update counters
                    if (!_syncBack)
                        mu.CSDocsDeleted += deletedCount;
                    else
                        mu.SyncBackDocsDeleted += deletedCount;

                    _log.WriteLine($"Aggressive change stream cleanup completed for {mu.DatabaseName}.{mu.CollectionName}: {deletedCount} documents deleted");
                }
                else
                {
                    _log.WriteLine($"Aggressive change stream cleanup completed for {mu.DatabaseName}.{mu.CollectionName}: No documents to delete");
                }
                // Save the updated state
                _jobList?.Save();
            }
            catch (Exception ex)
            {
                _log.WriteLine($"Error during aggressive change stream cleanup for {mu.DatabaseName}.{mu.CollectionName}: {ex.Message}", LogType.Error);

                // Remove from processing cache to allow retry
                lock (_cleanupLock)
                {
                    _aggressiveCleanupProcessed.TryRemove(collectionKey, out _);
                }
            }
        }

        public async Task CleanupAggressiveCSAllCollectionsAsync()
        {
            if (!_job.AggresiveChangeStream || _job.IsSimulatedRun)
            {
                return;
            }

            // Check if final cleanup has already been executed
            lock (_cleanupLock)
            {
                if (_finalCleanupExecuted)
                {
                    _log.WriteLine("Aggressive change stream final cleanup already executed", LogType.Verbose);
                    return;
                }
                _finalCleanupExecuted = true;
            }

            try
            {
                _log.WriteLine("Starting aggressive change stream cleanup for all completed collections");

                int processedCount = 0;
                int skippedCount = 0;

                foreach (var migrationUnit in _job.MigrationUnits ?? new List<MigrationUnit>())
                {
                    if (migrationUnit.RestoreComplete && Helper.IsMigrationUnitValid(migrationUnit))
                    {
                        if (!migrationUnit.AggressiveCacheDeleted)
                        {
                            await CleanupAggressiveCSAsync(migrationUnit);
                            processedCount++;
                        }
                        else
                        {
                            skippedCount++;
                            _log.ShowInMonitor($"Skipping aggressive cleanup for {migrationUnit.DatabaseName}.{migrationUnit.CollectionName} - already processed");
                        }
                    }
                }

                // Final cleanup of any remaining temp collections
                try
                {
                    var aggressiveHelper = new AggressiveChangeStreamHelper(_targetClient, _log, _job.Id ?? string.Empty);
                    await aggressiveHelper.CleanupTempDatabaseAsync();
                }
                catch (Exception ex)
                {
                    _log.WriteLine($"Error during final aggressive change stream cleanup: {ex.Message}", LogType.Error);
                }

                _log.WriteLine($"Aggressive change stream cleanup completed for all collections: {processedCount} processed, {skippedCount} already completed");
            }
            catch (Exception ex)
            {
                _log.WriteLine($"Error during CleanupAggressiveCSAllCollectionsAsync: {ex.Message}", LogType.Error);

                // Reset flag to allow retry
                lock (_cleanupLock)
                {
                    _finalCleanupExecuted = false;
                }
            }
        }

        #region Critical Failure Tracking - Common for Server and Collection Level

        /// <summary>
        /// Check for critical failures and throw if detected
        /// Should be called at the start of main processing loops
        /// </summary>
        protected void CheckForCriticalFailure()
        {
            if (_criticalFailureDetected && _criticalFailureException != null)
            {
                _log.WriteLine($"{_syncBackPrefix}CRITICAL: Background task failure detected. Stopping processing.", LogType.Error);
                throw _criticalFailureException;
            }
        }

        /// <summary>
        /// Track a background task and monitor for critical failures
        /// Use this instead of fire-and-forget to maintain exception visibility
        /// </summary>
        protected void TrackBackgroundTask(Task task)
        {
            
            try
            {
                var monitoredTask = Task.Run(async () =>
                {
                    try
                    {
                        await task;
                    }
                    catch (InvalidOperationException ex) when (ex.Message.Contains("CRITICAL"))
                    {
                        _criticalFailureDetected = true;
                        _criticalFailureException = ex;
                        _log.WriteLine($"{_syncBackPrefix}CRITICAL failure in background task. Job must terminate. Error: {ex.Message}", LogType.Error);
                    }
                    catch (AggregateException aex) when (aex.InnerExceptions.Any(e => e is InvalidOperationException ioe && ioe.Message.Contains("CRITICAL")))
                    {
                        var criticalEx = aex.InnerExceptions.First(e => e is InvalidOperationException ioe && ioe.Message.Contains("CRITICAL"));
                        _criticalFailureDetected = true;
                        _criticalFailureException = criticalEx;
                        _log.WriteLine($"{_syncBackPrefix}CRITICAL failure (from AggregateException) in background task. Job must terminate. Error: {criticalEx.Message}", LogType.Error);
                    }
                    catch (Exception ex)
                    {
                        _log.WriteLine($"{_syncBackPrefix}Non-critical exception in background task. Error: {ex.Message}", LogType.Error);
                    }
                });

                _backgroundProcessingTasks.Add(monitoredTask);
                PruneCompletedBackgroundTasks();
            }
            catch
            {
                throw;
            }
        }

        /// <summary>
        /// Prune completed background tasks to prevent memory leak
        /// </summary>
        protected void PruneCompletedBackgroundTasks()
        {
            var activeTasks = new ConcurrentBag<Task>();
            foreach (var task in _backgroundProcessingTasks)
            {
                if (!task.IsCompleted)
                {
                    activeTasks.Add(task);
                }
            }
            
            _backgroundProcessingTasks.Clear();
            foreach (var task in activeTasks)
            {
                _backgroundProcessingTasks.Add(task);
            }
        }

        /// <summary>
        /// Wait for all background tasks to complete and check for critical failures
        /// Should be called during shutdown
        /// </summary>
        protected async Task WaitForBackgroundTasksAndCheckFailures()
        {
            if (_backgroundProcessingTasks.Count > 0)
            {
                _log.WriteLine($"{_syncBackPrefix}Waiting for {_backgroundProcessingTasks.Count} background processing tasks to complete", LogType.Debug);
                try
                {
                    await Task.WhenAll(_backgroundProcessingTasks);
                    _log.WriteLine($"{_syncBackPrefix}All background processing tasks completed successfully", LogType.Verbose);
                }
                catch (Exception ex)
                {
                    _log.WriteLine($"{_syncBackPrefix}Error waiting for background tasks: {ex.Message}", LogType.Error);
                }
            }

            CheckForCriticalFailure();
        }

        #endregion

        #region IDisposable Implementation
        
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!_disposed && disposing)
            {
                // Dispose managed resources - override in derived classes if needed
                ExecutionCancelled = true;
                _disposed = true;
            }
        }

        ~ChangeStreamProcessor()
        {
            Dispose(false);
        }

        #endregion
    }
}
