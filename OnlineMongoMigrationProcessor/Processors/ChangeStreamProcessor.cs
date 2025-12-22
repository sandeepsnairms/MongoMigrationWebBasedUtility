using MongoDB.Bson;
using MongoDB.Driver;
using OnlineMongoMigrationProcessor.Helpers;
using OnlineMongoMigrationProcessor.Helpers.JobManagement;
using OnlineMongoMigrationProcessor.Models;
using OnlineMongoMigrationProcessor.Workers;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using static OnlineMongoMigrationProcessor.Helpers.Mongo.MongoHelper;
using OnlineMongoMigrationProcessor.Helpers.Mongo;
using OnlineMongoMigrationProcessor.Context;

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
        //protected JobList? _jobList;
        //protected MigrationJob? MigrationJobContext.CurrentlyActiveJob;

        protected MigrationSettings? _config;
        protected bool _syncBack = false;
        protected string _syncBackPrefix = string.Empty;
        protected bool _isCSProcessing = false;
        protected Log _log;

        // Reference to MigrationWorker for coordinated shutdown
        protected MigrationWorker? _migrationWorker;
       
        // Resume token cache - used by collection-level processors to track individual collection resume tokens
        // Server-level processors don't need this as they use MigrationJob properties directly for global tokens
        protected virtual bool UseResumeTokenCache => true; // Override in server-level processor to return false
        protected ConcurrentDictionary<string, string> _resumeTokenCache = new ConcurrentDictionary<string, string>();
        protected ConcurrentDictionary<string, long> _migrationUnitsToProcess = new ConcurrentDictionary<string, long>();

        // Tracking for aggressive cleanup to prevent duplicate executions
        protected ConcurrentDictionary<string, bool> _aggressiveCleanupProcessed = new ConcurrentDictionary<string, bool>();
        protected bool _finalCleanupExecuted = false;

        // Critical failure tracking - shared across server-level and collection-level processors
        protected readonly ConcurrentBag<Task> _backgroundProcessingTasks = new();
        protected volatile bool _criticalFailureDetected = false;
        protected Exception? _criticalFailureException = null;

        protected static readonly object _processingLock = new object();
        protected static readonly object _cleanupLock = new object();

        // Delegate to wait for resume token setup task for a specific collection
        public Func<string, Task>? WaitForResumeTokenTaskDelegate { get; set; }

        // Global backpressure tracking across ALL collections/processors to prevent OOM
        protected static readonly object _pendingWritesLock = new object();

        // UI update throttling to prevent Blazor rendering OOM (max 1 update per second per collection)
        const int GLOBAL_UI_UPDATE_INTERVAL_MS = 500; // 500ms global throttling across all collections
        protected readonly ConcurrentDictionary<string, DateTime> _lastUIUpdateTime = new ConcurrentDictionary<string, DateTime>();

        protected bool StopProcessing = false;

        private bool _disposed = false;
        protected DateTime _lastGlobalUIUpdate = DateTime.MinValue; // Track last global UI update time for 500ms throttling

        protected Dictionary<string, AccumulatedChangesTracker> _accumulatedChangesPerCollection = new Dictionary<string, AccumulatedChangesTracker>();

        protected void InitializeAccumulatedChangesTracker(string collectionKey)
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

        /// <summary>
        /// Stops the job immediately by setting flags and calling the migration worker's stop method
        /// </summary>
        protected void StopJob(string reason)
        {
            _log.WriteLine($"{_syncBackPrefix}StopJob called - Reason: {reason}", LogType.Error);
            
            // Set flags to stop processing loops
            StopProcessing = true;
            ExecutionCancelled = true;
            
            // Call the MigrationWorker's stop migration to coordinate shutdown
            try
            {
                if (_migrationWorker is not null)
                {
                    _log.WriteLine($"{_syncBackPrefix}Requesting immediate job termination via MigrationWorker", LogType.Error);
                    _migrationWorker.StopMigration();
                }
                else
                {
                    _log.WriteLine($"{_syncBackPrefix}MigrationWorker reference is null, cannot coordinate shutdown", LogType.Warning);
                }
            }
            catch (Exception ex)
            {
                _log.WriteLine($"{_syncBackPrefix}Error calling MigrationWorker.StopMigration: {ex.Message}", LogType.Error);
            }
        }

        public bool ExecutionCancelled { 
            get => _executionCancelled; 
            set 
            {
                if (_executionCancelled != value)
                {
                    //_log.WriteLine($"{_syncBackPrefix}ExecutionCancelled state change - From: {_executionCancelled}, To: {value}", LogType.Debug);
                    _executionCancelled = value;
                }
            }
        }
        private bool _executionCancelled = false;


        public ChangeStreamProcessor(Log log, MongoClient sourceClient, MongoClient targetClient,  ActiveMigrationUnitsCache muCache, MigrationSettings config, bool syncBack = false, MigrationWorker? migrationWorker = null)
        {
             _log = log;
            _sourceClient = sourceClient;
            _targetClient = targetClient;
            MigrationJobContext.MigrationUnitsCache= muCache;
            _config = config;
            _syncBack = syncBack;
            _migrationWorker = migrationWorker;
            if (_syncBack)
                _syncBackPrefix = "SyncBack: ";

            _concurrentProcessors = _config?.ChangeStreamMaxCollsInBatch ?? 5;
            _processorRunMaxDurationInSec = _config?.ChangeStreamBatchDuration ?? 120;
            _processorRunMinDurationInSec = _config?.ChangeStreamBatchDurationMin ?? 30;
            StopProcessing = false;

        }

        public bool AddCollectionsToProcess(string migrationUnitId, CancellationTokenSource cts)
        {
            var mu = MigrationJobContext.GetMigrationUnit(MigrationJobContext.CurrentlyActiveJob.Id,migrationUnitId);
            string key = $"{mu.DatabaseName}.{mu.CollectionName}";

            _log.WriteLine($"{_syncBackPrefix} AddCollectionsToProcess invoked for {key}", LogType.Debug);

            _log.WriteLine($"{_syncBackPrefix}Evaluating {key} for change streams processing. IsValid:{Helper.IsMigrationUnitValid(mu)} DumpComplete: {mu.DumpComplete}, RestoreComplete: {mu.RestoreComplete}", LogType.Debug);
            if (!Helper.IsMigrationUnitValid(mu)|| ((mu.DumpComplete != true || mu.RestoreComplete != true) && MigrationJobContext.CurrentlyActiveJob.ChangeStreamMode != ChangeStreamMode.Aggressive))
            {
                _log.WriteLine($"{_syncBackPrefix}Cannot add {key} to change streams for processing.", LogType.Debug);
                return false;
            }

            if (!_migrationUnitsToProcess.ContainsKey(mu.Id))
            {
                _migrationUnitsToProcess.TryAdd(mu.Id, 0);
                _log.WriteLine($"{_syncBackPrefix}Collection added to change stream queue - Key: {key}, DumpComplete: {mu.DumpComplete}, RestoreComplete: {mu.RestoreComplete}", LogType.Debug);
                _log.ShowInMonitor($"Change stream processor added {mu.DatabaseName}.{mu.CollectionName} to the monitoring queue.");
                _ = RunCSPostProcessingAsync(cts); // fire-and-forget by design
                return true;
            }
            else
            {
                _log.WriteLine($"{_syncBackPrefix} {key} is already added for change stream  processing.", LogType.Debug);
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

            _log.WriteLine($"{_syncBackPrefix} RunCSPostProcessingAsync invoked", LogType.Debug);
            try
            {
                cts = new CancellationTokenSource();
                var token = cts.Token;

                _migrationUnitsToProcess.Clear();
                foreach (var mu in MigrationJobContext.CurrentlyActiveJob.MigrationUnitBasics)
                {
                    _log.WriteLine($"{_syncBackPrefix}RunCSPostProcessingAsync looping for {mu.Id} ", LogType.Debug);

                    var migrationUnit = MigrationJobContext.GetMigrationUnit(MigrationJobContext.CurrentlyActiveJob.Id, mu.Id);
                    if (migrationUnit != null && (Helper.IsMigrationUnitValid(migrationUnit) && ((migrationUnit.DumpComplete == true && migrationUnit.RestoreComplete == true) || MigrationJobContext.CurrentlyActiveJob.ChangeStreamMode == ChangeStreamMode.Aggressive)))
                    {
                        _migrationUnitsToProcess[migrationUnit.Id] = migrationUnit.CSNormalizedUpdatesInLastBatch;

                        // For aggressive change stream, trigger cleanup for completed collections (only if not already processed)
                        if (MigrationJobContext.CurrentlyActiveJob.ChangeStreamMode == ChangeStreamMode.Aggressive && migrationUnit.RestoreComplete && !_syncBack && !migrationUnit.AggressiveCacheDeleted)
                        {
                            string collKey = $"{migrationUnit.DatabaseName}.{migrationUnit.CollectionName}";
                            _log.WriteLine($"{_syncBackPrefix}Aggressive cleanup queued for {collKey}", LogType.Debug);

                            try
                            {
                                _ = Task.Run(async () =>
                                {
                                    try
                                    {
                                        _log.WriteLine($"{_syncBackPrefix}Starting aggressive cleanup Task.Run for {migrationUnit.DatabaseName}.{migrationUnit.CollectionName}", LogType.Debug);
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
                        else
                        { 
                            _log.WriteLine($"{_syncBackPrefix}Skipping aggressive cleanup for {migrationUnit.DatabaseName}.{migrationUnit.CollectionName} - RestoreComplete: {migrationUnit.RestoreComplete}, AggressiveCacheDeleted: {migrationUnit.AggressiveCacheDeleted}", LogType.Debug);
                        }
                    }
                }

                MigrationJobContext.CurrentlyActiveJob.CSPostProcessingStarted = true;

                MigrationJobContext.SaveMigrationJob(MigrationJobContext.CurrentlyActiveJob); // persist state


                if (_migrationUnitsToProcess.Count == 0)
                {
                    _log.WriteLine($"{_syncBackPrefix}No change streams to process.");

                    _isCSProcessing = false;
                    return;
                }

                _log.WriteLine($"{_syncBackPrefix}Starting change stream processing for {_migrationUnitsToProcess.Count} collections.",LogType.Debug);

                await ProcessChangeStreamsAsync(token);

                _log.WriteLine($"{_syncBackPrefix}Change stream processing completed or paused.");

                MigrationJobContext.SaveMigrationJob(MigrationJobContext.CurrentlyActiveJob);

            }
            catch (OperationCanceledException)
            {
                _log.WriteLine($"{_syncBackPrefix}Change stream processing was paused.");
            }
            catch (Exception ex) when (ex is TimeoutException)
            {
                _log.WriteLine($"{_syncBackPrefix}Timeout during change stream processing: {ex}", LogType.Debug);
            }
            catch (Exception ex) 
            {
                _log.WriteLine($"{_syncBackPrefix}Error during change stream processing: {ex}", LogType.Debug);
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

        
        protected IMongoCollection<BsonDocument> GetTargetCollection(string databaseName, string collectionName)
        {
            if (!_syncBack)
            {
                if (!MigrationJobContext.CurrentlyActiveJob.IsSimulatedRun)
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

        

        protected async Task BulkProcessChangesAsync(
            MigrationUnit mu,
            IMongoCollection<BsonDocument> collection,
            List<ChangeStreamDocument<BsonDocument>> insertEvents,
            List<ChangeStreamDocument<BsonDocument>> updateEvents,
            List<ChangeStreamDocument<BsonDocument>> deleteEvents,
            AccumulatedChangesTracker accumulatedChangesInColl,
            int batchSize = 50)
        {
            
            string collectionKey = $"{mu.DatabaseName}.{mu.CollectionName}";

            if ((insertEvents.Count + updateEvents.Count + deleteEvents.Count) > 0)
                _log.ShowInMonitor($"{_syncBackPrefix}Flushing Changes for Collection: {collectionKey}, Events: {accumulatedChangesInColl.TotalEventCount}, Inserts: {insertEvents.Count}, Updates: {updateEvents.Count}, Deletes: {deleteEvents.Count}, BatchSize: {batchSize}");

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
                bool isAggressive = MigrationJobContext.CurrentlyActiveJob.ChangeStreamMode == ChangeStreamMode.Aggressive;
                bool isAggressiveComplete = mu.AggressiveCacheDeleted;
                string jobId = MigrationJobContext.CurrentlyActiveJob.Id ?? string.Empty;
                bool isSimulatedRun = MigrationJobContext.CurrentlyActiveJob.IsSimulatedRun;

                MigrationJobContext.AddVerboseLog($"{_syncBackPrefix}Processing context - Aggressive: {isAggressive}, AggressiveComplete: {isAggressiveComplete}, Simulated: {isSimulatedRun} for {collectionKey}");

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

                MigrationJobContext.AddVerboseLog($"{_syncBackPrefix}ParallelWriteProcessor completed - Success: {result.Success}, TotalFailures: {result.Failures}, WriteLatency: {result.WriteLatencyMS}ms for {collectionKey}");

                // Track write latency directly in AccumulatedChangesTracker
                accumulatedChangesInColl.CSTotaWriteDurationInMS += result.WriteLatencyMS;

                if (!result.Success)
                {
                    IncrementFailureCounter(mu, result.Failures);
                    _log.WriteLine($"{_syncBackPrefix}Bulk processing had {result.Failures} failures for {collectionKey}", LogType.Debug);
                    
                    // If there were critical errors that would cause data loss, stop the job
                    if (result.Errors.Any(e => e.Contains("CRITICAL")))
                    {
                        var criticalError = result.Errors.First(e => e.Contains("CRITICAL"));
                        _log.WriteLine($"{_syncBackPrefix}Stopping job due to critical error: {criticalError}", LogType.Error);
                        throw new InvalidOperationException(criticalError);
                    }
                }
                else if (result.Failures > 0)
                {
                    IncrementFailureCounter(mu, result.Failures);
                    _log.WriteLine($"{_syncBackPrefix}Bulk processing had {result.Failures} non-critical failures for {collectionKey}", LogType.Debug);
                }
                
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
            MigrationJobContext.AddVerboseLog($"ChangeStreamProcessor.CleanupAggressiveCSAsync: muId={mu.Id}, collection={mu.DatabaseName}.{mu.CollectionName}, RestoreComplete={mu.RestoreComplete}");

            if (MigrationJobContext.CurrentlyActiveJob.ChangeStreamMode != ChangeStreamMode.Aggressive || !mu.RestoreComplete || MigrationJobContext.CurrentlyActiveJob.IsSimulatedRun)
            {
                return;
            }

            string collectionKey = $"{mu.DatabaseName}.{mu.CollectionName}";

            // Check if cleanup has already been processed for this collection
            lock (_cleanupLock)
            {
                if (mu.AggressiveCacheDeleted || _aggressiveCleanupProcessed.ContainsKey(collectionKey))
                {
                    _log.WriteLine($"Aggressive change stream cleanup already processed for {collectionKey}", LogType.Debug);
                    return;
                }

                // Mark as being processed to prevent concurrent execution
                _aggressiveCleanupProcessed.TryAdd(collectionKey, true);
            }

            try
            {
                var aggressiveHelper = new AggressiveChangeStreamHelper(_targetClient, _log, MigrationJobContext.CurrentlyActiveJob.Id ?? string.Empty);

                _log.WriteLine($"Processing aggressive change stream cleanup for {mu.DatabaseName}.{mu.CollectionName}");

                // First, apply stored inserts and updates
                var (inserted, updated, skipped) = await aggressiveHelper.ApplyStoredChangesAsync(mu.DatabaseName, mu.CollectionName, _sourceClient);
                
                if (inserted > 0 || updated > 0)
                {
                    // Update counters
                    if (!_syncBack)
                    {
                        mu.CSDocsInserted += inserted;
                        mu.CSDocsUpdated += updated;
                    }
                    else
                    {
                        mu.SyncBackDocsInserted += inserted;
                        mu.SyncBackDocsUpdated += updated;
                    }

                    _log.WriteLine($"Aggressive change stream applied changes for {mu.DatabaseName}.{mu.CollectionName}: {inserted} inserted, {updated} updated, {skipped} skipped");
                }

                // Then, process deletes
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
                MigrationJobContext.SaveMigrationJob(MigrationJobContext.CurrentlyActiveJob);
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
            if (MigrationJobContext.CurrentlyActiveJob.ChangeStreamMode != ChangeStreamMode.Aggressive || MigrationJobContext.CurrentlyActiveJob.IsSimulatedRun)
            {
                return;
            }

            // Check if final cleanup has already been executed
            lock (_cleanupLock)
            {
                if (_finalCleanupExecuted)
                {
                    _log.WriteLine("Aggressive change stream final cleanup already executed", LogType.Debug);
                    return;
                }
                _finalCleanupExecuted = true;
            }

            try
            {
                _log.WriteLine("Starting aggressive change stream cleanup for all completed collections");

                int processedCount = 0;
                int skippedCount = 0;

                foreach (var mub in MigrationJobContext.CurrentlyActiveJob.MigrationUnitBasics)
                {
                    var migrationUnit = MigrationJobContext.GetMigrationUnit(MigrationJobContext.CurrentlyActiveJob.Id, mub.Id);
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
                    var aggressiveHelper = new AggressiveChangeStreamHelper(_targetClient, _log, MigrationJobContext.CurrentlyActiveJob.Id ?? string.Empty);
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
		/*
        protected bool IsReadyForFlush(AccumulatedChangesTracker accumulatedChangesInColl, out int totalAccumulated)
        {
            totalAccumulated = accumulatedChangesInColl.DocsToBeInserted.Count +
                                  accumulatedChangesInColl.DocsToBeUpdated.Count +
                                  accumulatedChangesInColl.DocsToBeDeleted.Count;
            if (totalAccumulated >= _config?.ChangeStreamMaxCollsInBatch)
                return true;
            else
                return false;
        }*/
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
                    _log.ShowInMonitor($"{_syncBackPrefix}{change.OperationType} operation detected in {collNameSpace} for _id: {change.DocumentKey["_id"]} with TS (UTC): {timeStamp}. Sequence in batch #{counter}. Lag: {Helper.GetChangeStreamLag(timeStamp)}");
                }
            }

            return shouldUpdateUI;
        }
        
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
