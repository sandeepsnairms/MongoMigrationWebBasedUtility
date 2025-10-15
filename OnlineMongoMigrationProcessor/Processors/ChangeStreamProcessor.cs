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

        private bool _disposed = false;

        public bool ExecutionCancelled { get; set; }

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
        }

        public bool AddCollectionsToProcess(MigrationUnit mu, CancellationTokenSource cts)
        {
            string key = $"{mu.DatabaseName}.{mu.CollectionName}";
            if (!Helper.IsMigrationUnitValid(mu)|| ((mu.DumpComplete != true || mu.RestoreComplete != true) && !_job.AggresiveChangeStream))
            {
                _log.WriteLine($"{_syncBackPrefix}Cannot add {key} to change streams for processing.", LogType.Error);
                return false;
            }
            if (!_migrationUnitsToProcess.ContainsKey(key))
            {
                _migrationUnitsToProcess.TryAdd(key, mu);
                _log.WriteLine($"{_syncBackPrefix}Change stream for {key} added to queue.", LogType.Debug);

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
                            _ = Task.Run(async () => await CleanupAggressiveCSAsync(migrationUnit));
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

               
                // Clone the event collections to allow original collections to be cleared immediately
                var insertEventsClone = new List<ChangeStreamDocument<BsonDocument>>(insertEvents);
                var updateEventsClone = new List<ChangeStreamDocument<BsonDocument>>(updateEvents);
                var deleteEventsClone = new List<ChangeStreamDocument<BsonDocument>>(deleteEvents);


                // Start all processing operations asynchronously on separate threads
                var insertTask = Task.Run(async () =>
                    await MongoHelper.ProcessInsertsAsync<MigrationUnit>(
                        mu, collection, insertEventsClone, counterDelegate, _log, _syncBackPrefix,
                        batchSize, isAggressive, isAggressiveComplete, jobId, _targetClient, isSimulatedRun));

                var updateTask = Task.Run(async () =>
                    await MongoHelper.ProcessUpdatesAsync<MigrationUnit>(
                        mu, collection, updateEventsClone, counterDelegate, _log, _syncBackPrefix,
                        batchSize, isAggressive, isAggressiveComplete, jobId, _targetClient, isSimulatedRun));

                var deleteTask = Task.Run(async () =>
                    await MongoHelper.ProcessDeletesAsync<MigrationUnit>(
                        mu, collection, deleteEventsClone, counterDelegate, _log, _syncBackPrefix,
                        batchSize, isAggressive, isAggressiveComplete, jobId, _targetClient, isSimulatedRun));

                // Wait for all tasks to complete and get their results
                var results = await Task.WhenAll(insertTask, updateTask, deleteTask);
                
                var insertFailures = results[0];
                var updateFailures = results[1];
                var deleteFailures = results[2];

                var totalFailures = insertFailures + updateFailures + deleteFailures;
                if (totalFailures > 0)
                {
                    IncrementFailureCounter(mu, totalFailures);
                }
            }
            catch (InvalidOperationException ex) when (ex.Message.Contains("CRITICAL") && ex.Message.Contains("persistent deadlock"))
            {
                // Critical deadlock failure - re-throw to stop the job and prevent data loss
                _log.WriteLine($"{_syncBackPrefix}Stopping job due to persistent deadlock that would cause data loss. Details: {ex.Message}", LogType.Error);
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
                    _log.WriteLine($"Aggressive change stream cleanup already processed for {collectionKey}", LogType.Debug);
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
            if (!_job.AggresiveChangeStream)
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
                _log.WriteLine($"{_syncBackPrefix}Waiting for {_backgroundProcessingTasks.Count} background tasks to complete...", LogType.Debug);
                try
                {
                    await Task.WhenAll(_backgroundProcessingTasks);
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