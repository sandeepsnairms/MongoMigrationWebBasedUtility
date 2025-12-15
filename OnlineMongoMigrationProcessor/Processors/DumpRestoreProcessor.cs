using MongoDB.Bson;
using MongoDB.Driver;
using OnlineMongoMigrationProcessor.Helpers;
using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using OnlineMongoMigrationProcessor.Models;
using OnlineMongoMigrationProcessor.Processors;
using OnlineMongoMigrationProcessor.Workers;
using OnlineMongoMigrationProcessor.Helpers.Mongo;
using OnlineMongoMigrationProcessor.Helpers.JobManagement;
using OnlineMongoMigrationProcessor.Context;

// CS4014: Use explicit discards for intentional fire-and-forget tasks.

namespace OnlineMongoMigrationProcessor
{
    internal class DumpRestoreProcessor : MigrationProcessor
    {
        private string _mongoDumpOutputFolder = Path.Combine(Helper.GetWorkingFolder(), "mongodump");
        private static readonly SemaphoreSlim _uploadLock = new(1, 1);
        private OrderedUniqueList<string> _migrationUnitsPendingUpload = new OrderedUniqueList<string>();
        private bool _uploadLockAcquired = false; // Track if we currently hold the upload lock

        // Worker pool references (shared across all processors via coordinator)
        private WorkerPoolManager? _dumpPool;
        private WorkerPoolManager? _restorePool;
        private readonly string _jobId;
        
        // Thread-safe locks
        private readonly object _pidLock = new object();
        private readonly object _progressLock = new object();
        private readonly object _chunkUpdateLock = new object();
        
        // Error tracking
        private readonly ConcurrentBag<(int ChunkIndex, Exception Error)> _chunkErrors = new();
        
        // Chunk work queues
        private readonly ConcurrentQueue<ChunkWorkItem> _dumpQueue = new();
        private readonly ConcurrentQueue<ChunkWorkItem> _restoreQueue = new();
        
        // Worker context for dynamic spawning (shared between dump and restore)
        private IMongoCollection<BsonDocument>? _currentCollection;
        private string? _currentFolder;
        private MigrationUnit? _currentMigrationUnit;  // Contains DatabaseName and CollectionName
        private string? _currentSourceConnectionString;
        private string? _currentTargetConnectionString;
        private string? _currentDbName;
        private string? _currentColName;
        private int _nextDumpWorkerId = 1;
        private int _nextRestoreWorkerId = 1;
        
        // Worker task tracking for dynamic spawning
        private List<Task<TaskResult>>? _activeDumpWorkerTasks;
        private List<Task<TaskResult>>? _activeRestoreWorkerTasks;
        private readonly object _workerTaskLock = new object();
        
        // Work item class for chunk processing
        private class ChunkWorkItem : IComparable<ChunkWorkItem>
        {
            public int ChunkIndex { get; set; }
            public MigrationUnit MigrationUnit { get; set; } = null!;
            public MigrationChunk Chunk { get; set; } = null!;
            public DateTime QueuedAt { get; set; }
            
            public int CompareTo(ChunkWorkItem? other)
            {
                if (other == null) return 1;
                int result = ChunkIndex.CompareTo(other.ChunkIndex);
                if (result == 0)
                    return QueuedAt.CompareTo(other.QueuedAt);
                return result;
            }
        }
        
        // Result class for parallel restore operations
        private class RestoreResult
        {
            public TaskResult Result { get; set; }
            public int RestoredChunks { get; set; }
            public long RestoredDocs { get; set; }
        }

        // Attempts to enter the upload semaphore without waiting
        private bool TryEnterUploadLock()
        {
            bool acquired = _uploadLock.Wait(0); // non-async instant try
            //Console.WriteLine($"[{DateTime.Now:HH:mm:ss.fff}] TryEnterUploadLock: {acquired}, CurrentCount={_uploadLock.CurrentCount}");
            return acquired;
        }

        public DumpRestoreProcessor(Log log, MongoClient sourceClient, MigrationSettings config, MigrationWorker? migrationWorker = null)
            : base(log,  sourceClient, config, migrationWorker)
        {
            _jobId = MigrationJobContext.CurrentlyActiveJob.Id ?? throw new InvalidOperationException("Job ID cannot be null");
            
            // Calculate optimal concurrency
            int maxDumpWorkers, maxRestoreWorkers;
            if (MigrationJobContext.CurrentlyActiveJob.EnableParallelProcessing)
            {
                maxDumpWorkers = WorkerCountHelper.CalculateOptimalConcurrency(
                    MigrationJobContext.CurrentlyActiveJob.MaxParallelDumpProcesses,
                    isDump: true
                );
                
                maxRestoreWorkers = WorkerCountHelper.CalculateOptimalConcurrency(
                    MigrationJobContext.CurrentlyActiveJob.MaxParallelRestoreProcesses,
                    isDump: false
                );
                
                _log.WriteLine($"Calculated dump concurrency: {maxDumpWorkers}");
                _log.WriteLine($"Calculated restore concurrency: {maxRestoreWorkers}");
            }
            else
            {
                maxDumpWorkers = 1;
                maxRestoreWorkers = 1;
            }
            
            // Get or create shared worker pools from coordinator
            _dumpPool = WorkerPoolCoordinator.GetOrCreateDumpPool(_jobId, _log, maxDumpWorkers);
            _restorePool = WorkerPoolCoordinator.GetOrCreateRestorePool(_jobId, _log, maxRestoreWorkers);
            
            // Store initial values in MigrationJobContext.CurrentlyActiveJob for UI monitoring
            MigrationJobContext.CurrentlyActiveJob.CurrentDumpWorkers = maxDumpWorkers;
            MigrationJobContext.CurrentlyActiveJob.CurrentRestoreWorkers = maxRestoreWorkers;
            
            
            if (!MigrationJobContext.CurrentlyActiveJob.MaxInsertionWorkersPerCollection.HasValue)
            {
                MigrationJobContext.CurrentlyActiveJob.CurrentInsertionWorkers = WorkerCountHelper.CalculateDefaultInsertionWorkers();
            }
            else
            {
                MigrationJobContext.CurrentlyActiveJob.CurrentInsertionWorkers = WorkerCountHelper.ValidateWorkerCount(
                    MigrationJobContext.CurrentlyActiveJob.MaxInsertionWorkersPerCollection.Value
                );
            }
            
            _log.WriteLine($"Parallel processing: Dump workers={maxDumpWorkers}, Restore workers={maxRestoreWorkers}");
        }

        /// <summary>
        /// Adjusts the number of dump workers at runtime. Increase adds workers, decrease reduces capacity.
        /// </summary>
        public void AdjustDumpWorkers(int newCount)
        {
            int workersToSpawn = WorkerPoolCoordinator.AdjustDumpWorkers(_jobId, newCount, _log);
            
            if (workersToSpawn <= 0)
                return;
                
            // Check if there's an active collection being processed
            if (_currentCollection == null || _currentMigrationUnit == null)
            {
                _log.WriteLine($"New dump workers will be utilized when the next collection starts");
                return;
            }
            
            // Check if there's work in the queue
            if (_dumpQueue.Count == 0)
            {
                _log.WriteLine($"No chunks in dump queue - new workers will be utilized when more work becomes available");
                return;
            }
            
            // Spawn new workers immediately
            lock (_workerTaskLock)
            {
                if (_activeDumpWorkerTasks == null)
                {
                    _log.WriteLine($"Worker task list not initialized - new workers will start with next collection");
                    return;
                }
                
                int workersToSpawnNow = Math.Min(workersToSpawn, _dumpQueue.Count);
                string collectionKey = $"{_currentDbName}.{_currentColName}";
                
                _log.WriteLine($"Spawning {workersToSpawnNow} new dump worker(s) immediately for {collectionKey}");
                
                for (int i = 0; i < workersToSpawnNow; i++)
                {
                    int workerId = _nextDumpWorkerId++;
                    
                    // Register worker with coordinator
                    WorkerPoolCoordinator.RegisterDumpWorker(_jobId, collectionKey, workerId, _log);
                    
                    // Capture locals for closure
                    var collection = _currentCollection;
                    var folder = _currentFolder!;
                    var sourceConn = _currentSourceConnectionString!;
                    var targetConn = _currentTargetConnectionString!;
                    var dbName = _currentDbName!;
                    var colName = _currentColName!;
                    
                    var workerTask = Task.Run(async () =>
                    {
                        try
                        {
                            return await DumpWorkerAsync(
                                workerId,
                                collection,
                                folder,
                                sourceConn,
                                targetConn,
                                dbName,
                                colName
                            );
                        }
                        finally
                        {
                            // Mark worker as completed in coordinator
                            WorkerPoolCoordinator.MarkDumpWorkerCompleted(_jobId, collectionKey, workerId, _log);
                        }
                    }, _cts.Token);
                    
                    _activeDumpWorkerTasks.Add(workerTask);
                }
            }
        }

        /// <summary>
        /// Adjusts the number of restore workers at runtime. Increase adds workers, decrease reduces capacity.
        /// </summary>
        public void AdjustRestoreWorkers(int newCount)
        {
            int workersToSpawn = WorkerPoolCoordinator.AdjustRestoreWorkers(_jobId, newCount, _log);
            
            if (workersToSpawn <= 0)
                return;
                
            // Check if there's an active collection being processed
            if (_currentFolder == null || _currentMigrationUnit == null)
            {
                _log.WriteLine($"New restore workers will be utilized when the next collection starts");
                return;
            }
            
            // Check if there's work in the queue
            if (_restoreQueue.Count == 0)
            {
                _log.WriteLine($"No chunks in restore queue - new workers will be utilized when more work becomes available");
                return;
            }
            
            // Spawn new workers immediately
            lock (_workerTaskLock)
            {
                if (_activeRestoreWorkerTasks == null)
                {
                    _log.WriteLine($"Worker task list not initialized - new workers will start with next collection");
                    return;
                }
                
                int workersToSpawnNow = Math.Min(workersToSpawn, _restoreQueue.Count);
                string collectionKey = $"{_currentDbName}.{_currentColName}";
                
                _log.WriteLine($"Spawning {workersToSpawnNow} new restore worker(s) immediately for {collectionKey}");
                
                for (int i = 0; i < workersToSpawnNow; i++)
                {
                    int workerId = _nextRestoreWorkerId++;
                    
                    // Register worker with coordinator
                    WorkerPoolCoordinator.RegisterRestoreWorker(_jobId, collectionKey, workerId, _log);
                    
                    // Capture locals for closure
                    var folder = _currentFolder!;
                    var targetConn = _currentTargetConnectionString!;
                    var dbName = _currentDbName!;
                    var colName = _currentColName!;
                    
                    var workerTask = Task.Run(async () =>
                    {
                        try
                        {
                            return await RestoreWorkerAsync(
                                workerId,
                                _restoreQueue,
                                folder,
                                targetConn,
                                dbName,
                                colName
                            );
                        }
                        finally
                        {
                            // Mark worker as completed in coordinator
                            WorkerPoolCoordinator.MarkRestoreWorkerCompleted(_jobId, collectionKey, workerId, _log);
                        }
                    }, _cts.Token);
                    
                    _activeRestoreWorkerTasks.Add(workerTask);
                }
            }
        }

        /// <summary>
        /// Adjusts the number of insertion workers per collection for mongorestore at runtime.
        /// </summary>
        public void AdjustInsertionWorkers(int newCount)
        {
            WorkerCountHelper.AdjustInsertionWorkers(newCount, _log);
        }

        #region Thread-Safe PID Tracking

        private void RegisterDumpProcess(int pid)
        {
            lock (_pidLock)
            {
                if (!MigrationJobContext.ActiveDumpProcessIds.Contains(pid))
                {
                    MigrationJobContext.ActiveDumpProcessIds.Add(pid);
                    _log.WriteLine($"Registered dump process: PID {pid} (total active: {MigrationJobContext.ActiveDumpProcessIds.Count})");
                    MigrationJobContext.SaveMigrationJob(MigrationJobContext.CurrentlyActiveJob);
                }
            }
        }

        private void UnregisterDumpProcess(int pid)
        {
            lock (_pidLock)
            {
                if (MigrationJobContext.ActiveDumpProcessIds.Remove(pid))
                {
                    _log.WriteLine($"Unregistered dump process: PID {pid} (total active: {MigrationJobContext.ActiveDumpProcessIds.Count})");
                    MigrationJobContext.SaveMigrationJob(MigrationJobContext.CurrentlyActiveJob);
                }
            }
        }

        private void RegisterRestoreProcess(int pid)
        {
            lock (_pidLock)
            {
                if (MigrationJobContext.ActiveRestoreProcessIds.Contains(pid))
                {
                    MigrationJobContext.ActiveRestoreProcessIds.Add(pid);
                    _log.WriteLine($"Registered restore process: PID {pid} (total active: {MigrationJobContext.ActiveRestoreProcessIds.Count})");
                    MigrationJobContext.SaveMigrationJob(MigrationJobContext.CurrentlyActiveJob);
                }
            }
        }

        private void UnregisterRestoreProcess(int pid)
        {
            lock (_pidLock)
            {
                if (MigrationJobContext.ActiveRestoreProcessIds.Remove(pid))
                {
                    _log.WriteLine($"Unregistered restore process: PID {pid} (total active: {MigrationJobContext.ActiveRestoreProcessIds.Count})");
                    MigrationJobContext.SaveMigrationJob(MigrationJobContext.CurrentlyActiveJob);
                }
            }
        }

        private void UpdateChunkStatusSafe(MigrationChunk chunk, Action updateAction)
        {
            lock (_chunkUpdateLock)
            {
                updateAction();
                MigrationJobContext.SaveMigrationJob(MigrationJobContext.CurrentlyActiveJob);
            }
        }

        private void KillAllActiveProcesses()
        {
            lock (_pidLock)
            {
                int totalProcesses = MigrationJobContext.ActiveDumpProcessIds.Count + MigrationJobContext.ActiveRestoreProcessIds.Count;
                
                if (totalProcesses == 0)
                {
                    return;
                }
                
                _log.WriteLine($"Killing {totalProcesses} active processes ({MigrationJobContext.ActiveDumpProcessIds.Count} dump, {MigrationJobContext.ActiveRestoreProcessIds.Count} restore)");

                // Kill all dump processes
                foreach (var pid in MigrationJobContext.ActiveDumpProcessIds.ToList())
                {
                    try
                    {
                        var process = Process.GetProcessById(pid);
                        if (process != null && !process.HasExited)
                        {
                            process.Kill(entireProcessTree: true);
                            _log.WriteLine($"Killed dump process {pid}");
                        }
                    }
                    catch (Exception ex)
                    {
                        _log.WriteLine($"Failed to kill dump process {pid}: {ex.Message}", LogType.Debug);
                    }
                }
                MigrationJobContext.ActiveDumpProcessIds.Clear();

                // Kill all restore processes
                foreach (var pid in MigrationJobContext.ActiveRestoreProcessIds.ToList())
                {
                    try
                    {
                        var process = Process.GetProcessById(pid);
                        if (process != null && !process.HasExited)
                        {
                            process.Kill(entireProcessTree: true);
                            _log.WriteLine($"Killed restore process {pid}");
                        }
                    }
                    catch (Exception ex)
                    {
                        _log.WriteLine($"Failed to kill restore process {pid}: {ex.Message}", LogType.Debug);
                    }
                }
                MigrationJobContext.ActiveRestoreProcessIds.Clear();

                //_jobList.Save();
                MigrationJobContext.SaveMigrationJob(MigrationJobContext.CurrentlyActiveJob);
                _log.WriteLine("All active processes terminated");
            }
        }

        #endregion

        #region Parallel Dump Infrastructure

        private async Task<TaskResult> ParallelDumpChunksAsync(
            MigrationUnit mu, 
            IMongoCollection<BsonDocument> collection,
            string folder, 
            string sourceConnectionString, 
            string targetConnectionString,
            string dbName, 
            string colName)
        {
            _cts.Token.ThrowIfCancellationRequested();
            
            // Build work queue (ordered by chunk index)
            var sortedChunks = mu.MigrationChunks
                .Select((chunk, index) => new ChunkWorkItem
                {
                    ChunkIndex = index,
                    MigrationUnit = mu,
                    Chunk = chunk,
                    QueuedAt = DateTime.UtcNow
                })
                .Where(item => item.Chunk.IsDownloaded != true)
                .OrderBy(item => item.ChunkIndex)
                .ToList();
            
            if (!sortedChunks.Any())
            {
                _log.WriteLine($"All chunks already downloaded for {dbName}.{colName}");
                return TaskResult.Success;
            }
            
            _log.WriteLine($"Starting parallel dump of {sortedChunks.Count} chunks with {_dumpPool!.MaxWorkers} workers");
            
            // Store context for dynamic worker spawning
            _currentCollection = collection;
            _currentFolder = folder;
            _currentMigrationUnit = mu;
            _currentSourceConnectionString = sourceConnectionString;
            _currentTargetConnectionString = targetConnectionString;
            _currentDbName = dbName;
            _currentColName = colName;
            _nextDumpWorkerId = 1;
            
            // Enqueue all chunks
            foreach (var chunk in sortedChunks)
            {
                _dumpQueue.Enqueue(chunk);
            }
            
            // Start worker tasks (make accessible for dynamic spawning)
            lock (_workerTaskLock)
            {
                _activeDumpWorkerTasks = new();
            }
            
            List<Task<TaskResult>> workerTasks = _activeDumpWorkerTasks;
            string collectionKey = $"{dbName}.{colName}";
            
            for (int i = 0; i < Math.Min(_dumpPool!.MaxWorkers, sortedChunks.Count); i++)
            {
                int workerId = _nextDumpWorkerId++;
                
                // Register worker with coordinator
                WorkerPoolCoordinator.RegisterDumpWorker(_jobId, collectionKey, workerId, _log);
                
                var workerTask = Task.Run(async () =>
                {
                    try
                    {
                        return await DumpWorkerAsync(
                            workerId, 
                            collection, 
                            folder, 
                            sourceConnectionString, 
                            targetConnectionString,
                            dbName, 
                            colName
                        );
                    }
                    finally
                    {
                        // Mark worker as completed in coordinator
                        WorkerPoolCoordinator.MarkDumpWorkerCompleted(_jobId, collectionKey, workerId, _log);
                    }
                }, _cts.Token);
                
                workerTasks.Add(workerTask);
            }
            
            // Wait for all workers to complete
            var results = await Task.WhenAll(workerTasks);
            
            // Clear context
            _currentCollection = null;
            _currentSourceConnectionString = null;
            _currentTargetConnectionString = null;
            _currentDbName = null;
            _currentColName = null;
            lock (_workerTaskLock)
            {
                _activeDumpWorkerTasks = null;
            }
            
            // Check for controlled pause - workers have finished, now stop
            if (MigrationJobContext.ControlledPauseRequested)
            {
                _log.WriteLine("Controlled pause - dump workers completed, stopping processor", LogType.Debug);
                StopProcessing(updateStatus: true);
                return TaskResult.Success;
            }
            
            // Check if any worker failed
            if (results.Any(r => r == TaskResult.FailedAfterRetries || r == TaskResult.Abort))
            {
                _log.WriteLine($"Dump failed for {dbName}.{colName}", LogType.Error);
                return TaskResult.FailedAfterRetries;
            }
            
            if (results.Any(r => r == TaskResult.Canceled))
            {
                return TaskResult.Canceled;
            }
            
            // Report errors if any
            if (_chunkErrors.Any())
            {
                _log.WriteLine($"Dump completed with {_chunkErrors.Count} chunk errors:", LogType.Warning);
                foreach (var (index, error) in _chunkErrors.Take(5))
                {
                    _log.WriteLine($"  Chunk {index}: {Helper.RedactPii(error.Message)}", LogType.Error);
                }
            }
            
            return TaskResult.Success;
        }

        private async Task<TaskResult> DumpWorkerAsync(
            int workerId,
            IMongoCollection<BsonDocument> collection,
            string folder,
            string sourceConnectionString,
            string targetConnectionString,
            string dbName,
            string colName)
        {
            _log.WriteLine($"Dump worker {workerId} started", LogType.Debug);
            
            while (true)
            {
                // Check for controlled pause first
                if (MigrationJobContext.ControlledPauseRequested)
                {
                    _log.WriteLine($"Dump worker {workerId}: Controlled pause active - exiting", LogType.Debug);
                    break;
                }
                
                if (_cts.Token.IsCancellationRequested)
                {
                    _log.WriteLine($"Dump worker {workerId} cancelled");
                    return TaskResult.Canceled;
                }
                
                ChunkWorkItem? workItem = null;
                bool semaphoreAcquired = false;
                try
                {
                    // Acquire semaphore slot first
                    await _dumpPool!.WaitAsync(_cts.Token);
                    semaphoreAcquired = true;
                    
                    // Now try to dequeue work
                    if (!_dumpQueue.TryDequeue(out workItem))
                    {
                        // No more work, exit
                        break;
                    }
                    
                    // Check again after dequeue in case pause was requested
                    if (MigrationJobContext.ControlledPauseRequested)
                    {
                    _log.WriteLine($"Dump worker {workerId}: Controlled pause active - returning at chunk {workItem.ChunkIndex}", LogType.Debug);
                    break;
                }

                    _log.WriteLine($"Worker {workerId} dumping chunk {workItem.ChunkIndex}");
                    
                    double initialPercent = ((double)100 / workItem.MigrationUnit.MigrationChunks.Count) * workItem.ChunkIndex;
                    double contributionFactor = 1.0 / workItem.MigrationUnit.MigrationChunks.Count;
                    
                    TaskResult result = await new RetryHelper().ExecuteTask(
                        () => DumpChunkAsync(
                            workItem.MigrationUnit, 
                            workItem.ChunkIndex, 
                            collection, 
                            folder,
                            sourceConnectionString, 
                            targetConnectionString, 
                            initialPercent, 
                            contributionFactor,
                            dbName, 
                            colName
                        ),
                        (ex, attemptCount, currentBackoff) => DumpChunk_ExceptionHandler(
                            ex, attemptCount, "DumpChunk", dbName, colName, workItem.ChunkIndex, currentBackoff
                        ),
                        _log
                    );
                    
                    if (result == TaskResult.FailedAfterRetries || result == TaskResult.Abort)
                    {
                        _log.WriteLine($"Worker {workerId} failed on chunk {workItem.ChunkIndex}", LogType.Error);
                        
                        // Kill all processes on failure
                        KillAllActiveProcesses();
                        return TaskResult.FailedAfterRetries;
                    }
                    
                    if (result == TaskResult.Canceled)
                    {
                        return TaskResult.Canceled;
                    }
                }
                catch (OperationCanceledException)
                {
                    _log.WriteLine($"Dump worker {workerId} cancelled");
                    return TaskResult.Canceled;
                }
                catch (Exception ex)
                {
                    _log.WriteLine($"Dump worker {workerId} error: {Helper.RedactPii(ex.Message)}", LogType.Error);
                    if (workItem != null)
                        _chunkErrors.Add((workItem.ChunkIndex, ex));
                    
                    // Continue processing other chunks unless critical error
                    if (ex is OutOfMemoryException || ex is StackOverflowException)
                    {
                        KillAllActiveProcesses();
                        return TaskResult.Abort;
                    }
                }
                finally
                {
                    // Always release semaphore if it was acquired
                    if (semaphoreAcquired)
                    {
                        if (!_dumpPool!.TryRelease())
                        {
                            _log.WriteLine($"Dump worker {workerId} semaphore overflow on release.", LogType.Debug);
                        }
                        semaphoreAcquired = false; // Reset flag after release
                    }
                }
            }
            
            _log.WriteLine($"Dump worker {workerId} completed", LogType.Debug);
            
            // Check if controlled pause completed
            if (MigrationJobContext.ControlledPauseRequested && _dumpQueue.IsEmpty)
            {
                _log.WriteLine("All dump chunks completed during controlled pause");
            }
            
            return TaskResult.Success;
        }

        #endregion

        #region Parallel Restore Infrastructure

        private async Task<RestoreResult> ParallelRestoreChunksAsync(
            MigrationUnit mu,
            string folder,
            string targetConnectionString,
            string dbName,
            string colName)
        {
            int restoredChunks = 0;
            long restoredDocs = 0;


            try

            {
                _cts.Token.ThrowIfCancellationRequested();

                // Build work queue (ordered by chunk index)
                var sortedChunks = BuildRestoreWorkQueue(mu);

                // Check if all chunks are already uploaded (not just that there are no pending chunks)
                bool allChunksUploaded = mu.MigrationChunks.All(c => c.IsUploaded == true);

                // Validate dump completion integrity
                var validationResult = ValidateDumpCompletion(mu);
                if (validationResult != null)
                {
                    return validationResult;
                }

                // Handle already completed restore scenario
                if (!sortedChunks.Any() && allChunksUploaded)
                {
                    return HandleCompletedRestore(mu, dbName, colName, ref restoredChunks, ref restoredDocs);
                }
                
                // If no chunks are ready for restore yet (still being downloaded), return early
                if (!sortedChunks.Any())
                {
                    _log.WriteLine($"No chunks ready for restore yet for {dbName}.{colName} - waiting for dump to complete", LogType.Debug);
                    return new RestoreResult
                    {
                        Result = TaskResult.Success,
                        RestoredChunks = 0,
                        RestoredDocs = 0
                    };
                }

                _log.WriteLine($"Starting parallel restore of {sortedChunks.Count} chunks with {_restorePool!.MaxWorkers} workers");

                // Store context for dynamic worker spawning
                _currentFolder = folder;
                _currentMigrationUnit = mu;
                _currentTargetConnectionString = targetConnectionString;
                _currentDbName = dbName;
                _currentColName = colName;
                _nextRestoreWorkerId = 1;

                // Clear chunk errors before starting
                _chunkErrors.Clear();

                // Enqueue restore work to global queue
                foreach (var chunk in sortedChunks)
                {
                    _restoreQueue.Enqueue(chunk);
                }

                // Start worker tasks (make accessible for dynamic spawning)
                lock (_workerTaskLock)
                {
                    _activeRestoreWorkerTasks = new();
                }
                
                List<Task<TaskResult>> workerTasks = _activeRestoreWorkerTasks;
                string collectionKey = $"{dbName}.{colName}";
                
                for (int i = 0; i < Math.Min(_restorePool!.MaxWorkers, sortedChunks.Count); i++)
                {
                    int workerId = _nextRestoreWorkerId++;
                    
                    // Register worker with coordinator
                    WorkerPoolCoordinator.RegisterRestoreWorker(_jobId, collectionKey, workerId, _log);
                    
                    var workerTask = Task.Run(async () =>
                    {
                        try
                        {
                            return await RestoreWorkerAsync(
                                workerId,
                                _restoreQueue,
                                folder,
                                targetConnectionString,
                                dbName,
                                colName
                            );
                        }
                        finally
                        {
                            // Mark worker as completed in coordinator
                            WorkerPoolCoordinator.MarkRestoreWorkerCompleted(_jobId, collectionKey, workerId, _log);
                        }
                    }, _cts.Token);

                    workerTasks.Add(workerTask);
                }

                // Wait for all workers to complete
                var results = await Task.WhenAll(workerTasks);

                // Clear context
                _currentFolder = null;
                _currentTargetConnectionString = null;
                _currentDbName = null;
                _currentColName = null;
                lock (_workerTaskLock)
                {
                    _activeRestoreWorkerTasks = null;
                }

                // Check for controlled pause - workers have finished, now stop
                if (MigrationJobContext.ControlledPauseRequested)
                {
                    _log.WriteLine("Controlled pause - restore workers completed, stopping processor");
                    StopProcessing(updateStatus: true);
                    return new RestoreResult
                    {
                        Result = TaskResult.Success,
                        RestoredChunks = restoredChunks,
                        RestoredDocs = restoredDocs
                    };
                }

                // Check if any worker failed
                if (results.Any(r => r == TaskResult.FailedAfterRetries || r == TaskResult.Abort))
                {
                    _log.WriteLine($"Restore failed for {dbName}.{colName}", LogType.Error);
                    return new RestoreResult
                    {
                        Result = TaskResult.FailedAfterRetries,
                        RestoredChunks = restoredChunks,
                        RestoredDocs = restoredDocs
                    };
                }

                if (results.Any(r => r == TaskResult.Canceled))
                {
                    return new RestoreResult
                    {
                        Result = TaskResult.Canceled,
                        RestoredChunks = restoredChunks,
                        RestoredDocs = restoredDocs
                    };
                }

                // Calculate restored counts
                foreach (var chunk in mu.MigrationChunks)
                {
                    if (chunk.IsUploaded == true)
                    {
                        restoredChunks++;
                        restoredDocs += Math.Max(chunk.RestoredSuccessDocCount, chunk.DocCountInTarget);
                    }
                }

                // Report errors if any
                if (_chunkErrors.Any())
                {
                    _log.WriteLine($"Restore completed with {_chunkErrors.Count} chunk errors:", LogType.Warning);
                    foreach (var (index, error) in _chunkErrors.Take(5))
                    {
                        _log.WriteLine($"  Chunk {index}: {Helper.RedactPii(error.Message)}", LogType.Error);
                    }
                }

                return new RestoreResult
                {
                    Result = TaskResult.Success,
                    RestoredChunks = restoredChunks,
                    RestoredDocs = restoredDocs
                };
            }
            catch (Exception ex)
            {
                _log.WriteLine($"Exception in Parallel Restore chunks for {{mu.DatabaseName}}.{{mu.CollectionName}}, Details: {ex}", LogType.Error);
                return new RestoreResult
                {
                    Result = TaskResult.Retry,
                    RestoredChunks = 0,
                    RestoredDocs = 0
                };                
            }
        }

        private async Task<TaskResult> RestoreWorkerAsync(
            int workerId,
            ConcurrentQueue<ChunkWorkItem> restoreQueue,
            string folder,
            string targetConnectionString,
            string dbName,
            string colName)
        {
            _log.WriteLine($"Restore worker {workerId} started", LogType.Debug);
            
            while (true)
            {
                // Check for controlled pause first
                if (MigrationJobContext.ControlledPauseRequested)
                {
                    _log.WriteLine($"Restore worker {workerId}: Controlled pause active - exiting");
                    break;
                }
                
                if (_cts.Token.IsCancellationRequested)
                {
                    _log.WriteLine($"Restore worker {workerId} cancelled");
                    return TaskResult.Canceled;
                }
                
                ChunkWorkItem? workItem = null;
                bool semaphoreAcquired = false;
                try
                {
                    // Acquire semaphore slot first
                    await _restorePool!.WaitAsync(_cts.Token);
                    semaphoreAcquired = true;
                    
                    // Now try to dequeue work
                    if (!restoreQueue.TryDequeue(out workItem))
                    {
                        // No more work, exit
                        break;
                    }
                    
                    // Check again after dequeue in case pause was requested
                    if (MigrationJobContext.ControlledPauseRequested)
                    {
                        // Put the work item back for later
                        //restoreQueue.Enqueue(workItem);
                        _log.WriteLine($"Restore worker {workerId}: Controlled pause active - returning chunk {workItem.ChunkIndex}", LogType.Debug);
                        break;
                    }
                    
                    _log.WriteLine($"Worker {workerId} restoring chunk {workItem.ChunkIndex}");
                    
                    double initialPercent = ((double)100 / workItem.MigrationUnit.MigrationChunks.Count) * workItem.ChunkIndex;
                    double contributionFactor = (double)workItem.Chunk.DumpQueryDocCount / Helper.GetMigrationUnitDocCount(workItem.MigrationUnit);
                    if (workItem.MigrationUnit.MigrationChunks.Count == 1) contributionFactor = 1;
                    
                    TaskResult result = await new RetryHelper().ExecuteTask(
                        () => RestoreChunkAsync(
                            workItem.MigrationUnit,
                            workItem.ChunkIndex,
                            folder,
                            targetConnectionString,
                            initialPercent,
                            contributionFactor,
                            dbName,
                            colName
                        ),
                        (ex, attemptCount, currentBackoff) => RestoreChunk_ExceptionHandler(
                            ex, attemptCount, "RestoreChunk", dbName, colName, workItem.ChunkIndex, currentBackoff
                        ),
                        _log
                    );
                    
                    if (result == TaskResult.FailedAfterRetries || result == TaskResult.Abort)
                    {
                        _log.WriteLine($"Worker {workerId} failed on chunk {workItem.ChunkIndex}", LogType.Error);
                        
                        // Kill all processes on failure
                        KillAllActiveProcesses();
                        return TaskResult.FailedAfterRetries;
                    }
                    
                    if (result == TaskResult.Canceled)
                    {
                        return TaskResult.Canceled;
                    }
                }
                catch (OperationCanceledException)
                {
                    _log.WriteLine($"Restore worker {workerId} cancelled");
                    return TaskResult.Canceled;
                }
                catch (Exception ex)
                {
                    _log.WriteLine($"Restore worker {workerId} error: {Helper.RedactPii(ex.Message)}", LogType.Error);
                    if (workItem != null)
                        _chunkErrors.Add((workItem.ChunkIndex, ex));
                    
                    // Continue processing other chunks unless critical error
                    if (ex is OutOfMemoryException || ex is StackOverflowException)
                    {
                        KillAllActiveProcesses();
                        return TaskResult.Abort;
                    }
                }
                finally
                {
                    // Always release semaphore if it was acquired
                    if (semaphoreAcquired)
                    {
                        if (!_restorePool!.TryRelease())
                        {
                            _log.WriteLine($"Restore worker {workerId} semaphore overflow on release.", LogType.Debug);
                        }
                        semaphoreAcquired = false; // Reset flag after release
                    }
                }
            }
            
            _log.WriteLine($"Restore worker {workerId} completed");
            
            // Check if controlled pause completed
            if (MigrationJobContext.ControlledPauseRequested && restoreQueue.IsEmpty)
            {
                _log.WriteLine("All restore chunks completed during controlled pause");
            }
            
            return TaskResult.Success;
        }

        #endregion

        // Custom exception handler delegate with logic to control retry flow (parity with CopyProcessor)
        private Task<TaskResult> DumpChunk_ExceptionHandler(Exception ex, int attemptCount, string processName, string dbName, string colName, int chunkIndex, int currentBackoff)
        {
            if (ex is OperationCanceledException)
            {
                _log.WriteLine($"Dump operation was paused for {dbName}.{colName}[{chunkIndex}]");
                return Task.FromResult(TaskResult.Canceled);
            }
            else if (ex is MongoExecutionTimeoutException)
            {
                _log.WriteLine($" {processName} attempt {attemptCount} failed due to timeout. Details:{Helper.RedactPii(ex.ToString())}.  Retrying in {currentBackoff} seconds...", LogType.Error);
                return Task.FromResult(TaskResult.Retry);
            }
            else
            {
                _log.WriteLine($"{processName} attempt {attemptCount} for {dbName}.{colName} failed. Error details:{Helper.RedactPii(ex.ToString())}. Retrying in {currentBackoff} seconds...", LogType.Error);
                return Task.FromResult(TaskResult.Retry);
            }
        }

        private Task<TaskResult> DumpChunkAsync(MigrationUnit mu, int chunkIndex, IMongoCollection<BsonDocument> collection,
            string folder, string sourceConnectionString, string targetConnectionString,
            double initialPercent, double contributionFactor, string dbName, string colName)
        {
            _cts.Token.ThrowIfCancellationRequested();

            // Disk space/backpressure check
            if (!CheckDiskSpaceForDump(mu, folder, targetConnectionString, dbName, colName))
            {
                return Task.FromResult(TaskResult.Canceled);
            }

            // Build base args per attempt
            string args = BuildDumpArgs(sourceConnectionString, dbName, colName);

            long docCount = BuildDumpQuery(mu, chunkIndex, collection, dbName, colName, ref args);
            var dumpFilePath = PrepareDumpFile(folder, chunkIndex);

            try
            {
                bool result = ExecuteDumpProcess(mu, chunkIndex, args, dumpFilePath, docCount, initialPercent, contributionFactor);

                if (result)
                {
                    return HandleDumpChunkSuccess(mu, chunkIndex, targetConnectionString, dbName, colName);
                }
                else
                {
                    return Task.FromResult(TaskResult.Retry);
                }
            }
            catch (OperationCanceledException)
            {
                return Task.FromResult(TaskResult.Canceled);
            }
        }

        // Custom exception handler delegate for restore path (parity with CopyProcessor)
        private Task<TaskResult> RestoreChunk_ExceptionHandler(Exception ex, int attemptCount, string processName, string dbName, string colName, int chunkIndex, int currentBackoff)
        {
            if (ex is OperationCanceledException)
            {
                _log.WriteLine($"Restore operation was paused for {dbName}.{colName}[{chunkIndex}]");
                return Task.FromResult(TaskResult.Canceled);
            }
            else if (ex is MongoExecutionTimeoutException)
            {
                _log.WriteLine($" {processName} attempt {attemptCount} failed due to timeout. Details:{Helper.RedactPii(ex.ToString())}", LogType.Error);
                return Task.FromResult(TaskResult.Retry);
            }
            else
            {
                _log.WriteLine($"{processName} attempt {attemptCount} for {dbName}.{colName} failed. Error details:{Helper.RedactPii(ex.ToString())}. Retrying in {currentBackoff} seconds...", LogType.Error);
                return Task.FromResult(TaskResult.Retry);
            }
        }

        // 2. In RestoreChunkAsync, _targetClient can be null. Add null check before using _targetClient.
        private Task<TaskResult> RestoreChunkAsync(MigrationUnit mu, int chunkIndex,
            string folder, string targetConnectionString,
            double initialPercent, double contributionFactor,
            string dbName, string colName)
        {           

            _cts.Token.ThrowIfCancellationRequested();

            // Check and handle simulation mode if enabled
            var simulationResult = SimulateRestoreChunk(mu, chunkIndex, initialPercent, contributionFactor);
            if (simulationResult != null)
            {
                return simulationResult;
            }

            // Build args per attempt
            string args = BuildRestoreArgs(targetConnectionString, mu, chunkIndex, dbName, colName);

            // If first mu, drop collection, else append. Also No drop in AppendMode
            if (chunkIndex == 0 && !MigrationJobContext.CurrentlyActiveJob.AppendMode)
            {
                args = $"{args} --drop";
                if (MigrationJobContext.CurrentlyActiveJob.SkipIndexes)
                {
                    args = $"{args} --noIndexRestore"; // No index to create for all chunks.
                }
            }
            else
            {
                args = $"{args} --noIndexRestore"; // No index to create. Index restore only for 1st chunk.
            }            

            long docCount = (mu.MigrationChunks.Count > 1)
                ? mu.MigrationChunks[chunkIndex].DumpQueryDocCount
                : Helper.GetMigrationUnitDocCount(mu);

            // Determine insertion workers and add to args if needed
            args = ConfigureInsertionWorkers(args, dbName, colName, chunkIndex, docCount);

            try
            {
                var dumpFilePath = $"{Path.Combine(folder, $"{chunkIndex}.bson")}";

                // Validate dump file exists before restore
                var validationResult = ValidateDumpFileExists(mu, chunkIndex, dumpFilePath);
                if (validationResult != null)
                {
                    return validationResult;
                }

                bool result = ExecuteRestoreProcess(mu, chunkIndex, args, dumpFilePath, docCount, initialPercent, contributionFactor);                             

                if (result)
                {
                    return HandleRestoreChunkSuccess(mu, chunkIndex, folder, targetConnectionString, dbName, colName, docCount);
                }
                else
                {
                    if (mu.MigrationChunks[chunkIndex].IsUploaded == true)
                    {
                        // Already uploaded, treat as success
                        MigrationJobContext.SaveMigrationUnit(mu,false);
                        return Task.FromResult(TaskResult.Success);
                    }

                    return Task.FromResult(TaskResult.Retry);
                }
            }
            catch (OperationCanceledException)
            {
                return Task.FromResult(TaskResult.Canceled);
            }
        }

        public override async Task<TaskResult> StartProcessAsync(string migrationUnitId, string sourceConnectionString, string targetConnectionString, string idField = "_id")
        {
            var initResult = InitializeStartProcess(migrationUnitId, sourceConnectionString, targetConnectionString);
            if (initResult.Result != TaskResult.Success)
            {
                return initResult.Result;
            }
            
            var mu = initResult.MigrationUnit!;
            var ctx = initResult.Context!;
            string jobId = ctx.JobId;
            string dbName = ctx.DatabaseName;
            string colName = ctx.CollectionName;

            
            // when resuming a CurrentlyActiveJob, check if post-upload change stream processing is already in progress
            if (CheckChangeStreamAlreadyProcessingAsync(ctx))
                return TaskResult.Success;


            //invoke upload for chunks that are already dumped
            _ = Task.Run(() => Upload(mu.Id, ctx.TargetConnectionString), _cts.Token);


            // Validate and prepare dump process
            PrepareDumpProcess(mu, jobId, dbName, colName);

            // check if  dump complete and process chunks as needed
            if (!mu.DumpComplete && !_cts.Token.IsCancellationRequested)
            {
                string folder = InitializeDumpFolder(mu, ctx, jobId, dbName, colName);

                long downloadCount = 0;

                // Always use parallel dump infrastructure (supports dynamic worker scaling)
                _log.WriteLine($"Starting dump with {_dumpPool!.MaxWorkers} worker(s)");
                
                TaskResult result = await ParallelDumpChunksAsync(
                    mu, 
                    ctx.Collection, 
                    folder, 
                    ctx.SourceConnectionString, 
                    ctx.TargetConnectionString,
                    dbName, 
                    colName
                );
                
                if (result == TaskResult.Abort || result == TaskResult.FailedAfterRetries)
                {
                    _log.WriteLine($"Dump operation for {dbName}.{colName} failed after multiple attempts.", LogType.Error);
                    StopProcessing();
                    return result;
                }
                
                if (result == TaskResult.Canceled)
                {
                    return result;
                }

                if (!_cts.Token.IsCancellationRequested)
                {
                    FinalizeDumpCompletion(mu, ref downloadCount);

                    // Only trigger restore if not paused
                    if (!MigrationJobContext.ControlledPauseRequested)
                    {
                        _log.WriteLine($"{dbName}.{colName} dump complete, adding  to restore queue.");
  
                        _migrationUnitsPendingUpload.Add(mu.Id);
                        _ = Task.Run(() => Upload(mu.Id, ctx.TargetConnectionString), _cts.Token);
                    }
                    
                }
            }
            else if (mu.DumpComplete && !mu.RestoreComplete && !_cts.Token.IsCancellationRequested)
            {
                _log.WriteLine($"{dbName}.{colName} added to upload queue.");

                //MigrationUnitsPendingUpload.AddOrUpdate($"{mu.DatabaseName}.{mu.CollectionName}", mu);
                _migrationUnitsPendingUpload.Add(mu.Id);
                _ = Task.Run(() => Upload(mu.Id, ctx.TargetConnectionString), _cts.Token);
            }            

            return TaskResult.Success;
        }


        private void Upload(string migrationUnitId, string targetConnectionString, bool force = false)
        {
           
            if (!force)
            {
                if (!TryEnterUploadLock())
                {
                    return; // Prevent concurrent uploads
                }
                _uploadLockAcquired = true; // Mark that we acquired the lock
            }

            // Don't restart processing if controlled pause was requested
            if (MigrationJobContext.ControlledPauseRequested)
            {
                _log.WriteLine($"Upload skipped for {migrationUnitId} - controlled pause is active",LogType.Debug);
                if (_uploadLockAcquired)
                {
                    try { _uploadLock.Release(); _uploadLockAcquired = false; } catch { }
                }
                return;
            }

            ProcessRunning=true;
            var mu = MigrationJobContext.MigrationUnitsCache.GetMigrationUnit(migrationUnitId);
            mu.ParentJob = MigrationJobContext.CurrentlyActiveJob;
            string dbName = mu.DatabaseName;
            string colName = mu.CollectionName;
            string jobId = MigrationJobContext.CurrentlyActiveJob.Id ?? string.Empty;
            string key = $"{mu.DatabaseName}.{mu.CollectionName}";
            string folder = GetDumpFolder(jobId, dbName, colName);

            _log.WriteLine($"Processing {key} from upload queue");
 
            try
            {
                ProcessRestoreLoop(mu, folder, targetConnectionString, dbName, colName);

                _log.WriteLine($"ProcessRestoreLoop exited for {key}", LogType.Verbose);

                if ((mu.RestoreComplete && mu.DumpComplete) || (mu.DumpComplete && MigrationJobContext.CurrentlyActiveJob.IsSimulatedRun))
                {
                    _log.WriteLine($"FinalizeUpload invoked for {key}", LogType.Verbose);

                    FinalizeUpload(mu.Id, key, folder, targetConnectionString, jobId);
                }
            }
            finally
            {
                _log.WriteLine($"Release Upload lock for {key}", LogType.Verbose);
                // Always release the upload lock if we acquired it
                if (_uploadLockAcquired)
                {
                    try { _uploadLock.Release(); _uploadLockAcquired = false; } catch { }
                }
            }
        }


        // Builds the dump folder path for a db/collection under the current CurrentlyActiveJob
        private string GetDumpFolder(string jobId, string dbName, string colName)
            => Path.Combine(_mongoDumpOutputFolder, jobId, Helper.SafeFileName($"{dbName}.{colName}"));

        // Core restore loop: iterates until all chunks are restored or cancellation/simulation stops it
        private void ProcessRestoreLoop(MigrationUnit mu, string folder, string targetConnectionString, string dbName, string colName)
        {
            // Handle simulation mode
            if (ProcessSimulationModeRestore(mu, folder, targetConnectionString, dbName, colName))
            {
                return; // Simulation completed
            }

            // Process regular restore loop
            while (ShouldContinueUploadLoop(mu, folder))
            {
                if (CheckAndHandleControlledPause(dbName, colName))
                {
                    return;
                }
                
                if (!mu.RestoreComplete && !_cts.Token.IsCancellationRequested && Helper.IsMigrationUnitValid(mu))
                {
                    if (!ProcessRestoreIteration(mu, folder, targetConnectionString, dbName, colName))
                    {
                        return; // Exit if iteration indicated completion or error
                    }
                }
            }
        }

        private bool ProcessSimulationModeRestore(MigrationUnit mu, string folder, string targetConnectionString, string dbName, string colName)
        {
            if (!MigrationJobContext.CurrentlyActiveJob.IsSimulatedRun || mu.RestoreComplete || !mu.DumpComplete)
            {
                return false; // Not simulation mode or already complete
            }
            
            _log.WriteLine($"Simulation mode: Simulating parallel restore for {dbName}.{colName} with {_restorePool!.MaxWorkers} workers");
            
            // Calculate initial progress based on already restored chunks
            int alreadyRestored = mu.MigrationChunks.Count(c => c.IsUploaded == true);
            if (alreadyRestored > 0)
            {
                mu.RestorePercent = ((double)alreadyRestored / mu.MigrationChunks.Count) * 100;
                _log.WriteLine($"Simulation mode: Starting from {alreadyRestored}/{mu.MigrationChunks.Count} chunks already restored ({mu.RestorePercent:F2}%)");
            }
            else
            {
                mu.RestorePercent = 0;
            }
            
            // Use the parallel restore infrastructure even in simulation mode
            var result = ParallelRestoreChunksAsync(
                mu,
                folder,
                targetConnectionString,
                dbName,
                colName
            ).GetAwaiter().GetResult();
            
            // Mark as complete
            mu.RestorePercent = 100;
            mu.RestoreComplete = true;
            mu.RestoreGap = 0; // Assume perfect simulation

            _log.WriteLine($"Simulation mode: Restore completed for {dbName}.{colName} - {result.RestoredChunks} chunks, {result.RestoredDocs} docs");
            
            if (mu.DumpComplete && mu.RestoreComplete)
            {
                if (!mu.BulkCopyEndedOn.HasValue || mu.BulkCopyEndedOn.Value == DateTime.MinValue)
                {
                    mu.BulkCopyEndedOn = DateTime.UtcNow;
                }
            }

            MigrationJobContext.SaveMigrationUnit(mu, true);
            MigrationJobContext.MigrationUnitsCache.RemoveMigrationUnit(mu.Id);
            _log.WriteLine($"Simulation mode: Restore completed for {dbName}.{colName} - Final RestorePercent={mu.RestorePercent}%");
            
            return true; // Simulation handled
        }

        private bool CheckAndHandleControlledPause(string dbName, string colName)
        {
            if (MigrationJobContext.ControlledPauseRequested)
            {
                _log.WriteLine($"Controlled pause detected in restore loop for {dbName}.{colName} - exiting", LogType.Debug);
                return true;
            }
            return false;
        }

        private bool ProcessRestoreIteration(MigrationUnit mu, string folder, string targetConnectionString, string dbName, string colName)
        {
            int restoredChunks;
            long restoredDocs;
            RestoreAllPendingChunksOnce(mu, folder, targetConnectionString, dbName, colName, out restoredChunks, out restoredDocs);

            if (restoredChunks == mu.MigrationChunks.Count && !_cts.Token.IsCancellationRequested)
            {
                FinalizeRestoreCompletion(mu, restoredDocs);
                return false; // Indicate completion
            }
            else
            {
                return HandleIncompleteRestore(mu, dbName, colName);
            }
        }

        private void FinalizeRestoreCompletion(MigrationUnit mu, long restoredDocs)
        {
            mu.RestoreGap = Helper.GetMigrationUnitDocCount(mu) - restoredDocs;
            mu.RestorePercent = 100;
            mu.RestoreComplete = true;
            
            // Set BulkCopyEndedOn only after both dump and restore are complete
            if (mu.DumpComplete && mu.RestoreComplete)
            {
                if (!mu.BulkCopyEndedOn.HasValue || mu.BulkCopyEndedOn.Value == DateTime.MinValue)
                {
                    mu.BulkCopyEndedOn = DateTime.UtcNow;
                }
            }

            MigrationJobContext.SaveMigrationUnit(mu, true);
            MigrationJobContext.MigrationUnitsCache.RemoveMigrationUnit(mu.Id);
        }

        private bool HandleIncompleteRestore(MigrationUnit mu, string dbName, string colName)
        {
            // If there are no pending chunks to restore, exit the loop instead of sleeping
            if (!HasPendingChunks(mu))
            {
                return false; // Exit iteration
            }

            try
            {
                _log.WriteLine($"Restore loop for {dbName}.{colName} sleeping for 10 seconds before next pass...");
                Task.Delay(10000, _cts.Token).Wait(_cts.Token);
                return true; // Continue iteration
            }
            catch (OperationCanceledException)
            {
                return false; // Exit if cancellation was requested during delay
            }
        }

        // Returns true if there is at least one chunk that has been downloaded but not yet uploaded
        private bool HasPendingChunks(MigrationUnit mu)
        {
            for (int i = 0; i < mu.MigrationChunks.Count; i++)
            {
                if (mu.MigrationChunks[i].IsDownloaded == true && mu.MigrationChunks[i].IsUploaded != true)
                {
                    return true;
                }
            }
            return false;
        }

        private bool ShouldContinueUploadLoop(MigrationUnit mu, string folder)
        { 
            _log.WriteLine($"Checking if restore should continue for {mu.DatabaseName}.{mu.CollectionName}: RestoreComplete={mu.RestoreComplete}, DumpFolderExists={Directory.Exists(folder)}, CancellationRequested={_cts.Token.IsCancellationRequested}, IsSimulatedRun={MigrationJobContext.CurrentlyActiveJob.IsSimulatedRun}", LogType.Verbose);
            return !mu.RestoreComplete && Directory.Exists(folder) && !_cts.Token.IsCancellationRequested && !MigrationJobContext.CurrentlyActiveJob.IsSimulatedRun;
        }

        // Performs a single pass over all chunks, restoring any downloaded-but-not-uploaded ones
        private void RestoreAllPendingChunksOnce(
            MigrationUnit mu,
            string folder,
            string targetConnectionString,
            string dbName,
            string colName,
            out int restoredChunks,
            out long restoredDocs)
        {
            restoredChunks = 0;
            restoredDocs = 0;

            // Always use parallel restore infrastructure (supports dynamic worker scaling)
            _log.WriteLine($"Starting restore with {_restorePool!.MaxWorkers} worker(s)");
            
            var result = ParallelRestoreChunksAsync(
                mu,
                folder,
                targetConnectionString,
                dbName,
                colName
            ).GetAwaiter().GetResult();
            
            restoredChunks = result.RestoredChunks;
            restoredDocs = result.RestoredDocs;

            _log.WriteLine($"Completed ParallelRestoreChunksAsync for {mu.DatabaseName}.{mu.CollectionName} with {_restorePool!.MaxWorkers} worker(s)", LogType.Verbose);

            if (result.Result == TaskResult.Abort || result.Result == TaskResult.FailedAfterRetries)
            {
                _log.WriteLine($"Restore operation for {dbName}.{colName} failed after multiple attempts.", LogType.Error);
                StopProcessing();
                return;
            }
        }

        // Finalization after restore completes or simulated run concludes
        private void FinalizeUpload(string migrationunitId, string key, string folder, string targetConnectionString, string jobId)
        {
            
            _log.WriteLine($"Folder cleanup for {key} started", LogType.Verbose);

            // Best-effort cleanup of local dump folder
            try
            {
                if (Directory.Exists(folder))
                    Directory.Delete(folder, true);
            }
            catch { }

            if (MigrationJobContext.ControlledPauseRequested)
                return;

            _log.WriteLine($"AddCollectionToChangeStreamQueue for {key} invoked", LogType.Verbose);

            // Start change stream immediately if configured
            AddCollectionToChangeStreamQueue(migrationunitId, targetConnectionString);

            _log.WriteLine($"Removing {key} from _migrationUnitsPendingUpload queue", LogType.Verbose);

            // Remove from upload queue
            _migrationUnitsPendingUpload.Remove(migrationunitId);

            // Process next pending upload if any
            if (_migrationUnitsPendingUpload.Count>0)
            {
                if (MigrationJobContext.ControlledPauseRequested)
                    return;

                _log.WriteLine($"Upload invoked for {_migrationUnitsPendingUpload[0]}.", LogType.Verbose);

                Upload(_migrationUnitsPendingUpload[0], targetConnectionString, true);
                return;
            }

            _log.WriteLine($"No uploads are pending.", LogType.Verbose);

            // Handle offline completion and post-upload CS logic
            if (!_cts.Token.IsCancellationRequested)
            {                
                if (!Helper.IsOnline(MigrationJobContext.CurrentlyActiveJob) && Helper.IsOfflineJobCompleted(MigrationJobContext.CurrentlyActiveJob))
                {
                    // Don't mark as completed if this is a controlled pause
                    if (!MigrationJobContext.ControlledPauseRequested)
                    {
                        _log.WriteLine($"Job {MigrationJobContext.CurrentlyActiveJob.Id} Completed");
                        MigrationJobContext.CurrentlyActiveJob.IsCompleted = true;
                        MigrationJobContext.SaveMigrationJob(MigrationJobContext.CurrentlyActiveJob);
                    }                        
                        
                    StopProcessing();
                }
                else
                {
                    _log.WriteLine($"Invoke RunChangeStreamProcessorForAllCollections.", LogType.Verbose);

                    RunChangeStreamProcessorForAllCollections(targetConnectionString);
                }                
            }
        }
        
        /// <summary>
        /// Override to handle controlled pause for DumpRestoreProcessor
        /// </summary>
        public override void InitiateControlledPause()
        {
            base.InitiateControlledPause();
            _log.WriteLine("DumpRestoreProcessor: Controlled pause - no new chunks will be queued");
            // Don't cancel _cts - let workers complete naturally
        }
        
        public new void StopProcessing(bool updateStatus = true)
        {
            _log.WriteLine("Stopping DumpRestoreProcessor...");
            
            // For controlled pause, don't kill processes - they should have completed naturally
            // Only kill if not a controlled pause (i.e., hard cancellation)
            if (!MigrationJobContext.ControlledPauseRequested)
            {
                _log.WriteLine("Hard stop - killing active processes");
                KillAllActiveProcesses();
            }            
            
            // Dispose worker pools (handles semaphores and blocker tasks)
            _dumpPool?.Dispose();
            _restorePool?.Dispose();
            
            // Clear queues
            while (_dumpQueue.TryDequeue(out _)) { }
            while (_restoreQueue.TryDequeue(out _)) { }
            
            // Clear error tracking
            while (_chunkErrors.TryTake(out _)) { }
            
            // Only try to release the upload lock if this instance acquired it
            if (_uploadLockAcquired)
            {
                try
                {
                    _uploadLock.Release();
                    _uploadLockAcquired = false;
                    _log.WriteLine("Released upload lock during stop");
                }
                catch (Exception ex)
                {
                    _log.WriteLine($"Failed to release upload lock: {ex.Message}", LogType.Debug);
                }
            }
            
            // Call base implementation
            base.StopProcessing(updateStatus);
            
            _log.WriteLine("DumpRestoreProcessor stopped");
        }

        #region Helper Methods for StartProcess

        private class StartProcessInitResult
        {
            public TaskResult Result { get; set; }
            public MigrationUnit? MigrationUnit { get; set; }
            public ProcessorContext? Context { get; set; }
        }

        private StartProcessInitResult InitializeStartProcess(string migrationUnitId, string sourceConnectionString, string targetConnectionString)
        {
            try
            {
                MigrationJobContext.ControlledPauseRequested = false;
                ProcessRunning = true;
                var mu = MigrationJobContext.MigrationUnitsCache.GetMigrationUnit(migrationUnitId);
                mu.ParentJob = MigrationJobContext.CurrentlyActiveJob;

                // Initialize processor context (parity with CopyProcessor)
                var ctx = SetProcessorContext(mu, sourceConnectionString, targetConnectionString);

                return new StartProcessInitResult
                {
                    Result = TaskResult.Success,
                    MigrationUnit = mu,
                    Context = ctx
                };
            }
            catch (Exception ex)
            {
                _log.WriteLine($"Error starting process for migration unit {migrationUnitId},Ex: {Helper.RedactPii(ex.ToString())}", LogType.Error);
                return new StartProcessInitResult
                {
                    Result = TaskResult.Retry
                };
            }
        }

        private void PrepareDumpProcess(MigrationUnit mu, string jobId, string dbName, string colName)
        {
            // starting the regular dump and restore process
            if (!mu.BulkCopyStartedOn.HasValue || mu.BulkCopyStartedOn == DateTime.MinValue)
                mu.BulkCopyStartedOn = DateTime.UtcNow;

            bool isDirty = false;
            
            // check if dump complete but restore not complete yet all chunks are downloaded
            if (mu.DumpComplete && !mu.RestoreComplete && mu.MigrationChunks.All(c => c.IsDownloaded == true))
            {
                _log.WriteLine($"{dbName}.{colName} dump complete, but chunks pending. Reprocessing.", LogType.Warning);
                mu.DumpComplete = false;
                isDirty = true;
            }

            // check to make sure chunk files are present, else mark chunk as not downloaded
            if (mu.DumpComplete && !mu.RestoreComplete)
            {
                for (int i = 0; i < mu.MigrationChunks.Count; i++)
                {
                    string chunkFilePath = Path.Combine(_mongoDumpOutputFolder, jobId, Helper.SafeFileName($"{dbName}.{colName}"), $"{i}.bson");
                    if (!File.Exists(chunkFilePath))
                    {
                        _log.WriteLine($"Chunk file missing for {dbName}.{colName}[{i}] at {chunkFilePath}. Marking as not downloaded; will reprocess.", LogType.Warning);
                        mu.MigrationChunks[i].IsDownloaded = false;
                        mu.DumpComplete = false;
                        isDirty = true;
                    }
                }
            }

            if (isDirty)
            {
                MigrationJobContext.SaveMigrationUnit(mu, true);
            }
        }

        private string InitializeDumpFolder(MigrationUnit mu, ProcessorContext ctx, string jobId, string dbName, string colName)
        {
            if (!System.IO.Directory.Exists(_mongoDumpOutputFolder))
            {
                System.IO.Directory.CreateDirectory(_mongoDumpOutputFolder);
            }

            // mongodump output folder 
            string folder = Path.Combine(_mongoDumpOutputFolder, jobId, Helper.SafeFileName($"{dbName}.{colName}"));

            Directory.CreateDirectory(folder);

            _log.WriteLine($"{dbName}.{colName} download started");

            mu.EstimatedDocCount = ctx.Collection.EstimatedDocumentCount();

            // Start the actual document count operation in the background without awaiting
            Task<long>? actualCountTask = null;
            if (mu.ActualDocCount == 0) // Only start if we don't already have the count
            {
                actualCountTask = Task.Run(() => MongoHelper.GetActualDocumentCount(ctx.Collection, mu), _cts.Token);

                // Optional: Fire-and-forget completion handler to update the count when ready
                _ = actualCountTask.ContinueWith(task =>
                {
                    if (task.IsCompletedSuccessfully)
                    {
                        mu.ActualDocCount = task.Result;
                        MigrationJobContext.SaveMigrationUnit(mu, false);
                        _log.WriteLine($"{dbName}.{colName} actual document count: {task.Result}");
                    }
                }, _cts.Token, TaskContinuationOptions.OnlyOnRanToCompletion, TaskScheduler.Default);
            }

            return folder;
        }

        private void FinalizeDumpCompletion(MigrationUnit mu, ref long downloadCount)
        {
            mu.SourceCountDuringCopy = mu.MigrationChunks.Sum(chunk => chunk.DumpQueryDocCount);
            downloadCount = mu.SourceCountDuringCopy; // recompute from chunks to avoid incremental tracking

            mu.DumpGap = Helper.GetMigrationUnitDocCount(mu) - downloadCount;
            mu.DumpPercent = 100;
            mu.DumpComplete = true;

            MigrationJobContext.SaveMigrationUnit(mu, true);

            // BulkCopyEndedOn will be set after restore completes, not here
        }

        #endregion

        #region Helper Methods for Dump

        private bool CheckDiskSpaceForDump(MigrationUnit mu, string folder, string targetConnectionString, string dbName, string colName)
        {
            bool continueDownloads;
            double pendingUploadsGB = 0;
            double freeSpaceGB = 0;
            
            while (true)
            {
                // Check for controlled pause
                if (MigrationJobContext.ControlledPauseRequested)
                {
                    _log.WriteLine("Controlled pause detected - exiting dump");
                    return false;
                }
                
                continueDownloads = Helper.CanProceedWithDownloads(folder, _config.ChunkSizeInMb * 2, out pendingUploadsGB, out freeSpaceGB);
                if (!continueDownloads)
                {
                    _log.WriteLine($"{dbName}.{colName} added to upload queue");
                    _migrationUnitsPendingUpload.Add(mu.Id);
                    _ = Task.Run(() => Upload(mu.Id, targetConnectionString), _cts.Token);

                    _log.WriteLine($"Disk space is running low, with only {freeSpaceGB}GB available. Pending jobList are using {pendingUploadsGB}GB of space. Free up disk space by deleting unwanted jobList. Alternatively, you can scale up tp Premium App Service plan, which will reset the WebApp. New downloads will resume in 5 minutes...", LogType.Error);

                    try { Task.Delay(TimeSpan.FromMinutes(5), _cts.Token).Wait(_cts.Token); }
                    catch (OperationCanceledException) { return false; }
                }
                else break;
            }
            
            return true;
        }

        private long BuildDumpQuery(MigrationUnit mu, int chunkIndex, IMongoCollection<BsonDocument> collection, 
            string dbName, string colName, ref string args)
        {
            long docCount = 0;
            
            if (mu.MigrationChunks.Count > 1)
            {
                docCount = BuildMultiChunkDumpQuery(mu, chunkIndex, collection, dbName, colName, ref args);
            }
            else if (mu.MigrationChunks.Count == 1 && !string.IsNullOrEmpty(mu.UserFilter))
            {
                docCount = BuildSingleChunkWithFilterDumpQuery(mu, collection, ref args);
            }
            else
            {
                docCount = Helper.GetMigrationUnitDocCount(mu);
                mu.MigrationChunks[chunkIndex].DumpQueryDocCount = docCount;
            }
            
            return docCount;
        }

        private long BuildMultiChunkDumpQuery(MigrationUnit mu, int chunkIndex, IMongoCollection<BsonDocument> collection,
            string dbName, string colName, ref string args)
        {
            var bounds = SamplePartitioner.GetChunkBounds(mu.MigrationChunks[chunkIndex].Gte!, mu.MigrationChunks[chunkIndex].Lt!, mu.MigrationChunks[chunkIndex].DataType);
            var gte = bounds.gte;
            var lt = bounds.lt;

            _log.WriteLine($"{dbName}.{colName}-Chunk [{chunkIndex}] generating query");

            BsonDocument? userFilterDoc = MongoHelper.GetFilterDoc(mu.UserFilter);
            string query = MongoHelper.GenerateQueryString(gte, lt, mu.MigrationChunks[chunkIndex].DataType, userFilterDoc, mu);
            long docCount = MongoHelper.GetDocumentCount(collection, gte, lt, mu.MigrationChunks[chunkIndex].DataType, userFilterDoc, mu.DataTypeFor_Id.HasValue);
            mu.MigrationChunks[chunkIndex].DumpQueryDocCount = docCount;
            _log.WriteLine($"Count for {dbName}.{colName}[{chunkIndex}] is {docCount}");

            string extendedQuery = MongoQueryConverter.ConvertMondumpFilter(query, gte, lt, mu.MigrationChunks[chunkIndex].DataType);
            args = $"{args} --query=\"{extendedQuery}\"";
            
            return docCount;
        }

        private long BuildSingleChunkWithFilterDumpQuery(MigrationUnit mu, IMongoCollection<BsonDocument> collection, ref string args)
        {
            BsonDocument? userFilterDoc = MongoHelper.GetFilterDoc(mu.UserFilter);
            long docCount = MongoHelper.GetActualDocumentCount(collection, mu);
            string query = MongoHelper.GenerateQueryString(userFilterDoc);
            args = $"{args} --query=\"{query}\"";
            
            return docCount;
        }

        #endregion

        #region Helper Methods for Restore

        private List<ChunkWorkItem> BuildRestoreWorkQueue(MigrationUnit mu)
        {
            return mu.MigrationChunks
                .Select((chunk, index) => new ChunkWorkItem
                {
                    ChunkIndex = index,
                    MigrationUnit = mu,
                    Chunk = chunk,
                    QueuedAt = DateTime.UtcNow
                })
                .Where(item => item.Chunk.IsUploaded != true && item.Chunk.IsDownloaded == true)
                .OrderBy(item => item.ChunkIndex)
                .ToList();
        }

        private RestoreResult? ValidateDumpCompletion(MigrationUnit mu)
        {
            // Check if mu.DumpComplete == true but some chunks have IsDownloaded = false
            if (mu.DumpComplete && mu.MigrationChunks.Any(c => c.IsDownloaded != true))
            {
                _log.WriteLine($"{mu.DatabaseName}.{mu.CollectionName} has incomplete chunks. Reverting its download status and triggering a controlled pause. Resume the job to resolve.", LogType.Warning);
                mu.DumpComplete = false;

                MigrationJobContext.SaveMigrationUnit(mu, true);

                return new RestoreResult
                {
                    Result = TaskResult.Canceled,
                    RestoredChunks = 0,
                    RestoredDocs = 0
                };
            }

            return null;
        }

        private RestoreResult HandleCompletedRestore(MigrationUnit mu, string dbName, string colName, ref int restoredChunks, ref long restoredDocs)
        {
            _log.WriteLine($"{mu.MigrationChunks.Count} chunks in {dbName}.{colName}");

            // Count already restored chunks
            long dumpedDocs = 0;
            foreach (var chunk in mu.MigrationChunks.Where(c => c.IsUploaded == true))
            {
                restoredChunks++;
                restoredDocs += chunk.RestoredSuccessDocCount;
                dumpedDocs += chunk.DumpQueryDocCount;
            }

            _log.WriteLine($"{restoredChunks} chunks restored for {mu.DatabaseName}.{mu.CollectionName}, Restored documents: {restoredDocs}", LogType.Verbose);

            if (restoredDocs < dumpedDocs)
            {
                _log.WriteLine($"Only {restoredDocs} out of {dumpedDocs} documents restored. Resetting chunk states for all chunks for {mu.DatabaseName}.{mu.CollectionName}.", LogType.Warning);
                foreach (var chunk in mu.MigrationChunks)
                {
                    chunk.IsUploaded = false;
                    chunk.RestoredSuccessDocCount = 0;
                }

                MigrationJobContext.SaveMigrationUnit(mu, true);

                MigrationJobContext.ControlledPauseRequested = true;

                // Return 0 to trigger retry since we reset the chunks
                return new RestoreResult
                {
                    Result = TaskResult.Canceled,
                    RestoredChunks = 0,
                    RestoredDocs = 0
                };
            }

            // Return the actual count of restored chunks, not 0
            return new RestoreResult
            {
                Result = TaskResult.Success,
                RestoredChunks = restoredChunks,
                RestoredDocs = restoredDocs
            };
        }

        private Task<TaskResult> HandleRestoreChunkSuccess(MigrationUnit mu, int chunkIndex, string folder, 
            string targetConnectionString, string dbName, string colName, long docCount)
        {
            bool skipFinalize = false;

            if (mu.MigrationChunks[chunkIndex].RestoredFailedDocCount > 0)
            {
                skipFinalize = ValidateRestoredChunkDocumentCount(mu, chunkIndex, targetConnectionString, dbName, colName);
            }
            
            // mongorestore doesn't report on doc count sometimes. hence we need to calculate based on targetCount percent
            mu.MigrationChunks[chunkIndex].RestoredSuccessDocCount = docCount - (mu.MigrationChunks[chunkIndex].RestoredFailedDocCount + mu.MigrationChunks[chunkIndex].SkippedAsDuplicateCount);
            _log.WriteLine($"{dbName}.{colName}[{chunkIndex}] uploader processing completed");

            if (!skipFinalize)
            {
                return FinalizeRestoreChunk(mu, chunkIndex, folder);
            }
            else
            {
                return Task.FromResult(TaskResult.Retry);
            }
        }

        private bool ValidateRestoredChunkDocumentCount(MigrationUnit mu, int chunkIndex, 
            string targetConnectionString, string dbName, string colName)
        {
            if (_targetClient == null && !MigrationJobContext.CurrentlyActiveJob.IsSimulatedRun)
                _targetClient = MongoClientFactory.Create(_log, targetConnectionString);

            try
            {
                var targetDb = _targetClient!.GetDatabase(mu.DatabaseName);
                var targetCollection = targetDb.GetCollection<BsonDocument>(mu.CollectionName);

                var bounds = SamplePartitioner.GetChunkBounds(mu.MigrationChunks[chunkIndex].Gte!, mu.MigrationChunks[chunkIndex].Lt!, mu.MigrationChunks[chunkIndex].DataType);
                var gte = bounds.gte;
                var lt = bounds.lt;

                // get count in target collection
                mu.MigrationChunks[chunkIndex].DocCountInTarget = MongoHelper.GetDocumentCount(targetCollection, gte, lt, mu.MigrationChunks[chunkIndex].DataType, MongoHelper.ConvertUserFilterToBSONDocument(mu.UserFilter!), mu.DataTypeFor_Id.HasValue);

                // checking if source and target doc counts are same or more
                if (mu.MigrationChunks[chunkIndex].DocCountInTarget >= mu.MigrationChunks[chunkIndex].DumpQueryDocCount)
                {
                    _log.WriteLine($"Restore for {dbName}.{colName}[{chunkIndex}] No documents missing, count in Target: {mu.MigrationChunks[chunkIndex].DocCountInTarget}");
                    mu.MigrationChunks[chunkIndex].SkippedAsDuplicateCount = mu.MigrationChunks[chunkIndex].RestoredFailedDocCount;
                    mu.MigrationChunks[chunkIndex].RestoredFailedDocCount = 0;
                    MigrationJobContext.SaveMigrationUnit(mu, false);
                    return false; // Don't skip finalize
                }
                else
                {
                    // since count is mismatched, we will reprocess the chunk
                    _log.WriteLine($"Restore for {dbName}.{colName}[{chunkIndex}] Documents missing, Chunk will be reprocessed", LogType.Error);
                    MigrationJobContext.SaveMigrationUnit(mu, false);
                    return true; // Skip finalize
                }
            }
            catch (Exception ex)
            {
                _log.WriteLine($"Restore for {dbName}.{colName}[{chunkIndex}] encountered error while counting documents on target. Chunk will be reprocessed. Details: {Helper.RedactPii(ex.ToString())}", LogType.Error);
                return true; // Skip finalize
            }
        }

        private Task<TaskResult> FinalizeRestoreChunk(MigrationUnit mu, int chunkIndex, string folder)
        {
            mu.MigrationChunks[chunkIndex].IsUploaded = true;
            MigrationJobContext.SaveMigrationUnit(mu, false);

            try
            {
                File.Delete($"{Path.Combine(folder, $"{chunkIndex}.bson")}");
            }
            catch
            {
                // Ignore file delete errors
            }
            return Task.FromResult(TaskResult.Success);
        }

        private Task<TaskResult>? SimulateRestoreChunk(MigrationUnit mu, int chunkIndex, double initialPercent, double contributionFactor)
        {
            // Check if simulation mode is enabled
            if (!MigrationJobContext.CurrentlyActiveJob.IsSimulatedRun)
            {
                return null; // Not in simulation mode, proceed with normal restore
            }
            
            // Simulate successful restore
            mu.MigrationChunks[chunkIndex].RestoredSuccessDocCount = mu.MigrationChunks[chunkIndex].DumpQueryDocCount;
            mu.MigrationChunks[chunkIndex].RestoredFailedDocCount = 0;
            mu.MigrationChunks[chunkIndex].IsUploaded = true;
            
            // Update progress
            double progress = initialPercent + (contributionFactor * 100);
            mu.RestorePercent = Math.Min(progress, 100);
            
            _log.WriteLine($"Simulation mode: Chunk {chunkIndex} restore simulated - {mu.RestorePercent:F2}% complete (RestorePercent={mu.RestorePercent})");

            MigrationJobContext.SaveMigrationUnit(mu, true);

            // Small delay to simulate processing time (50ms per chunk)
            try { Task.Delay(50, _cts.Token).Wait(_cts.Token); } catch { }
            
            return Task.FromResult(TaskResult.Success);
        }

        private int GetInsertionWorkersCount()
        {
            return WorkerCountHelper.GetInsertionWorkersCount(
                MigrationJobContext.CurrentlyActiveJob.MaxInsertionWorkersPerCollection,
                MigrationJobContext.CurrentlyActiveJob.CurrentInsertionWorkers
            );
        }

        private Task<TaskResult>? ValidateDumpFileExists(MigrationUnit mu, int chunkIndex, string dumpFilePath)
        {
            // Data integrity check - force controlled pause if dump file missing
            if (!File.Exists(dumpFilePath))
            {
                _log.WriteLine($"Chunk file missing for {mu.DatabaseName}.{mu.CollectionName}[{chunkIndex}] during restore. Triggering controlled pause. Resume job to continue.", LogType.Warning);
                mu.MigrationChunks[chunkIndex].IsDownloaded = false;
                mu.DumpComplete = false;

                MigrationJobContext.SaveMigrationUnit(mu, true);

                MigrationJobContext.ControlledPauseRequested = true;
                return Task.FromResult(TaskResult.Canceled);
            }
            
            return null; // File exists, validation passed
        }

        private string BuildDumpArgs(string sourceConnectionString, string dbName, string colName)
        {
            return $" --uri=\"{sourceConnectionString}\" --gzip --db={dbName} --collection=\"{colName}\" --archive";
        }

        private string PrepareDumpFile(string folder, int chunkIndex)
        {
            var dumpFilePath = $"{Path.Combine(folder, $"{chunkIndex}.bson")}";
            
            // Ensure previous dump file (if any) is removed before fresh dump
            if (File.Exists(dumpFilePath))
            {
                try { File.Delete(dumpFilePath); } catch { }
            }
            
            return dumpFilePath;
        }

        private bool ExecuteDumpProcess(MigrationUnit mu, int chunkIndex, string args, string dumpFilePath, 
            long docCount, double initialPercent, double contributionFactor)
        {
            // Create dedicated executor for this worker to avoid shared state issues
            var processExecutor = new ProcessExecutor(_log);

            var task = Task.Run(() => processExecutor.Execute(
                mu, 
                mu.MigrationChunks[chunkIndex], 
                chunkIndex, 
                initialPercent, 
                contributionFactor, 
                docCount, 
                $"{MongoToolsFolder}mongodump", 
                args,
                dumpFilePath,
                _cts.Token,
                onProcessStarted: (pid) => RegisterDumpProcess(pid),
                onProcessEnded: (pid) => UnregisterDumpProcess(pid),
                isControlledPauseRequested: () => MigrationJobContext.ControlledPauseRequested
            ), _cts.Token);
            
            task.Wait(_cts.Token);
            return task.Result;
        }

        private Task<TaskResult> HandleDumpChunkSuccess(MigrationUnit mu, int chunkIndex, 
            string targetConnectionString, string dbName, string colName)
        {
            UpdateChunkStatusSafe(mu.MigrationChunks[chunkIndex], () =>
            {
                mu.MigrationChunks[chunkIndex].IsDownloaded = true;
            });
            
            _log.WriteLine($"{dbName}.{colName} added to upload queue.");
            _migrationUnitsPendingUpload.Add(mu.Id);
            Task.Run(() => Upload(mu.Id, targetConnectionString), _cts.Token);
            
            return Task.FromResult(TaskResult.Success);
        }

        private string BuildRestoreArgs(string targetConnectionString, MigrationUnit mu, int chunkIndex, 
            string dbName, string colName)
        {
            string args = $" --uri=\"{targetConnectionString}\" --gzip --archive";

            // If first mu, drop collection, else append. Also No drop in AppendMode
            if (chunkIndex == 0 && !MigrationJobContext.CurrentlyActiveJob.AppendMode)
            {
                args = $"{args} --drop";
                if (MigrationJobContext.CurrentlyActiveJob.SkipIndexes)
                {
                    args = $"{args} --noIndexRestore"; // No index to create for all chunks.
                }
            }
            else
            {
                args = $"{args} --noIndexRestore"; // No index to create. Index restore only for 1st chunk.
            }
            
            return args;
        }

        private string ConfigureInsertionWorkers(string args, string dbName, string colName, 
            int chunkIndex, long docCount)
        {
            int insertionWorkers = GetInsertionWorkersCount();
            _log.WriteLine($"Restore will use {insertionWorkers} insertion worker(s) for {dbName}.{colName}[{chunkIndex}] ({docCount} docs)");
            
            if (insertionWorkers > 1)
            {
                args = $"{args} --numInsertionWorkersPerCollection={insertionWorkers}";
            }
            
            return args;
        }

        private bool ExecuteRestoreProcess(MigrationUnit mu, int chunkIndex, string args, string dumpFilePath, 
            long docCount, double initialPercent, double contributionFactor)
        {
            // Create dedicated executor for this worker to avoid shared state issues
            var processExecutor = new ProcessExecutor(_log);

            var task = Task.Run(() => processExecutor.Execute(
                mu, 
                mu.MigrationChunks[chunkIndex], 
                chunkIndex, 
                initialPercent, 
                contributionFactor, 
                docCount, 
                $"{MongoToolsFolder}mongorestore", 
                args,
                dumpFilePath,
                _cts.Token,
                onProcessStarted: (pid) => RegisterRestoreProcess(pid),
                onProcessEnded: (pid) => UnregisterRestoreProcess(pid),
                isControlledPauseRequested: () => MigrationJobContext.ControlledPauseRequested
            ), _cts.Token);
            
            task.Wait(_cts.Token);
            return task.Result;
        }

        #endregion
    }
}

