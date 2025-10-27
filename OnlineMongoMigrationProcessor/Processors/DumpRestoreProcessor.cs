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

// CS4014: Use explicit discards for intentional fire-and-forget tasks.

namespace OnlineMongoMigrationProcessor
{
    internal class DumpRestoreProcessor : MigrationProcessor
    {

        //private string _toolsLaunchFolder = string.Empty;
        private string _mongoDumpOutputFolder = $"{Helper.GetWorkingFolder()}mongodump";
        private static readonly SemaphoreSlim _uploadLock = new(1, 1);

        private SafeFifoCollection<string, MigrationUnit> MigrationUnitsPendingUpload = new SafeFifoCollection<string, MigrationUnit>();

        // Parallel processing infrastructure
        private int _maxParallelDumpInstances;
        private int _maxParallelRestoreInstances;
        private SemaphoreSlim? _dumpSemaphore;
        private SemaphoreSlim? _restoreSemaphore;
        
        // Cancellation tokens for semaphore blocker tasks
        private CancellationTokenSource? _dumpBlockerCts;
        private CancellationTokenSource? _restoreBlockerCts;
        
        // Thread-safe locks
        private readonly object _pidLock = new object();
        private readonly object _progressLock = new object();
        private readonly object _chunkUpdateLock = new object();
        
        // Error tracking
        private readonly ConcurrentBag<(int ChunkIndex, Exception Error)> _chunkErrors = new();
        
        // Chunk work queues
        private readonly ConcurrentQueue<ChunkWorkItem> _dumpQueue = new();
        private readonly ConcurrentQueue<ChunkWorkItem> _restoreQueue = new();
        
        // Progress tracking
        private DateTime _lastSave = DateTime.MinValue;
        private const int SAVE_DEBOUNCE_SECONDS = 2;
        
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

        public DumpRestoreProcessor(Log log, JobList jobList, MigrationJob job, MongoClient sourceClient, MigrationSettings config)
            : base(log, jobList, job, sourceClient, config)
        {
            // Calculate optimal concurrency
            if (job.EnableParallelProcessing)
            {
                _maxParallelDumpInstances = CalculateOptimalConcurrency(
                    job.MaxParallelDumpProcesses,
                    isDump: true
                );
                
                _maxParallelRestoreInstances = CalculateOptimalConcurrency(
                    job.MaxParallelRestoreProcesses,
                    isDump: false
                );
            }
            else
            {
                _maxParallelDumpInstances = 1;
                _maxParallelRestoreInstances = 1;
            }
            
            // Store initial values in job for UI monitoring
            _job.CurrentDumpWorkers = _maxParallelDumpInstances;
            _job.CurrentRestoreWorkers = _maxParallelRestoreInstances;
            
            // Initialize insertion workers (will be calculated per chunk based on doc count)
            if (!_job.MaxInsertionWorkersPerCollection.HasValue)
            {
                _job.CurrentInsertionWorkers = Math.Min(Environment.ProcessorCount / 4, 8);
            }
            else
            {
                _job.CurrentInsertionWorkers = _job.MaxInsertionWorkersPerCollection.Value;
            }
            
            // Initialize semaphores
            _dumpSemaphore = new SemaphoreSlim(_maxParallelDumpInstances, _maxParallelDumpInstances);
            _restoreSemaphore = new SemaphoreSlim(_maxParallelRestoreInstances, _maxParallelRestoreInstances);
            
            _log.WriteLine($"Parallel processing: Dump workers={_maxParallelDumpInstances}, Restore workers={_maxParallelRestoreInstances}");
        }

        /// <summary>
        /// Adjusts the number of dump workers at runtime. Increase adds workers, decrease reduces capacity.
        /// </summary>
        public void AdjustDumpWorkers(int newCount)
        {
            if (newCount < 1) newCount = 1;
            if (newCount > 16) newCount = 16; // Safety limit
            
            int difference = newCount - _maxParallelDumpInstances;
            
            if (difference == 0) return;
            
            _maxParallelDumpInstances = newCount;
            _job.CurrentDumpWorkers = newCount;
            _job.MaxParallelDumpProcesses = newCount; // Update config too
            
            if (difference > 0)
            {
                // Cancel any existing blocker task first
                _dumpBlockerCts?.Cancel();
                _dumpBlockerCts?.Dispose();
                _dumpBlockerCts = null;
                
                // Increase: Create a new semaphore with increased capacity
                // Get current available count before disposing
                int currentAvailable = _dumpSemaphore?.CurrentCount ?? newCount;
                _dumpSemaphore?.Dispose();
                
                // New semaphore has higher max count, start with previous available + difference
                _dumpSemaphore = new SemaphoreSlim(currentAvailable + difference, newCount);
                
                _log.WriteLine($"Increased dump workers to {newCount} (+{difference})");
            }
            else
            {
                // Decrease: Consume semaphore slots to prevent new workers from starting
                // Cancel any previous blocker task and create a new one
                _dumpBlockerCts?.Cancel();
                _dumpBlockerCts?.Dispose();
                _dumpBlockerCts = new CancellationTokenSource();
                
                var cts = _dumpBlockerCts;
                Task.Run(async () =>
                {
                    int acquiredCount = 0;
                    try
                    {
                        for (int i = 0; i < Math.Abs(difference); i++)
                        {
                            await _dumpSemaphore!.WaitAsync(cts.Token);
                            acquiredCount++;
                            _log.WriteLine($"Consumed dump semaphore slot {acquiredCount}/{Math.Abs(difference)} to enforce limit");
                        }
                    }
                    catch (OperationCanceledException)
                    {
                        // Release any slots we acquired before cancellation
                        for (int i = 0; i < acquiredCount; i++)
                        {
                            try { _dumpSemaphore?.Release(); } catch { }
                        }
                        _log.WriteLine($"Dump blocker task cancelled (limit increased), released {acquiredCount} slots");
                    }
                    catch { }
                }, cts.Token);
                _log.WriteLine($"Decreased dump worker limit to {newCount} ({difference}). Active workers will finish current tasks.");
            }
            
            _jobList?.Save();
        }

        /// <summary>
        /// Adjusts the number of restore workers at runtime. Increase adds workers, decrease reduces capacity.
        /// </summary>
        public void AdjustRestoreWorkers(int newCount)
        {
            if (newCount < 1) newCount = 1;
            if (newCount > 16) newCount = 16; // Safety limit
            
            int difference = newCount - _maxParallelRestoreInstances;
            
            if (difference == 0) return;
            
            _maxParallelRestoreInstances = newCount;
            _job.CurrentRestoreWorkers = newCount;
            _job.MaxParallelRestoreProcesses = newCount; // Update config too
            
            if (difference > 0)
            {
                // Cancel any existing blocker task first
                _restoreBlockerCts?.Cancel();
                _restoreBlockerCts?.Dispose();
                _restoreBlockerCts = null;
                
                // Increase: Create a new semaphore with increased capacity
                // Get current available count before disposing
                int currentAvailable = _restoreSemaphore?.CurrentCount ?? newCount;
                _restoreSemaphore?.Dispose();
                
                // New semaphore has higher max count, start with previous available + difference
                _restoreSemaphore = new SemaphoreSlim(currentAvailable + difference, newCount);
                
                _log.WriteLine($"Increased restore workers to {newCount} (+{difference})");
            }
            else
            {
                // Decrease: Consume semaphore slots to prevent new workers from starting
                // Cancel any previous blocker task and create a new one
                _restoreBlockerCts?.Cancel();
                _restoreBlockerCts?.Dispose();
                _restoreBlockerCts = new CancellationTokenSource();
                
                var cts = _restoreBlockerCts;
                Task.Run(async () =>
                {
                    int acquiredCount = 0;
                    try
                    {
                        for (int i = 0; i < Math.Abs(difference); i++)
                        {
                            await _restoreSemaphore!.WaitAsync(cts.Token);
                            acquiredCount++;
                            _log.WriteLine($"Consumed restore semaphore slot {acquiredCount}/{Math.Abs(difference)} to enforce limit");
                        }
                    }
                    catch (OperationCanceledException)
                    {
                        // Release any slots we acquired before cancellation
                        for (int i = 0; i < acquiredCount; i++)
                        {
                            try { _restoreSemaphore?.Release(); } catch { }
                        }
                        _log.WriteLine($"Restore blocker task cancelled (limit increased), released {acquiredCount} slots");
                    }
                    catch { }
                }, cts.Token);
                _log.WriteLine($"Decreased restore worker limit to {newCount} ({difference}). Active workers will finish current tasks.");
            }
            
            _jobList?.Save();
        }

        /// <summary>
        /// Adjusts the number of insertion workers per collection for mongorestore at runtime.
        /// </summary>
        public void AdjustInsertionWorkers(int newCount)
        {
            if (newCount < 1) newCount = 1;
            if (newCount > 16) newCount = 16; // Safety limit
            
            _job.CurrentInsertionWorkers = newCount;
            _job.MaxInsertionWorkersPerCollection = newCount;
            
            _log.WriteLine($"Set insertion workers per collection to {newCount}. Will apply to new restore operations.");
            
            _jobList?.Save();
        }

        private int CalculateOptimalConcurrency(int? configOverride, bool isDump)
        {
            //return 1;
            // User override takes precedence
            if (configOverride.HasValue && configOverride.Value > 0)
            {
                _log.WriteLine($"Using configured {(isDump ? "dump" : "restore")} concurrency: {configOverride.Value}");
                return configOverride.Value;
            }
            
            // Base calculation: 1 instance per 2.5 cores
            int baseConcurrency = Math.Max(1, (int)(Environment.ProcessorCount / 2.5));
            
            // Memory safety check (500MB per process) - simplified, assume 8GB if we can't check
            int memorySafeConcurrency = baseConcurrency; // Simplified for now
            
            // Final calculation
            int finalConcurrency = Math.Min(baseConcurrency, memorySafeConcurrency);
            
            _log.WriteLine($"Calculated {(isDump ? "dump" : "restore")} concurrency: " +
                          $"cores={baseConcurrency}, final={finalConcurrency}");
            
            return finalConcurrency;
        }

        #region Thread-Safe PID Tracking

        private void RegisterDumpProcess(int pid)
        {
            lock (_pidLock)
            {
                if (!_jobList.ActiveDumpProcessIds.Contains(pid))
                {
                    _jobList.ActiveDumpProcessIds.Add(pid);
                    _log.WriteLine($"Registered dump process: PID {pid} (total active: {_jobList.ActiveDumpProcessIds.Count})");
                    SaveJobListDebounced();
                }
            }
        }

        private void UnregisterDumpProcess(int pid)
        {
            lock (_pidLock)
            {
                if (_jobList.ActiveDumpProcessIds.Remove(pid))
                {
                    _log.WriteLine($"Unregistered dump process: PID {pid} (total active: {_jobList.ActiveDumpProcessIds.Count})");
                    SaveJobListDebounced();
                }
            }
        }

        private void RegisterRestoreProcess(int pid)
        {
            lock (_pidLock)
            {
                if (!_jobList.ActiveRestoreProcessIds.Contains(pid))
                {
                    _jobList.ActiveRestoreProcessIds.Add(pid);
                    _log.WriteLine($"Registered restore process: PID {pid} (total active: {_jobList.ActiveRestoreProcessIds.Count})");
                    SaveJobListDebounced();
                }
            }
        }

        private void UnregisterRestoreProcess(int pid)
        {
            lock (_pidLock)
            {
                if (_jobList.ActiveRestoreProcessIds.Remove(pid))
                {
                    _log.WriteLine($"Unregistered restore process: PID {pid} (total active: {_jobList.ActiveRestoreProcessIds.Count})");
                    SaveJobListDebounced();
                }
            }
        }

        private void UpdateChunkStatusSafe(MigrationChunk chunk, Action updateAction)
        {
            lock (_chunkUpdateLock)
            {
                updateAction();
                SaveJobListDebounced();
            }
        }

        private void SaveJobListDebounced()
        {
            // Already in a lock when called
            if ((DateTime.UtcNow - _lastSave).TotalSeconds > SAVE_DEBOUNCE_SECONDS)
            {
                _jobList.Save();
                _lastSave = DateTime.UtcNow;
            }
        }

        private void KillAllActiveProcesses()
        {
            lock (_pidLock)
            {
                int totalProcesses = _jobList.ActiveDumpProcessIds.Count + _jobList.ActiveRestoreProcessIds.Count;
                
                if (totalProcesses == 0)
                {
                    return;
                }
                
                _log.WriteLine($"Killing {totalProcesses} active processes ({_jobList.ActiveDumpProcessIds.Count} dump, {_jobList.ActiveRestoreProcessIds.Count} restore)");

                // Kill all dump processes
                foreach (var pid in _jobList.ActiveDumpProcessIds.ToList())
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
                _jobList.ActiveDumpProcessIds.Clear();

                // Kill all restore processes
                foreach (var pid in _jobList.ActiveRestoreProcessIds.ToList())
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
                _jobList.ActiveRestoreProcessIds.Clear();

                _jobList.Save();
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
            
            _log.WriteLine($"Starting parallel dump of {sortedChunks.Count} chunks with {_maxParallelDumpInstances} workers");
            
            // Enqueue all chunks
            foreach (var chunk in sortedChunks)
            {
                _dumpQueue.Enqueue(chunk);
            }
            
            // Start worker tasks
            var workerTasks = new List<Task<TaskResult>>();
            for (int i = 0; i < Math.Min(_maxParallelDumpInstances, sortedChunks.Count); i++)
            {
                int workerId = i + 1;
                var workerTask = Task.Run(async () =>
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
                }, _cts.Token);
                
                workerTasks.Add(workerTask);
            }
            
            // Wait for all workers to complete
            var results = await Task.WhenAll(workerTasks);
            
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
            _log.WriteLine($"Dump worker {workerId} started");
            
            while (_dumpQueue.TryDequeue(out var workItem))
            {
                if (_cts.Token.IsCancellationRequested)
                {
                    _log.WriteLine($"Dump worker {workerId} cancelled");
                    return TaskResult.Canceled;
                }
                
                bool semaphoreAcquired = false;
                try
                {
                    // Acquire semaphore slot
                    await _dumpSemaphore!.WaitAsync(_cts.Token);
                    semaphoreAcquired = true;
                    
                    try
                    {
                        _log.WriteLine($"Worker {workerId} processing chunk {workItem.ChunkIndex}");
                        
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
                    finally
                    {
                        // Always release semaphore if it was acquired
                        if (semaphoreAcquired)
                        {
                            try
                            {
                                _dumpSemaphore.Release();
                            }
                            catch (SemaphoreFullException ex)
                            {
                                _log.WriteLine($"Dump worker {workerId} chunk {workItem.ChunkIndex} semaphore overflow on release: {ex.Message}. Current count may already be at max.", LogType.Error);
                            }
                        }
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
                    _chunkErrors.Add((workItem.ChunkIndex, ex));
                    
                    // Continue processing other chunks unless critical error
                    if (ex is OutOfMemoryException || ex is StackOverflowException)
                    {
                        KillAllActiveProcesses();
                        return TaskResult.Abort;
                    }
                }
            }
            
            _log.WriteLine($"Dump worker {workerId} completed");
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
                .Where(item => item.Chunk.IsUploaded != true && item.Chunk.IsDownloaded == true)
                .OrderBy(item => item.ChunkIndex)
                .ToList();
            
            if (!sortedChunks.Any())
            {
                _log.WriteLine($"All chunks already restored for {dbName}.{colName}");
                
                // Count already restored chunks
                foreach (var chunk in mu.MigrationChunks.Where(c => c.IsUploaded == true))
                {
                    restoredChunks++;
                    restoredDocs += chunk.RestoredSuccessDocCount;
                }
                
                return new RestoreResult 
                { 
                    Result = TaskResult.Success, 
                    RestoredChunks = restoredChunks, 
                    RestoredDocs = restoredDocs 
                };
            }
            
            _log.WriteLine($"Starting parallel restore of {sortedChunks.Count} chunks with {_maxParallelRestoreInstances} workers");
            
            // Clear chunk errors before starting
            _chunkErrors.Clear();
            
            // Create queue for restore work
            var restoreQueue = new ConcurrentQueue<ChunkWorkItem>();
            foreach (var chunk in sortedChunks)
            {
                restoreQueue.Enqueue(chunk);
            }
            
            // Start worker tasks
            var workerTasks = new List<Task<TaskResult>>();
            for (int i = 0; i < Math.Min(_maxParallelRestoreInstances, sortedChunks.Count); i++)
            {
                int workerId = i + 1;
                var workerTask = Task.Run(async () =>
                {
                    return await RestoreWorkerAsync(
                        workerId,
                        restoreQueue,
                        folder,
                        targetConnectionString,
                        dbName,
                        colName
                    );
                }, _cts.Token);
                
                workerTasks.Add(workerTask);
            }
            
            // Wait for all workers to complete
            var results = await Task.WhenAll(workerTasks);
            
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

        private async Task<TaskResult> RestoreWorkerAsync(
            int workerId,
            ConcurrentQueue<ChunkWorkItem> restoreQueue,
            string folder,
            string targetConnectionString,
            string dbName,
            string colName)
        {
            _log.WriteLine($"Restore worker {workerId} started");
            
            while (restoreQueue.TryDequeue(out var workItem))
            {
                if (_cts.Token.IsCancellationRequested)
                {
                    _log.WriteLine($"Restore worker {workerId} cancelled");
                    return TaskResult.Canceled;
                }
                
                bool semaphoreAcquired = false;
                try
                {
                    // Acquire semaphore slot
                    await _restoreSemaphore!.WaitAsync(_cts.Token);
                    semaphoreAcquired = true;
                    
                    try
                    {
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
                    finally
                    {
                        // Always release semaphore if it was acquired
                        if (semaphoreAcquired)
                        {
                            try
                            {
                                _restoreSemaphore.Release();
                            }
                            catch (SemaphoreFullException ex)
                            {
                                _log.WriteLine($"Restore worker {workerId} chunk {workItem.ChunkIndex} semaphore overflow on release: {ex.Message}. Current count may already be at max.", LogType.Error);
                            }
                        }
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
                    _chunkErrors.Add((workItem.ChunkIndex, ex));
                    
                    // Continue processing other chunks unless critical error
                    if (ex is OutOfMemoryException || ex is StackOverflowException)
                    {
                        KillAllActiveProcesses();
                        return TaskResult.Abort;
                    }
                }
            }
            
            _log.WriteLine($"Restore worker {workerId} completed");
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

            // Build base args per attempt
            string args = $" --uri=\"{sourceConnectionString}\" --gzip --db={dbName} --collection=\"{colName}\"  --out {folder}\\{chunkIndex}.bson";

            // Disk space/backpressure check (retain existing behavior)
            bool continueDownloads;
            double pendingUploadsGB = 0;
            double freeSpaceGB = 0;
            while (true)
            {
                continueDownloads = Helper.CanProceedWithDownloads(folder, _config.ChunkSizeInMb * 2, out pendingUploadsGB, out freeSpaceGB);
                if (!continueDownloads)
                {
                    _log.WriteLine($"{dbName}.{colName} added to upload queue");
                    MigrationUnitsPendingUpload.AddOrUpdate($"{mu.DatabaseName}.{mu.CollectionName}", mu);
                    _ = Task.Run(() => Upload(mu, targetConnectionString), _cts.Token);

                    _log.WriteLine($"Disk space is running low, with only {freeSpaceGB}GB available. Pending jobList are using {pendingUploadsGB}GB of space. Free up disk space by deleting unwanted jobList. Alternatively, you can scale up tp Premium App Service plan, which will reset the WebApp. New downloads will resume in 5 minutes...", LogType.Error);

                    try { Task.Delay(TimeSpan.FromMinutes(5), _cts.Token).Wait(_cts.Token); }
                    catch (OperationCanceledException) { return Task.FromResult(TaskResult.Canceled); }
                }
                else break;
            }

            long docCount = 0;
            if (mu.MigrationChunks.Count > 1)
            {
                var bounds = SamplePartitioner.GetChunkBounds(mu.MigrationChunks[chunkIndex].Gte!, mu.MigrationChunks[chunkIndex].Lt!, mu.MigrationChunks[chunkIndex].DataType);
                var gte = bounds.gte;
                var lt = bounds.lt;

                _log.WriteLine($"{dbName}.{colName}-Chunk [{chunkIndex}] generating query");

                BsonDocument? userFilterDoc = MongoHelper.GetFilterDoc(mu.UserFilter);
                string query = MongoHelper.GenerateQueryString(gte, lt, mu.MigrationChunks[chunkIndex].DataType, userFilterDoc, mu);
                docCount = MongoHelper.GetDocumentCount(collection, gte, lt, mu.MigrationChunks[chunkIndex].DataType, userFilterDoc, mu.DataTypeFor_Id.HasValue);
                mu.MigrationChunks[chunkIndex].DumpQueryDocCount = docCount;
                _log.WriteLine($"Count for {dbName}.{colName}[{chunkIndex}] is {docCount}");

                string extendedQuery= MongoQueryConverter.ConvertMondumpFilter(query, gte, lt, mu.MigrationChunks[chunkIndex].DataType);
                args = $"{args} --query=\"{extendedQuery}\"";
            }
            else if (mu.MigrationChunks.Count == 1 && !string.IsNullOrEmpty(mu.UserFilter))
            {
                BsonDocument? userFilterDoc = MongoHelper.GetFilterDoc(mu.UserFilter);
                docCount = MongoHelper.GetActualDocumentCount(collection, mu);
                string query = MongoHelper.GenerateQueryString(userFilterDoc);
                args = $"{args} --query=\"{query}\"";
            }
            else
            {
                docCount = Helper.GetMigrationUnitDocCount(mu);
                mu.MigrationChunks[chunkIndex].DumpQueryDocCount = docCount;
            }

            // Ensure previous dump file (if any) is removed before fresh dump
            var dumpFilePath = $"{folder}\\{chunkIndex}.bson";
            if (File.Exists(dumpFilePath))
            {
                try { File.Delete(dumpFilePath); } catch { }
            }

            try
            {
                // Create dedicated executor for this worker to avoid shared state issues
                var processExecutor = new ProcessExecutor(_log);

                var task = Task.Run(() => processExecutor.Execute(
                    _jobList, 
                    mu, 
                    mu.MigrationChunks[chunkIndex], 
                    chunkIndex, 
                    initialPercent, 
                    contributionFactor, 
                    docCount, 
                    $"{MongoToolsFolder}\\mongodump.exe", 
                    args,
                    _cts.Token,
                    onProcessStarted: (pid) => RegisterDumpProcess(pid),
                    onProcessEnded: (pid) => UnregisterDumpProcess(pid)
                ), _cts.Token);
                task.Wait(_cts.Token);
                bool result = task.Result;

                if (result)
                {
                    UpdateChunkStatusSafe(mu.MigrationChunks[chunkIndex], () =>
                    {
                        mu.MigrationChunks[chunkIndex].IsDownloaded = true;
                    });
                    _log.WriteLine($"{dbName}.{colName} added to upload queue.");
                    MigrationUnitsPendingUpload.AddOrUpdate($"{mu.DatabaseName}.{mu.CollectionName}", mu);
                    Task.Run(() => Upload(mu, targetConnectionString), _cts.Token);
                    return Task.FromResult(TaskResult.Success);
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

            // Build args per attempt
            string args = $" --uri=\"{targetConnectionString}\" --gzip {folder}\\{chunkIndex}.bson";

            // If first mu, drop collection, else append. Also No drop in AppendMode
            if (chunkIndex == 0 && !_job.AppendMode)
            {
                args = $"{args} --drop";
                if (_job.SkipIndexes)
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

            // Determine insertion workers based on configuration or auto-calculate
            int insertionWorkers = 1; // Default
            
            if (_job.MaxInsertionWorkersPerCollection.HasValue)
            {
                // Use configured value
                insertionWorkers = _job.MaxInsertionWorkersPerCollection.Value;
            }
            else
            {
                // Use the calculated default from constructor
                insertionWorkers = _job.CurrentInsertionWorkers;
            }
            
            _log.WriteLine($"Restore will use {insertionWorkers} insertion worker(s) for {dbName}.{colName}[{chunkIndex}] ({docCount} docs)");
            
            if (insertionWorkers > 1)
            {
                args = $"{args} --numInsertionWorkersPerCollection={insertionWorkers}";
            }

            try
            {
                // Create dedicated executor for this worker to avoid shared state issues
                var processExecutor = new ProcessExecutor(_log);

                var task = Task.Run(() => processExecutor.Execute(
                    _jobList, 
                    mu, 
                    mu.MigrationChunks[chunkIndex], 
                    chunkIndex, 
                    initialPercent, 
                    contributionFactor, 
                    docCount, 
                    $"{MongoToolsFolder}\\mongorestore.exe", 
                    args,
                    _cts.Token,
                    onProcessStarted: (pid) => RegisterRestoreProcess(pid),
                    onProcessEnded: (pid) => UnregisterRestoreProcess(pid)
                ), _cts.Token);
                task.Wait(_cts.Token);
                bool result = task.Result;                             

                if (result)
                {
                    bool skipFinalize = false;

                    if (mu.MigrationChunks[chunkIndex].RestoredFailedDocCount > 0)
                    {
                        if (_targetClient == null && !_job.IsSimulatedRun)
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
                            }
                            else
                            {
                                // since count is mismatched, we will reprocess the chunk
                                skipFinalize = true;
                                _log.WriteLine($"Restore for {dbName}.{colName}[{chunkIndex}] Documents missing, Chunk will be reprocessed", LogType.Error);
                            }

                            _jobList?.Save();
                        }
                        catch (Exception ex)
                        {
                            _log.WriteLine($"Restore for {dbName}.{colName}[{chunkIndex}] encountered error while counting documents on target. Chunk will be reprocessed. Details: {Helper.RedactPii(ex.ToString())}", LogType.Error);
                            skipFinalize = true;
                        }
                    }
                    //mongorestore doesn't report on doc count sometimes. hence we need to calculate  based on targetCount percent
                    mu.MigrationChunks[chunkIndex].RestoredSuccessDocCount = docCount - (mu.MigrationChunks[chunkIndex].RestoredFailedDocCount + mu.MigrationChunks[chunkIndex].SkippedAsDuplicateCount);
                    _log.WriteLine($"{dbName}.{colName}[{chunkIndex}] uploader processing completed");

                    if (!skipFinalize)
                    {
                        mu.MigrationChunks[chunkIndex].IsUploaded = true;
                        _jobList?.Save();

                        try { File.Delete($"{folder}\\{chunkIndex}.bson"); } catch { }

                        return Task.FromResult(TaskResult.Success);
                    }
                    else
                    {
                        return Task.FromResult(TaskResult.Retry);
                    }
                }
                else
                {
                    if (mu.MigrationChunks[chunkIndex].IsUploaded == true)
                    {
                        // Already uploaded, treat as success
                        _jobList?.Save();
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

        public override async Task<TaskResult> StartProcessAsync(MigrationUnit mu, string sourceConnectionString, string targetConnectionString, string idField = "_id")
        {
             ProcessRunning = true;

            // Initialize processor context (parity with CopyProcessor)
            ProcessorContext ctx = SetProcessorContext(mu, sourceConnectionString, targetConnectionString);

            string jobId = ctx.JobId;
            string dbName = ctx.DatabaseName;
            string colName = ctx.CollectionName;

            // Create mongodump output folder if it does not exist
            string folder = $"{_mongoDumpOutputFolder}\\{jobId}\\{Helper.SafeFileName($"{dbName}.{colName}")}";
            Directory.CreateDirectory(folder);


            // when resuming a job, check if post-upload change stream processing is already in progress
            if (CheckChangeStreamAlreadyProcessingAsync(ctx))
                return TaskResult.Success;

            // starting the regular dump and restore process
            if (!mu.BulkCopyStartedOn.HasValue || mu.BulkCopyStartedOn == DateTime.MinValue)
                mu.BulkCopyStartedOn = DateTime.UtcNow;

            // DumpAndRestore
            if (!mu.DumpComplete && !_cts.Token.IsCancellationRequested)
            {
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
                            _jobList.Save();
                            _log.WriteLine($"{dbName}.{colName} actual document count: {task.Result}");
                        }
                    }, _cts.Token, TaskContinuationOptions.OnlyOnRanToCompletion, TaskScheduler.Default);
                }

                long downloadCount = 0;

                // Use parallel dump if enabled and multiple instances configured
                if (_maxParallelDumpInstances > 1)
                {
                    _log.WriteLine($"Using parallel dump with {_maxParallelDumpInstances} instances");
                    
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
                        _log.WriteLine($"Parallel dump operation for {dbName}.{colName} failed after multiple attempts.", LogType.Error);
                        StopProcessing();
                        return result;
                    }
                    
                    if (result == TaskResult.Canceled)
                    {
                        return result;
                    }
                }
                else
                {
                    // Sequential dump (original logic)
                    _log.WriteLine($"Using sequential dump");
                    
                    for (int i = 0; i < mu.MigrationChunks.Count; i++)
                    {
                        _cts.Token.ThrowIfCancellationRequested();

                        double initialPercent = ((double)100 / mu.MigrationChunks.Count) * i;
                        double contributionFactor = 1.0 / mu.MigrationChunks.Count;

                        // docCount is computed inside DumpChunkAsync now
                        if (!mu.MigrationChunks[i].IsDownloaded == true)
                        {
                            TaskResult result = await new RetryHelper().ExecuteTask(
                                () => DumpChunkAsync(mu, i, ctx.Collection, folder, ctx.SourceConnectionString, ctx.TargetConnectionString, initialPercent, contributionFactor, dbName, colName),
                                (ex, attemptCount, currentBackoff) => DumpChunk_ExceptionHandler(ex, attemptCount, "Dump Executor", dbName, colName, i, currentBackoff),
                                _log
                            );

                            if (result == TaskResult.Abort || result == TaskResult.FailedAfterRetries)
                            {
                                _log.WriteLine($"Dump operation for {dbName}.{colName}[{i}] failed after multiple attempts.", LogType.Error);
                                StopProcessing();

                                return result; // Abort the process
                            }
                        }
                        else
                        {
                            downloadCount += mu.MigrationChunks[i].DumpQueryDocCount;

                        }
                    }
                }

                if (!_cts.Token.IsCancellationRequested)
                {
                    mu.SourceCountDuringCopy = mu.MigrationChunks.Sum(chunk => chunk.DumpQueryDocCount);
                    downloadCount = mu.SourceCountDuringCopy; // recompute from chunks to avoid incremental tracking

                    mu.DumpGap = Helper.GetMigrationUnitDocCount(mu) - downloadCount;
                    mu.DumpPercent = 100;
                    mu.DumpComplete = true;

                    // BulkCopyEndedOn will be set after restore completes, not here
                }
            }
            else if (mu.DumpComplete && !mu.RestoreComplete && !_cts.Token.IsCancellationRequested)
            {
                _log.WriteLine($"{dbName}.{colName} added to upload queue");

                MigrationUnitsPendingUpload.AddOrUpdate($"{mu.DatabaseName}.{mu.CollectionName}", mu);
                _ = Task.Run(() => Upload(mu, ctx.TargetConnectionString), _cts.Token);
            }            

            return TaskResult.Success;
        }


        private void Upload(MigrationUnit mu, string targetConnectionString, bool force = false)
        {
            if (!force)
            {
                if (!TryEnterUploadLock())
                {
                    return; // Prevent concurrent uploads
                }
            }

            ProcessRunning=true;

            string dbName = mu.DatabaseName;
            string colName = mu.CollectionName;
            string jobId = _job.Id ?? string.Empty;
            string key = $"{mu.DatabaseName}.{mu.CollectionName}";
            string folder = GetDumpFolder(jobId, dbName, colName);

            _log.WriteLine($"{dbName}.{colName} upload started.");

            try
            {
                ProcessRestoreLoop(mu, folder, targetConnectionString, dbName, colName);

                if ((mu.RestoreComplete && mu.DumpComplete) || (mu.DumpComplete && _job.IsSimulatedRun))
                {
                    FinalizeUpload(mu, key, folder, targetConnectionString, jobId);
                }
            }
            finally
            {
                // Always release the upload lock if we acquired it
                try { _uploadLock.Release(); } catch { }
            }
        }


        // Builds the dump folder path for a db/collection under the current job
        private string GetDumpFolder(string jobId, string dbName, string colName)
            => $"{_mongoDumpOutputFolder}\\{jobId}\\{Helper.SafeFileName($"{dbName}.{colName}")}";

        // Core restore loop: iterates until all chunks are restored or cancellation/simulation stops it
        private void ProcessRestoreLoop(MigrationUnit mu, string folder, string targetConnectionString, string dbName, string colName)
        {

            while (ShouldContinueUploadLoop(mu, folder))
            {
                // MongoRestore
                if (!mu.RestoreComplete && !_cts.Token.IsCancellationRequested && Helper.IsMigrationUnitValid(mu))
                {
                                        
                    int restoredChunks;
                    long restoredDocs;
                    RestoreAllPendingChunksOnce(mu, folder, targetConnectionString, dbName, colName, out restoredChunks, out restoredDocs);

                    if (restoredChunks == mu.MigrationChunks.Count && !_cts.Token.IsCancellationRequested)
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
                        _jobList.Save(); // Persist state
                    }
                    else
                    {
                        // If there are no pending chunks to restore, exit the loop instead of sleeping
                        if (!HasPendingChunks(mu))
                        {
                            return;
                        }

                        try
                        {
                            Task.Delay(10000, _cts.Token).Wait(_cts.Token);
                        }
                        catch (OperationCanceledException)
                        {
                            return; // Exit if cancellation was requested during delay
                        }
                    }
                }
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
            => !mu.RestoreComplete && Directory.Exists(folder) && !_cts.Token.IsCancellationRequested && !_job.IsSimulatedRun;

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

            // Use parallel restore if enabled and multiple instances configured
            if (_maxParallelRestoreInstances > 1)
            {
                _log.WriteLine($"Using parallel restore with {_maxParallelRestoreInstances} instances");
                
                var result = ParallelRestoreChunksAsync(
                    mu,
                    folder,
                    targetConnectionString,
                    dbName,
                    colName
                ).GetAwaiter().GetResult();
                
                restoredChunks = result.RestoredChunks;
                restoredDocs = result.RestoredDocs;
                
                if (result.Result == TaskResult.Abort || result.Result == TaskResult.FailedAfterRetries)
                {
                    _log.WriteLine($"Parallel restore operation for {dbName}.{colName} failed after multiple attempts.", LogType.Error);
                    StopProcessing();
                    return;
                }
                
                return;
            }
            
            // Sequential restore (original logic)
            _log.WriteLine($"Using sequential restore");

            for (int i = 0; i < mu.MigrationChunks.Count; i++)
            {
                _cts.Token.ThrowIfCancellationRequested();

                if (!mu.MigrationChunks[i].IsUploaded == true && mu.MigrationChunks[i].IsDownloaded == true)
                {
                    double initialPercent = ((double)100 / mu.MigrationChunks.Count) * i;
                    double contributionFactor = (double)mu.MigrationChunks[i].DumpQueryDocCount / Helper.GetMigrationUnitDocCount(mu);
                    if (mu.MigrationChunks.Count == 1) contributionFactor = 1;

                    _log.WriteLine($"{dbName}.{colName}[{i}] uploader processing");

                    var restoreResult = new RetryHelper()
                        .ExecuteTask(
                            () => RestoreChunkAsync(mu, i, folder, targetConnectionString, initialPercent, contributionFactor, dbName, colName),
                            (ex, attemptCount, currentBackoff) => RestoreChunk_ExceptionHandler(ex, attemptCount, "Restore Executor", dbName, colName, i, currentBackoff),
                            _log
                        )
                        .GetAwaiter().GetResult();

                    if (restoreResult == TaskResult.Abort || restoreResult == TaskResult.FailedAfterRetries)
                    {
                        _log.WriteLine($"Restore operation for {dbName}.{colName}[{i}] failed after multiple attempts.", LogType.Error);
                        StopProcessing();
                        return;
                    }

                    if (restoreResult == TaskResult.Success)
                    {
                        restoredChunks++;
                        restoredDocs += Math.Max(mu.MigrationChunks[i].RestoredSuccessDocCount, mu.MigrationChunks[i].DocCountInTarget);
                    }
                }
                else if (mu.MigrationChunks[i].IsUploaded == true)
                {
                    restoredChunks++;
                    restoredDocs += mu.MigrationChunks[i].RestoredSuccessDocCount;
                }
            }
        }

        // Finalization after restore completes or simulated run concludes
        private void FinalizeUpload(MigrationUnit mu, string key, string folder, string targetConnectionString, string jobId)
        {
            

            // Best-effort cleanup of local dump folder
            try
            {
                if (Directory.Exists(folder))
                    Directory.Delete(folder, true);
            }
            catch { }

            // Start change stream immediately if configured
            AddCollectionToChangeStreamQueue(mu, targetConnectionString);

            // Remove from upload queue
            MigrationUnitsPendingUpload.Remove(key);

            // Process next pending upload if any
            if (MigrationUnitsPendingUpload.TryGetFirst(out var nextItem))
            {
                _log.WriteLine($"Processing {nextItem.Value.DatabaseName}.{nextItem.Value.CollectionName} from upload queue");
                Upload(nextItem.Value, targetConnectionString, true);
                return;
            }

            

            // Handle offline completion and post-upload CS logic
            if (!_cts.Token.IsCancellationRequested)
            {
                var migrationJob = _jobList.MigrationJobs?.Find(m => m.Id == jobId);
                if (migrationJob != null)
                {
                    if (!Helper.IsOnline(_job) && Helper.IsOfflineJobCompleted(migrationJob))
                    {                        
                        _log.WriteLine($"Job {migrationJob.Id} Completed");
                        migrationJob.IsCompleted = true;
                        StopProcessing();
                    }
                    else
                    {
                        RunChangeStreamProcessorForAllCollections(targetConnectionString);
                    }
                }
            }
        }
        public new void StopProcessing(bool updateStatus = true)
        {
            _log.WriteLine("Stopping DumpRestoreProcessor...");
            
            // Kill all active processes first
            KillAllActiveProcesses();
            
            // Cancel and dispose blocker tasks
            _dumpBlockerCts?.Cancel();
            _dumpBlockerCts?.Dispose();
            _dumpBlockerCts = null;
            
            _restoreBlockerCts?.Cancel();
            _restoreBlockerCts?.Dispose();
            _restoreBlockerCts = null;
            
            // Dispose semaphores
            _dumpSemaphore?.Dispose();
            _restoreSemaphore?.Dispose();
            _dumpSemaphore = null;
            _restoreSemaphore = null;
            
            // Clear queues
            while (_dumpQueue.TryDequeue(out _)) { }
            while (_restoreQueue.TryDequeue(out _)) { }
            
            // Clear error tracking
            while (_chunkErrors.TryTake(out _)) { }
            
            try
            {
                _uploadLock.Release(); // reset the flag
            }
            catch
            {
                // Do nothing, just reset the flag
            }
            
            // Call base implementation
            base.StopProcessing(updateStatus);
            
            _log.WriteLine("DumpRestoreProcessor stopped");
        }
    }
}
