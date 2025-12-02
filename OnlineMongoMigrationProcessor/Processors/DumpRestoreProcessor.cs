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

        //private string _toolsLaunchFolder = string.Empty;
        private string _mongoDumpOutputFolder = Path.Combine(Helper.GetWorkingFolder(), "mongodump");
        private static readonly SemaphoreSlim _uploadLock = new(1, 1);

        //private SafeFifoCollection<string, MigrationUnit> MigrationUnitsPendingUpload = new SafeFifoCollection<string, MigrationUnit>();

        private OrderedUniqueList<string> _migrationUnitsPendingUpload = new OrderedUniqueList<string>();

        // Parallel processing infrastructure
        private WorkerPoolManager? _dumpPool;
        private WorkerPoolManager? _restorePool;
        
        // Thread-safe locks
        private readonly object _pidLock = new object();
        private readonly object _progressLock = new object();
        private readonly object _chunkUpdateLock = new object();
        
        // Error tracking
        private readonly ConcurrentBag<(int ChunkIndex, Exception Error)> _chunkErrors = new();
        
        // Chunk work queues
        private readonly ConcurrentQueue<ChunkWorkItem> _dumpQueue = new();
        private readonly ConcurrentQueue<ChunkWorkItem> _restoreQueue = new();
        
        // Active worker tracking
        private readonly List<Task<TaskResult>> _activeDumpWorkers = new();
        private readonly List<Task<TaskResult>> _activeRestoreWorkers = new();
        private readonly object _workerLock = new object();
        
        // Worker context for dynamic spawning (shared between dump and restore)
        private IMongoCollection<BsonDocument>? _currentCollection;
        private string? _currentFolder;
        private MigrationUnit? _currentMigrationUnit;  // Contains DatabaseName and CollectionName
        private int _nextDumpWorkerId = 1;
        private int _nextRestoreWorkerId = 1;
        
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

        public DumpRestoreProcessor(Log log,  ActiveMigrationUnitsCache muCache, MongoClient sourceClient, MigrationSettings config)
            : base(log, muCache, sourceClient, config)
        {
            // Calculate optimal concurrency
            int maxDumpWorkers, maxRestoreWorkers;
            if (CurrentlyActiveJob.EnableParallelProcessing)
            {
                maxDumpWorkers = CalculateOptimalConcurrency(
                    CurrentlyActiveJob.MaxParallelDumpProcesses,
                    isDump: true
                );
                
                maxRestoreWorkers = CalculateOptimalConcurrency(
                    CurrentlyActiveJob.MaxParallelRestoreProcesses,
                    isDump: false
                );
            }
            else
            {
                maxDumpWorkers = 1;
                maxRestoreWorkers = 1;
            }
            
            // Initialize worker pools
            _dumpPool = new WorkerPoolManager(_log, "Dump", maxDumpWorkers);
            _restorePool = new WorkerPoolManager(_log, "Restore", maxRestoreWorkers);
            
            // Store initial values in CurrentlyActiveJob for UI monitoring
            CurrentlyActiveJob.CurrentDumpWorkers = maxDumpWorkers;
            CurrentlyActiveJob.CurrentRestoreWorkers = maxRestoreWorkers;
            
            
            if (!CurrentlyActiveJob.MaxInsertionWorkersPerCollection.HasValue)
            {
                CurrentlyActiveJob.CurrentInsertionWorkers = Math.Min(Environment.ProcessorCount / 2, 8);
            }
            else
            {
                CurrentlyActiveJob.CurrentInsertionWorkers = CurrentlyActiveJob.MaxInsertionWorkersPerCollection.Value;
            }
            
            _log.WriteLine($"Parallel processing: Dump workers={maxDumpWorkers}, Restore workers={maxRestoreWorkers}");
        }

        /// <summary>
        /// Adjusts the number of dump workers at runtime. Increase adds workers, decrease reduces capacity.
        /// </summary>
        public void AdjustDumpWorkers(int newCount)
        {
            if (newCount < 1) newCount = 1;
            if (newCount > 16) newCount = 16; // Safety limit
            
            int difference = _dumpPool!.AdjustPoolSize(newCount);
            
            if (difference == 0) return;
            
            CurrentlyActiveJob.CurrentDumpWorkers = newCount;
            CurrentlyActiveJob.MaxParallelDumpProcesses = newCount; // Update config too
            
            // Spawn new worker tasks if capacity increased and processing is active
            if (difference > 0 && _currentCollection != null && !_dumpQueue.IsEmpty)
            {
                lock (_workerLock)
                {
                    _log.WriteLine($"Spawning {difference} additional dump workers to process queue");
                    
                    for (int i = 0; i < difference; i++)
                    {
                        int workerId = _nextDumpWorkerId++;
                        var workerTask = Task.Run(async () =>
                        {
                            return await DumpWorkerAsync(
                                workerId,
                                _currentCollection!,
                                _currentFolder!,
                                MigrationJobContext.SourceConnectionString[CurrentlyActiveJob.Id],
                                MigrationJobContext.TargetConnectionString[CurrentlyActiveJob.Id],
                                _currentMigrationUnit!.DatabaseName,
                                _currentMigrationUnit!.CollectionName
                            );
                        }, _cts.Token);
                        
                        _activeDumpWorkers.Add(workerTask);
                    }
                }
            }
            MigrationJobContext.SaveMigrationJob(CurrentlyActiveJob);
        }

        /// <summary>
        /// Adjusts the number of restore workers at runtime. Increase adds workers, decrease reduces capacity.
        /// </summary>
        public void AdjustRestoreWorkers(int newCount)
        {
            if (newCount < 1) newCount = 1;
            if (newCount > 16) newCount = 16; // Safety limit
            
            int difference = _restorePool!.AdjustPoolSize(newCount);
            
            if (difference == 0) return;
            
            CurrentlyActiveJob.CurrentRestoreWorkers = newCount;
            CurrentlyActiveJob.MaxParallelRestoreProcesses = newCount; // Update config too
            
            // Spawn new worker tasks if capacity increased and processing is active
            if (difference > 0 && _currentFolder != null && !_restoreQueue.IsEmpty)
            {
                lock (_workerLock)
                {
                    _log.WriteLine($"Spawning {difference} additional restore workers to process queue");
                    
                    for (int i = 0; i < difference; i++)
                    {
                        int workerId = _nextRestoreWorkerId++;
                        var workerTask = Task.Run(async () =>
                        {
                            return await RestoreWorkerAsync(
                                workerId,
                                _restoreQueue,
                                _currentFolder!,
                                MigrationJobContext.TargetConnectionString[CurrentlyActiveJob.Id],
                                _currentMigrationUnit!.DatabaseName,
                                _currentMigrationUnit!.CollectionName
                            );
                        }, _cts.Token);
                        
                        _activeRestoreWorkers.Add(workerTask);
                    }
                }
            }

            MigrationJobContext.SaveMigrationJob(CurrentlyActiveJob);
        }

        /// <summary>
        /// Adjusts the number of insertion workers per collection for mongorestore at runtime.
        /// </summary>
        public void AdjustInsertionWorkers(int newCount)
        {
            if (newCount < 1) newCount = 1;
            if (newCount > 16) newCount = 16; // Safety limit
            
            CurrentlyActiveJob.CurrentInsertionWorkers = newCount;
            CurrentlyActiveJob.MaxInsertionWorkersPerCollection = newCount;
            
            _log.WriteLine($"Set insertion workers per collection to {newCount}. Will apply to new restore operations.");
            
            
            MigrationJobContext.SaveMigrationJob(CurrentlyActiveJob);
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
                if (!MigrationJobContext.ActiveDumpProcessIds.Contains(pid))
                {
                    MigrationJobContext.ActiveDumpProcessIds.Add(pid);
                    _log.WriteLine($"Registered dump process: PID {pid} (total active: {MigrationJobContext.ActiveDumpProcessIds.Count})");
                    SaveJobListDebounced();
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
                    SaveJobListDebounced();
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
                    SaveJobListDebounced();
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
                //_jobList.Save();
                MigrationJobContext.SaveMigrationJob(CurrentlyActiveJob);
                _lastSave = DateTime.UtcNow;
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
                MigrationJobContext.SaveMigrationJob(CurrentlyActiveJob);
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
            _nextDumpWorkerId = 1;
            
            // Enqueue all chunks
            foreach (var chunk in sortedChunks)
            {
                _dumpQueue.Enqueue(chunk);
            }
            
            // Start worker tasks
            Task<TaskResult>[] workerTaskArray;
            lock (_workerLock)
            {
                _activeDumpWorkers.Clear();
                for (int i = 0; i < Math.Min(_dumpPool.MaxWorkers, sortedChunks.Count); i++)
                {
                    int workerId = _nextDumpWorkerId++;
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
                    
                    _activeDumpWorkers.Add(workerTask);
                }
                workerTaskArray = _activeDumpWorkers.ToArray();
            }
            
            // Wait for all workers to complete
            var results = await Task.WhenAll(workerTaskArray);
            
            // Clear context
            _currentCollection = null;
            
            // Check for controlled pause - workers have finished, now stop
            if (_controlledPauseRequested)
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
            _log.WriteLine($"Dump worker {workerId} started");
            
            while (true)
            {
                // Check for controlled pause first
                if (_controlledPauseRequested)
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
                    if (_controlledPauseRequested)
                    {
                        _log.WriteLine($"Dump worker {workerId}: Controlled pause active - returning at chunk {workItem.ChunkIndex}", LogType.Debug);
                        break;
                    }

                    _log.WriteLine($"Worker {workerId} processing chunk {workItem.ChunkIndex}", LogType.Debug);

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
            
            _log.WriteLine($"Dump worker {workerId} completed");
            
            // Check if controlled pause completed
            if (_controlledPauseRequested && _dumpQueue.IsEmpty)
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

                // Check if all chunks are already uploaded (not just that there are no pending chunks)
                bool allChunksUploaded = mu.MigrationChunks.All(c => c.IsUploaded == true);

                // Check if mu.DumpComplete == true but some chunks have IsDownloaded = false
                if(mu.DumpComplete && mu.MigrationChunks.Any(c => c.IsDownloaded != true))
                {
                    _log.WriteLine($"{mu.DatabaseName}.{mu.CollectionName} marked as downloaded but has chunks not yet downloaded. Resetting it as not downloaded and requesting controlled pause.", LogType.Warning);
                    mu.DumpComplete = false;

                    MigrationJobContext.SaveMigrationUnit(mu, true);
                    MigrationJobContext.SaveMigrationJob(CurrentlyActiveJob);
          
                    return new RestoreResult
                    {
                        Result = TaskResult.Canceled,
                        RestoredChunks = restoredChunks,
                        RestoredDocs = restoredDocs
                    };
                }


                if (!sortedChunks.Any() && allChunksUploaded)
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
                        MigrationJobContext.SaveMigrationJob(CurrentlyActiveJob);

                        _controlledPauseRequested=true;

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
                _nextRestoreWorkerId = 1;

                // Clear chunk errors before starting
                _chunkErrors.Clear();

                // Enqueue restore work to global queue
                foreach (var chunk in sortedChunks)
                {
                    _restoreQueue.Enqueue(chunk);
                }

                // Start worker tasks
                Task<TaskResult>[] workerTaskArray;
                lock (_workerLock)
                {
                    _activeRestoreWorkers.Clear();
                    for (int i = 0; i < Math.Min(_restorePool.MaxWorkers, sortedChunks.Count); i++)
                    {
                        int workerId = _nextRestoreWorkerId++;
                        var workerTask = Task.Run(async () =>
                        {
                            return await RestoreWorkerAsync(
                                workerId,
                                _restoreQueue,
                                folder,
                                targetConnectionString,
                                dbName,
                                colName
                            );
                        }, _cts.Token);

                        _activeRestoreWorkers.Add(workerTask);
                    }
                    workerTaskArray = _activeRestoreWorkers.ToArray();
                }

                // Wait for all workers to complete
                var results = await Task.WhenAll(workerTaskArray);

                // Clear context
                _currentFolder = null;

                // Check for controlled pause - workers have finished, now stop
                if (_controlledPauseRequested)
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
                if (_controlledPauseRequested)
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
                    if (_controlledPauseRequested)
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
            if (_controlledPauseRequested && restoreQueue.IsEmpty)
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

            // Build base args per attempt
            string args = $" --uri=\"{sourceConnectionString}\" --gzip --db={dbName} --collection=\"{colName}\" --archive";//  --out {Path.Combine(folder,$"{chunkIndex}.bson")}";

            // Disk space/backpressure check (retain existing behavior)
            bool continueDownloads;
            double pendingUploadsGB = 0;
            double freeSpaceGB = 0;
            while (true)
            {
                // Check for controlled pause
                if (_controlledPauseRequested)
                {
                    _log.WriteLine("Controlled pause detected - exiting dump");
                    return Task.FromResult(TaskResult.Canceled);
                }
                
                continueDownloads = Helper.CanProceedWithDownloads(folder, _config.ChunkSizeInMb * 2, out pendingUploadsGB, out freeSpaceGB);
                if (!continueDownloads)
                {
                    _log.WriteLine($"{dbName}.{colName} added to upload queue");
                    //MigrationUnitsPendingUpload.AddOrUpdate($"{mu.DatabaseName}.{mu.CollectionName}", mu);
                    _migrationUnitsPendingUpload.Add(mu.Id);
                    _ = Task.Run(() => Upload(mu.Id, targetConnectionString), _cts.Token);

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
            var dumpFilePath = $"{Path.Combine(folder, $"{chunkIndex}.bson")}";
            if (File.Exists(dumpFilePath))
            {
                try { File.Delete(dumpFilePath); } catch { }
            }

            try
            {
                // Create dedicated executor for this worker to avoid shared state issues
                var processExecutor = new ProcessExecutor(_log,_muCache);

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
                    //MigrationUnitsPendingUpload.AddOrUpdate($"{mu.DatabaseName}.{mu.CollectionName}", mu);
                    _migrationUnitsPendingUpload.Add(mu.Id);
                    Task.Run(() => Upload(mu.Id, targetConnectionString), _cts.Token);
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

            // Handle simulation mode - simulate successful restore immediately
            if (CurrentlyActiveJob.IsSimulatedRun)
            {
                // Simulate successful restore
                mu.MigrationChunks[chunkIndex].RestoredSuccessDocCount = mu.MigrationChunks[chunkIndex].DumpQueryDocCount;
                mu.MigrationChunks[chunkIndex].RestoredFailedDocCount = 0;
                mu.MigrationChunks[chunkIndex].IsUploaded = true;
                
                // Update progress
                double progress = initialPercent + (contributionFactor * 100);
                mu.RestorePercent = Math.Min(progress, 100);
                
                _log.WriteLine($"Simulation mode: Chunk {chunkIndex} restore simulated - {mu.RestorePercent:F2}% complete (RestorePercent={mu.RestorePercent})");

                MigrationJobContext.SaveMigrationUnit(mu,false);

                // Small delay to simulate processing time (50ms per chunk)
                try { Task.Delay(50, _cts.Token).Wait(_cts.Token); } catch { }
                
                return Task.FromResult(TaskResult.Success);
            }

            // Build args per attempt
            string args = $" --uri=\"{targetConnectionString}\" --gzip --archive";// {Path.Combine(folder, $"{chunkIndex}.bson")}";

            // If first mu, drop collection, else append. Also No drop in AppendMode
            if (chunkIndex == 0 && !CurrentlyActiveJob.AppendMode)
            {
                args = $"{args} --drop";
                if (CurrentlyActiveJob.SkipIndexes)
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
            
            if (CurrentlyActiveJob.MaxInsertionWorkersPerCollection.HasValue)
            {
                // Use configured value
                insertionWorkers = CurrentlyActiveJob.MaxInsertionWorkersPerCollection.Value;
            }
            else
            {
                // Use the calculated default from constructor
                insertionWorkers = CurrentlyActiveJob.CurrentInsertionWorkers;
            }
            
            _log.WriteLine($"Restore will use {insertionWorkers} insertion worker(s) for {dbName}.{colName}[{chunkIndex}] ({docCount} docs)");
            
            if (insertionWorkers > 1)
            {
                args = $"{args} --numInsertionWorkersPerCollection={insertionWorkers}";
            }

            try
            {
                // Create dedicated executor for this worker to avoid shared state issues
                var processExecutor = new ProcessExecutor(_log,_muCache);
                var dumpFilePath = $"{Path.Combine(folder, $"{chunkIndex}.bson")}";

                if (!File.Exists(dumpFilePath)) //data integrity check,force controlled pause if dump file missing
                {
                    _log.WriteLine($"Restore file for {dbName}.{colName}[{chunkIndex}] not found at {dumpFilePath}, Controlled pause requested.", LogType.Error);
                    mu.DumpComplete = false;
                    MigrationJobContext.SaveMigrationUnit(mu, false);
                    MigrationJobContext.SaveMigrationJob(CurrentlyActiveJob);
                    _controlledPauseRequested = true;
                    return Task.FromResult(TaskResult.Canceled);
                }

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
                    onProcessEnded: (pid) => UnregisterRestoreProcess(pid)
                ), _cts.Token);
                task.Wait(_cts.Token);
                bool result = task.Result;                             

                if (result)
                {
                    bool skipFinalize = false;

                    if (mu.MigrationChunks[chunkIndex].RestoredFailedDocCount > 0)
                    {
                        if (_targetClient == null && !CurrentlyActiveJob.IsSimulatedRun)
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

                            MigrationJobContext.SaveMigrationUnit(mu,false);
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
                        MigrationJobContext.SaveMigrationUnit(mu,false);

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
            string jobId;
            string dbName;
            string colName;
            ProcessorContext ctx;
            MigrationUnit mu;
            try
            {
                _controlledPauseRequested = false;
                ProcessRunning = true;
                mu = _muCache.GetMigrationUnit(migrationUnitId);
                mu.ParentJob = CurrentlyActiveJob;

                // Initialize processor context (parity with CopyProcessor)
                ctx = SetProcessorContext(mu, sourceConnectionString, targetConnectionString);

                jobId = ctx.JobId;
                dbName = ctx.DatabaseName;
                colName = ctx.CollectionName;
            }
            catch (Exception ex)
            {
                _log.WriteLine($"Error starting process for migration unit {migrationUnitId},Ex: {Helper.RedactPii(ex.ToString())}", LogType.Error);
                return TaskResult.Retry;
            }

            
            // when resuming a CurrentlyActiveJob, check if post-upload change stream processing is already in progress
            if (CheckChangeStreamAlreadyProcessingAsync(ctx))
                return TaskResult.Success;

            // starting the regular dump and restore process
            if (!mu.BulkCopyStartedOn.HasValue || mu.BulkCopyStartedOn == DateTime.MinValue)
                mu.BulkCopyStartedOn = DateTime.UtcNow;

            //check if dump complete but restore not complete  yet all chunks are downloaded
            if (mu.DumpComplete && !mu.RestoreComplete && mu.MigrationChunks.All(c => c.IsDownloaded == true))
            {
                _log.WriteLine($"{dbName}.{colName} dump already complete, but chunks are still being processed. Will reprocess the chunks.", LogType.Warning);
                mu.DumpComplete = false;
            }

            //check to make sure chunk files are present, else mark chunk as not downloaded
            if (mu.DumpComplete && !mu.RestoreComplete)
            {                 
                for (int i=0;i< mu.MigrationChunks.Count;i++)
                {
                    string chunkFilePath = Path.Combine(_mongoDumpOutputFolder, jobId, Helper.SafeFileName($"{dbName}.{colName}"), $"{i}.bson");
                    if (!File.Exists(chunkFilePath))
                    {
                        _log.WriteLine($"Chunk file missing for {dbName}.{colName}[{i}] at {chunkFilePath}, marking chunk as not downloaded.", LogType.Warning);
                        mu.MigrationChunks[i].IsDownloaded = false;
                    }
                }
                mu.DumpComplete = false;
            }

            // check if  dump complete and process chunks as needed
            if (!mu.DumpComplete && !_cts.Token.IsCancellationRequested)
            {
                _log.WriteLine($"DumpFolder is {_mongoDumpOutputFolder}, working directory  is {Helper.GetWorkingFolder()}", LogType.Verbose);
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
                            MigrationJobContext.SaveMigrationUnit(mu,false);
                            _log.WriteLine($"{dbName}.{colName} actual document count: {task.Result}");
                        }
                    }, _cts.Token, TaskContinuationOptions.OnlyOnRanToCompletion, TaskScheduler.Default);
                }

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
                    mu.SourceCountDuringCopy = mu.MigrationChunks.Sum(chunk => chunk.DumpQueryDocCount);
                    downloadCount = mu.SourceCountDuringCopy; // recompute from chunks to avoid incremental tracking

                    mu.DumpGap = Helper.GetMigrationUnitDocCount(mu) - downloadCount;
                    mu.DumpPercent = 100;
                    mu.DumpComplete = true;
                                        
                    MigrationJobContext.SaveMigrationUnit(mu,true);

                    // BulkCopyEndedOn will be set after restore completes, not here

                    // Only trigger restore if not paused
                    if (!_controlledPauseRequested)
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
            }

            // Don't restart processing if controlled pause was requested
            if (_controlledPauseRequested)
            {
                _log.WriteLine($"Upload skipped for {migrationUnitId} - controlled pause is active",LogType.Debug);
                try { _uploadLock.Release(); } catch { }
                return;
            }

            ProcessRunning=true;
            var mu = _muCache.GetMigrationUnit(migrationUnitId);
            mu.ParentJob = CurrentlyActiveJob;
            string dbName = mu.DatabaseName;
            string colName = mu.CollectionName;
            string jobId = CurrentlyActiveJob.Id ?? string.Empty;
            string key = $"{mu.DatabaseName}.{mu.CollectionName}";
            string folder = GetDumpFolder(jobId, dbName, colName);

            _log.WriteLine($"Processing {dbName}.{colName} from upload queue");
 
            try
            {
                ProcessRestoreLoop(mu, folder, targetConnectionString, dbName, colName);

                if ((mu.RestoreComplete && mu.DumpComplete) || (mu.DumpComplete && CurrentlyActiveJob.IsSimulatedRun))
                {
                    FinalizeUpload(mu.Id, key, folder, targetConnectionString, jobId);
                }
            }
            finally
            {
                // Always release the upload lock if we acquired it
                try { _uploadLock.Release(); } catch { }
            }
        }


        // Builds the dump folder path for a db/collection under the current CurrentlyActiveJob
        private string GetDumpFolder(string jobId, string dbName, string colName)
            => Path.Combine(_mongoDumpOutputFolder, jobId, Helper.SafeFileName($"{dbName}.{colName}"));

        // Core restore loop: iterates until all chunks are restored or cancellation/simulation stops it
        private void ProcessRestoreLoop(MigrationUnit mu, string folder, string targetConnectionString, string dbName, string colName)
        {
            // Handle simulation mode - use parallel restore infrastructure for consistency
            if (CurrentlyActiveJob.IsSimulatedRun && !mu.RestoreComplete && mu.DumpComplete)
            {
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
                    folder,///////
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


                MigrationJobContext.SaveMigrationUnit(mu,true);
                _muCache.RemoveMigrationUnit(mu.Id);
                _log.WriteLine($"Simulation mode: Restore completed for {dbName}.{colName} - Final RestorePercent={mu.RestorePercent}%");
                return;
            }

            if (mu.DumpComplete && !mu.RestoreComplete && !Directory.Exists(folder)) //data missing issue, forcing controlled pause
            {
                _log.WriteLine($"Dump folder not found for {mu.DatabaseName}.{mu.CollectionName}.", LogType.Info);
                mu.DumpComplete = false;
                MigrationJobContext.SaveMigrationUnit(mu, true);
                _muCache.RemoveMigrationUnit(mu.Id);
                MigrationJobContext.SaveMigrationJob(CurrentlyActiveJob); // Persist state

                _controlledPauseRequested = true;
                _log.WriteLine($"Dump folder not found for {mu.DatabaseName}.{mu.CollectionName}. Controlled pause requested. Please resume job.",LogType.Error);
                return;
            }

            while (ShouldContinueUploadLoop(mu, folder))
            {
                // Check for controlled pause at start of loop iteration
                if (_controlledPauseRequested)
                {
                    _log.WriteLine($"Controlled pause detected in restore loop for {dbName}.{colName} - exiting",LogType.Debug);
                    return;
                }
                
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

                        MigrationJobContext.SaveMigrationUnit(mu,true);
                        _muCache.RemoveMigrationUnit(mu.Id);
                        MigrationJobContext.SaveMigrationJob(CurrentlyActiveJob); // Persist state
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
                            _log.WriteLine($"Restore loop for {dbName}.{colName} sleeping for 10 seconds before next pass...");
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
        { 
            _log.WriteLine($"Checking if restore should continue for {mu.DatabaseName}.{mu.CollectionName}: RestoreComplete={mu.RestoreComplete}, DumpFolderExists={Directory.Exists(folder)}, CancellationRequested={_cts.Token.IsCancellationRequested}, IsSimulatedRun={CurrentlyActiveJob.IsSimulatedRun}", LogType.Verbose);
            return !mu.RestoreComplete && Directory.Exists(folder) && !_cts.Token.IsCancellationRequested && !CurrentlyActiveJob.IsSimulatedRun;
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
            

            // Best-effort cleanup of local dump folder
            try
            {
                if (Directory.Exists(folder))
                    Directory.Delete(folder, true);
            }
            catch { }

            if (_controlledPauseRequested)
                return;

            // Start change stream immediately if configured
            AddCollectionToChangeStreamQueue(migrationunitId, targetConnectionString);

            // Remove from upload queue
            _migrationUnitsPendingUpload.Remove(migrationunitId);

            // Process next pending upload if any
            if (_migrationUnitsPendingUpload.Count>0)
            {
                if (_controlledPauseRequested)
                    return;

                Upload(_migrationUnitsPendingUpload[0], targetConnectionString, true);
                return;
            }

            

            // Handle offline completion and post-upload CS logic
            if (!_cts.Token.IsCancellationRequested)
            {                
                if (!Helper.IsOnline(CurrentlyActiveJob) && Helper.IsOfflineJobCompleted(CurrentlyActiveJob))
                {
                    // Don't mark as completed if this is a controlled pause
                    if (!_controlledPauseRequested)
                    {
                        _log.WriteLine($"Job {CurrentlyActiveJob.Id} Completed");
                        CurrentlyActiveJob.IsCompleted = true;
                        MigrationJobContext.SaveMigrationJob(CurrentlyActiveJob);
                    }                        
                        
                    StopProcessing();
                }
                else
                {
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
            if (!_controlledPauseRequested)
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
