using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using OnlineMongoMigrationProcessor.Context;
using OnlineMongoMigrationProcessor.Models;

namespace OnlineMongoMigrationProcessor.Helpers.JobManagement
{
    /// <summary>
    /// Centralized coordinator for managing shared worker pools across all DumpRestoreProcessor instances.
    /// This ensures consistent worker management for all collections in a migration job.
    /// </summary>
    internal static class WorkerPoolCoordinator
    {
        private static readonly object _lock = new object();
        
        // Shared worker pools per job
        private static readonly Dictionary<string, WorkerPoolManager> _dumpPools = new();
        private static readonly Dictionary<string, WorkerPoolManager> _restorePools = new();
        
        // Track active workers per job
        private static readonly Dictionary<string, List<WorkerReference>> _activeDumpWorkers = new();
        private static readonly Dictionary<string, List<WorkerReference>> _activeRestoreWorkers = new();
        
        /// <summary>
        /// Represents a reference to an active worker task
        /// </summary>
        private class WorkerReference
        {
            public string CollectionKey { get; set; } = string.Empty;
            public int WorkerId { get; set; }
            public DateTime StartedAt { get; set; }
            public bool IsCompleted { get; set; }
        }
        
        /// <summary>
        /// Gets or creates a dump worker pool for the specified job
        /// </summary>
        public static WorkerPoolManager GetOrCreateDumpPool(string jobId, Log log, int initialMaxWorkers)
        {
            lock (_lock)
            {
                if (!_dumpPools.ContainsKey(jobId))
                {
                    _dumpPools[jobId] = new WorkerPoolManager(log, $"Dump-{jobId}", initialMaxWorkers);
                    _activeDumpWorkers[jobId] = new List<WorkerReference>();
                    log.WriteLine($"Created shared dump pool for job {jobId} with {initialMaxWorkers} workers");
                }
                
                return _dumpPools[jobId];
            }
        }
        
        /// <summary>
        /// Gets or creates a restore worker pool for the specified job
        /// </summary>
        public static WorkerPoolManager GetOrCreateRestorePool(string jobId, Log log, int initialMaxWorkers)
        {
            lock (_lock)
            {
                if (!_restorePools.ContainsKey(jobId))
                {
                    _restorePools[jobId] = new WorkerPoolManager(log, $"Restore-{jobId}", initialMaxWorkers);
                    _activeRestoreWorkers[jobId] = new List<WorkerReference>();
                    log.WriteLine($"Created shared restore pool for job {jobId} with {initialMaxWorkers} workers");
                }
                
                return _restorePools[jobId];
            }
        }
        
        /// <summary>
        /// Registers a dump worker as active
        /// </summary>
        public static void RegisterDumpWorker(string jobId, string collectionKey, int workerId, Log log)
        {
            lock (_lock)
            {
                if (!_activeDumpWorkers.ContainsKey(jobId))
                    _activeDumpWorkers[jobId] = new List<WorkerReference>();
                    
                _activeDumpWorkers[jobId].Add(new WorkerReference
                {
                    CollectionKey = collectionKey,
                    WorkerId = workerId,
                    StartedAt = DateTime.UtcNow,
                    IsCompleted = false
                });
                
                int totalActive = _activeDumpWorkers[jobId].Count(w => !w.IsCompleted);
                log.WriteLine($"[Coordinator] Registered dump worker: {collectionKey} Worker#{workerId} (Total active: {totalActive})", LogType.Debug);
            }
        }
        
        /// <summary>
        /// Registers a restore worker as active
        /// </summary>
        public static void RegisterRestoreWorker(string jobId, string collectionKey, int workerId, Log log)
        {
            lock (_lock)
            {
                if (!_activeRestoreWorkers.ContainsKey(jobId))
                    _activeRestoreWorkers[jobId] = new List<WorkerReference>();
                    
                _activeRestoreWorkers[jobId].Add(new WorkerReference
                {
                    CollectionKey = collectionKey,
                    WorkerId = workerId,
                    StartedAt = DateTime.UtcNow,
                    IsCompleted = false
                });
                
                int totalActive = _activeRestoreWorkers[jobId].Count(w => !w.IsCompleted);
                log.WriteLine($"[Coordinator] Registered restore worker: {collectionKey} Worker#{workerId} (Total active: {totalActive})", LogType.Debug);
            }
        }
        
        /// <summary>
        /// Marks a dump worker as completed
        /// </summary>
        public static void MarkDumpWorkerCompleted(string jobId, string collectionKey, int workerId, Log log)
        {
            lock (_lock)
            {
                if (_activeDumpWorkers.TryGetValue(jobId, out var workers))
                {
                    log.WriteLine($"[Coordinator] WARNING: Attempt to mark dump worker completed for job {jobId} but coordinator is tracking {_currentJobId}", LogType.Warning);
                    return;
                }
                
                var worker = _activeDumpWorkers.FirstOrDefault(w => w.WorkerId == workerId && w.CollectionKey == collectionKey);
                    if (worker != null)
                    {
                        worker.IsCompleted = true;
                        var duration = DateTime.UtcNow - worker.StartedAt;
                        int totalActive = workers.Count(w => !w.IsCompleted);
                        log.WriteLine($"[Coordinator] Completed dump worker: {collectionKey} Worker#{workerId} (Duration: {duration.TotalSeconds:F1}s, Remaining active: {totalActive})", LogType.Debug);
                    }
                    else
                    {
                        log.WriteLine($"[Coordinator] WARNING: Could not find dump worker to mark completed: {collectionKey} Worker#{workerId}", LogType.Warning);
                    }
                }
            }
        }
        
        /// <summary>
        /// Marks a restore worker as completed
        /// </summary>
        public static void MarkRestoreWorkerCompleted(string jobId, string collectionKey, int workerId, Log log)
        {
            lock (_lock)
            {
                if (_activeRestoreWorkers.TryGetValue(jobId, out var workers))
                {
                    log.WriteLine($"[Coordinator] WARNING: Attempt to mark dump worker completed for job {jobId} but coordinator is tracking {_currentJobId}", LogType.Warning);
                    return;
                }
                
                var worker = _activeDumpWorkers.FirstOrDefault(w => w.WorkerId == workerId && w.CollectionKey == collectionKey);
                    if (worker != null)
                    {
                        worker.IsCompleted = true;
                        var duration = DateTime.UtcNow - worker.StartedAt;
                        int totalActive = workers.Count(w => !w.IsCompleted);
                        log.WriteLine($"[Coordinator] Completed restore worker: {collectionKey} Worker#{workerId} (Duration: {duration.TotalSeconds:F1}s, Remaining active: {totalActive})", LogType.Debug);
                    }
                    else
                    {
                        log.WriteLine($"[Coordinator] WARNING: Could not find restore worker to mark completed: {collectionKey} Worker#{workerId}", LogType.Warning);
                    }
                }
            }
        }
        
        /// <summary>
        /// Gets the count of active (running) dump workers for a job
        /// </summary>
        public static int GetActiveDumpWorkerCount(string jobId, Log log = null)
        {
            lock (_lock)
            {
                if (!_activeDumpWorkers.TryGetValue(jobId, out var workers))
                    return 0;
                
                int beforeCleanup = workers.Count;
                // Clean up completed workers
                workers.RemoveAll(w => w.IsCompleted);
                int afterCleanup = workers.Count;
                
                if (log != null && beforeCleanup != afterCleanup)
                {
                    log.WriteLine($"[Coordinator] Cleaned up {beforeCleanup - afterCleanup} completed dump workers. Active: {afterCleanup}", LogType.Debug);
                }
                
                return workers.Count;
            }
        }
        
        /// <summary>
        /// Gets the count of active (running) restore workers for a job
        /// </summary>
        public static int GetActiveRestoreWorkerCount(string jobId, Log log = null)
        {
            lock (_lock)
            {
                if (!_activeRestoreWorkers.TryGetValue(jobId, out var workers))
                    return 0;
                
                int beforeCleanup = workers.Count;
                // Clean up completed workers
                workers.RemoveAll(w => w.IsCompleted);
                int afterCleanup = workers.Count;
                
                if (log != null && beforeCleanup != afterCleanup)
                {
                    log.WriteLine($"[Coordinator] Cleaned up {beforeCleanup - afterCleanup} completed restore workers. Active: {afterCleanup}", LogType.Debug);
                }
                
                return workers.Count;
            }
        }
        
        /// <summary>
        /// Adjusts the number of dump workers for a job
        /// </summary>
        public static int AdjustDumpWorkers(string jobId, int newCount, Log log)
        {
            lock (_lock)
            {
                if (!_dumpPools.TryGetValue(jobId, out var pool))
                {
                    log.WriteLine($"No dump pool found for job {jobId}", LogType.Warning);
                    return 0;
                }
                
                // Use the pool's actual active worker count (from semaphore state)
                int currentActiveCount = pool.CurrentActive;
                log.WriteLine($"[Coordinator] Adjusting dump workers from {currentActiveCount} to {newCount}", LogType.Debug);
                return WorkerCountHelper.AdjustDumpWorkers(newCount, currentActiveCount, pool, log);
            }
        }

        /// <summary>
        /// Adjusts the number of restore workers for a job
        /// </summary>
        public static int AdjustRestoreWorkers(string jobId, int newCount, Log log)
        {
            lock (_lock)
            {
                if (!_restorePools.TryGetValue(jobId, out var pool))
                {
                    log.WriteLine($"No restore pool found for job {jobId}", LogType.Warning);
                    return 0;
                }
                
                // Use the pool's actual active worker count (from semaphore state)
                int currentActiveCount = pool.CurrentActive;
                log.WriteLine($"[Coordinator] Adjusting restore workers from {currentActiveCount} to {newCount}", LogType.Debug);
                return WorkerCountHelper.AdjustRestoreWorkers(newCount, currentActiveCount, pool, log);
            }
        }        /// <summary>
        /// Cleans up resources for a completed job
        /// </summary>
        public static void CleanupJob(string jobId, Log log = null)
        {
            lock (_lock)
            {
                int dumpWorkers = _activeDumpWorkers.ContainsKey(jobId) ? _activeDumpWorkers[jobId].Count : 0;
                int restoreWorkers = _activeRestoreWorkers.ContainsKey(jobId) ? _activeRestoreWorkers[jobId].Count : 0;
                
                if (_dumpPools.TryGetValue(jobId, out var dumpPool))
                {
                    dumpPool.Dispose();
                    _dumpPools.Remove(jobId);
                }
                
                if (_restorePools.TryGetValue(jobId, out var restorePool))
                {
                    restorePool.Dispose();
                    _restorePools.Remove(jobId);
                }
                
                _activeDumpWorkers.Remove(jobId);
                _activeRestoreWorkers.Remove(jobId);
                
                log?.WriteLine($"[Coordinator] Cleaned up job {jobId} (Had {dumpWorkers} dump workers, {restoreWorkers} restore workers)", LogType.Debug);
            }
        }
        
        /// <summary>
        /// Gets statistics for a job's worker pools
        /// </summary>
        public static (int activeDump, int activeRestore, int maxDump, int maxRestore) GetJobStats(string jobId)
        {
            lock (_lock)
            {
                int activeDump = GetActiveDumpWorkerCount(jobId);
                int activeRestore = GetActiveRestoreWorkerCount(jobId);
                int maxDump = _dumpPools.TryGetValue(jobId, out var dPool) ? dPool.MaxWorkers : 0;
                int maxRestore = _restorePools.TryGetValue(jobId, out var rPool) ? rPool.MaxWorkers : 0;
                
                return (activeDump, activeRestore, maxDump, maxRestore);
            }
        }
    }
}
