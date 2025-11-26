using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace OnlineMongoMigrationProcessor.Helpers.JobManagement
{
    /// <summary>
    /// Manages a pool of workers with dynamic scaling capabilities.
    /// Handles semaphore-based concurrency control and worker lifecycle.
    /// </summary>
    internal class WorkerPoolManager
    {
        private readonly Log _log;
        private readonly string _poolName;
        private int _maxWorkers;
        private SemaphoreSlim? _semaphore;
        private CancellationTokenSource? _blockerCts;
        
        public int MaxWorkers => _maxWorkers;
        public int CurrentAvailable => _semaphore?.CurrentCount ?? 0;
        public int CurrentInUse => _maxWorkers - CurrentAvailable;

        public WorkerPoolManager(Log log, string poolName, int initialMaxWorkers)
        {
            _log = log ?? throw new ArgumentNullException(nameof(log));
            _poolName = poolName ?? throw new ArgumentNullException(nameof(poolName));
            _maxWorkers = Math.Max(1, initialMaxWorkers);
            _semaphore = new SemaphoreSlim(_maxWorkers, _maxWorkers);
        }

        /// <summary>
        /// Adjusts the worker pool size. Returns the difference (positive = increased, negative = decreased).
        /// </summary>
        /// <param name="newCount">New maximum worker count (will be clamped to 1-16)</param>
        /// <returns>Number of workers added (positive) or slots to block (negative)</returns>
        public int AdjustPoolSize(int newCount)
        {
            // Safety limits
            if (newCount < 1) newCount = 1;
            if (newCount > 16) newCount = 16;
            
            int oldCount = _maxWorkers;
            int difference = newCount - oldCount;
            
            if (difference == 0) return 0;
            
            _maxWorkers = newCount;
            
            if (difference > 0)
            {
                IncreaseCapacity(oldCount, newCount, difference);
            }
            else
            {
                DecreaseCapacity(newCount, difference);
            }
            
            return difference;
        }

        /// <summary>
        /// Acquires a semaphore slot asynchronously.
        /// </summary>
        public Task WaitAsync(CancellationToken cancellationToken)
        {
            return _semaphore!.WaitAsync(cancellationToken);
        }

        /// <summary>
        /// Releases a semaphore slot.
        /// </summary>
        /// <returns>True if released successfully, false if semaphore was already at max</returns>
        public bool TryRelease()
        {
            try
            {
                _semaphore?.Release();
                return true;
            }
            catch (SemaphoreFullException)
            {
                _log.WriteLine($"{_poolName} semaphore already at max capacity, skipping release", LogType.Debug);
                return false;
            }
        }

        /// <summary>
        /// Disposes resources.
        /// </summary>
        public void Dispose()
        {
            _blockerCts?.Cancel();
            _blockerCts?.Dispose();
            _blockerCts = null;
            
            _semaphore?.Dispose();
            _semaphore = null;
        }

        private void IncreaseCapacity(int oldCount, int newCount, int difference)
        {
            // Cancel any existing blocker task
            _blockerCts?.Cancel();
            _blockerCts?.Dispose();
            _blockerCts = null;
            
            // Calculate current usage with OLD max count
            int currentCount = _semaphore?.CurrentCount ?? 0;
            int inUse = oldCount - currentCount;
            
            // Dispose old semaphore
            _semaphore?.Dispose();
            
            // Create new semaphore with higher max capacity
            // Available slots = newCount - inUse
            _semaphore = new SemaphoreSlim(newCount - inUse, newCount);
            
            _log.WriteLine($"{_poolName}: Increased from {oldCount} to {newCount} (+{difference}). " +
                          $"Semaphore: max={newCount}, available={newCount - inUse}, in-use={inUse}");
        }

        private void DecreaseCapacity(int newCount, int difference)
        {
            // Cancel any previous blocker task
            _blockerCts?.Cancel();
            _blockerCts?.Dispose();
            _blockerCts = new CancellationTokenSource();
            
            var cts = _blockerCts;
            int slotsToBlock = Math.Abs(difference);
            
            // Spawn background task to consume semaphore slots
            Task.Run(async () =>
            {
                int acquiredCount = 0;
                try
                {
                    for (int i = 0; i < slotsToBlock; i++)
                    {
                        await _semaphore!.WaitAsync(cts.Token);
                        acquiredCount++;
                        _log.WriteLine($"{_poolName}: Consumed slot {acquiredCount}/{slotsToBlock} to enforce limit");
                    }
                }
                catch (OperationCanceledException)
                {
                    // Release any slots acquired before cancellation
                    for (int i = 0; i < acquiredCount; i++)
                    {
                        try { _semaphore?.Release(); } catch { }
                    }
                    _log.WriteLine($"{_poolName}: Blocker cancelled (limit increased), released {acquiredCount} slots");
                }
                catch (Exception ex)
                {
                    _log.WriteLine($"{_poolName}: Blocker error: {ex.Message}", LogType.Error);
                }
            }, cts.Token);
            
            _log.WriteLine($"{_poolName}: Decreased to {newCount} ({difference}). Active workers will finish current tasks.");
        }
    }
}
