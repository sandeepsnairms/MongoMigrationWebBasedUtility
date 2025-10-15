using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace OnlineMongoMigrationProcessor.Helpers
{
    /// <summary>
    /// Manages queue-based processing for change stream events with adaptive batch sizing and memory awareness.
    /// Shared between ServerLevel and CollectionLevel change stream processors.
    /// </summary>
    public class ChangeStreamQueueManager
    {
        // Queue configuration
        public const int MaxQueueSize = 50000; // Maximum items per queue
        public const int MinBatchSize = 50;    // Minimum batch size for processing
        public const int MaxBatchSize = 1000;  // Maximum batch size for processing
        public const int OptimalBatchSize = 1000; // Optimal batch size per processing cycle

        // Adaptive batch sizing
        private volatile int _adaptiveBatchSize;
        private readonly object _batchSizeLock = new object();

        // Memory monitoring
        private DateTime _lastMemoryCheck = DateTime.UtcNow;
        private const int MemoryCheckIntervalSeconds = 30;
        private long _totalSystemMemory = 0;

        // Processing throttle
        private readonly SemaphoreSlim _processingThrottle;
        private readonly int _maxConcurrentOperations;

        // Logging
        private readonly Log _log;
        private readonly string _logPrefix;

        public ChangeStreamQueueManager(Log log, string logPrefix, int maxConcurrentOperations = 50, int initialBatchSize = 200)
        {
            _log = log;
            _logPrefix = logPrefix;
            _maxConcurrentOperations = maxConcurrentOperations;
            _adaptiveBatchSize = Math.Min(initialBatchSize, MaxBatchSize);
            _processingThrottle = new SemaphoreSlim(maxConcurrentOperations);
        }

        /// <summary>
        /// Get current adaptive batch size
        /// </summary>
        public int GetAdaptiveBatchSize()
        {
            lock (_batchSizeLock)
            {
                return _adaptiveBatchSize;
            }
        }

        /// <summary>
        /// Set adaptive batch size with bounds checking
        /// </summary>
        public void SetAdaptiveBatchSize(int size)
        {
            lock (_batchSizeLock)
            {
                _adaptiveBatchSize = Math.Max(MinBatchSize, Math.Min(size, MaxBatchSize));
            }
        }

        /// <summary>
        /// Reduce adaptive batch size (typically after OOM or performance issues)
        /// </summary>
        public int ReduceBatchSize(int divisor = 2)
        {
            lock (_batchSizeLock)
            {
                _adaptiveBatchSize = Math.Max(MinBatchSize, _adaptiveBatchSize / divisor);
                return _adaptiveBatchSize;
            }
        }

        /// <summary>
        /// Check if queue is near capacity
        /// </summary>
        public bool IsQueueNearCapacity(int currentSize, out int threshold)
        {
            threshold = (int)(MaxQueueSize * 0.8); // 80% capacity
            return currentSize >= threshold;
        }

        /// <summary>
        /// Check if queue is at maximum capacity
        /// </summary>
        public bool IsQueueAtCapacity(int currentSize)
        {
            return currentSize >= MaxQueueSize;
        }

        /// <summary>
        /// Get drain target for queue (when pausing to allow processing)
        /// </summary>
        public int GetDrainTarget()
        {
            return (int)(MaxQueueSize * 0.5); // Drain to 50%
        }

        /// <summary>
        /// Check memory usage and return percentage
        /// </summary>
        public double GetMemoryUsagePercentage()
        {
            var now = DateTime.UtcNow;
            if ((now - _lastMemoryCheck).TotalSeconds < MemoryCheckIntervalSeconds)
            {
                // Return cached calculation
                var currentMemory = GC.GetTotalMemory(false);
                return _totalSystemMemory > 0 ? (currentMemory / (double)_totalSystemMemory) * 100.0 : 0;
            }

            _lastMemoryCheck = now;
            
            // Get total system memory
            if (_totalSystemMemory == 0)
            {
                try
                {
                    var gcMemoryInfo = GC.GetGCMemoryInfo();
                    _totalSystemMemory = gcMemoryInfo.TotalAvailableMemoryBytes;
                }
                catch
                {
                    _totalSystemMemory = 8L * 1024L * 1024L * 1024L; // Default to 8GB
                }
            }

            var memoryUsed = GC.GetTotalMemory(false);
            return (_totalSystemMemory > 0) ? (memoryUsed / (double)_totalSystemMemory) * 100.0 : 0;
        }

        /// <summary>
        /// Get optimal batch size based on memory usage
        /// </summary>
        public int GetOptimalBatchSize()
        {
            var memoryPercentage = GetMemoryUsagePercentage();
            var adaptiveBatch = GetAdaptiveBatchSize();

            // Memory-based throttling
            if (memoryPercentage > 20.0)
                return MinBatchSize; // Critical - minimal processing
            else if (memoryPercentage > 15.0)
                return Math.Max(MinBatchSize, adaptiveBatch / 4); // High - 25%
            else if (memoryPercentage > 12.0)
                return Math.Max(MinBatchSize, adaptiveBatch / 2); // Elevated - 50%
            else if (memoryPercentage > 10.0)
                return Math.Max(MinBatchSize, (int)(adaptiveBatch * 0.75)); // Moderate - 75%
            else
                return adaptiveBatch; // Normal - full speed
        }

        /// <summary>
        /// Perform emergency memory recovery
        /// </summary>
        public void PerformEmergencyMemoryRecovery()
        {
            _log.WriteLine($"{_logPrefix}Performing emergency memory recovery (GC)...");
            
            GC.Collect(GC.MaxGeneration, GCCollectionMode.Aggressive, blocking: true, compacting: true);
            GC.WaitForPendingFinalizers();
            GC.Collect(GC.MaxGeneration, GCCollectionMode.Aggressive, blocking: true, compacting: true);
            
            var memoryMB = GC.GetTotalMemory(false) / (1024 * 1024);
            _log.WriteLine($"{_logPrefix}Emergency GC completed. Current memory: {memoryMB}MB");
        }

        /// <summary>
        /// Perform memory cleanup (less aggressive than emergency)
        /// </summary>
        public void PerformMemoryCleanup()
        {
            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();
        }

        /// <summary>
        /// Wait for processing throttle
        /// </summary>
        public async Task WaitForThrottleAsync()
        {
            await _processingThrottle.WaitAsync();
        }

        /// <summary>
        /// Release processing throttle
        /// </summary>
        public void ReleaseThrottle()
        {
            _processingThrottle.Release();
        }

        /// <summary>
        /// Process items from a queue in parallel batches
        /// </summary>
        public async Task<int> ProcessQueueInBatchesAsync<T>(
            ConcurrentQueue<T> queue,
            Func<List<T>, Task<int>> batchProcessor,
            int maxParallelBatches = 5)
        {
            if (queue.IsEmpty)
                return 0;

            var queueSize = queue.Count;
            int numberOfBatches = Math.Min(maxParallelBatches, (queueSize / OptimalBatchSize) + 1);
            var batchTasks = new List<Task<int>>();
            int totalItemsDequeued = 0;

            for (int i = 0; i < numberOfBatches; i++)
            {
                // Dequeue a batch of items
                var batchItems = new List<T>();
                while (queue.TryDequeue(out var item) && batchItems.Count < OptimalBatchSize)
                {
                    batchItems.Add(item);
                }

                if (batchItems.Count == 0)
                    break;

                totalItemsDequeued += batchItems.Count;

                // Process this batch
                batchTasks.Add(batchProcessor(batchItems));
            }

            if (batchTasks.Count > 0)
            {
                var results = await Task.WhenAll(batchTasks);
                return results.Sum();
            }

            return 0;
        }

        /// <summary>
        /// Get flush interval based on lag
        /// </summary>
        public TimeSpan GetFlushInterval(double lagSeconds, double maxAcceptableLag = 20.0)
        {
            if (lagSeconds > 100) // Extreme lag
                return TimeSpan.FromMilliseconds(50);
            else if (lagSeconds > maxAcceptableLag) // High lag
                return TimeSpan.FromMilliseconds(100);
            else if (lagSeconds > maxAcceptableLag / 2) // Medium lag
                return TimeSpan.FromMilliseconds(200);
            else
                return TimeSpan.FromMilliseconds(500); // Normal
        }

        /// <summary>
        /// Calculate memory-protected frequency multiplier
        /// </summary>
        public double GetMemoryProtectedFrequency()
        {
            var memoryPercentage = GetMemoryUsagePercentage();
            
            if (memoryPercentage > 12.0)
                return 2.0;  // 2x slower if memory >12%
            else if (memoryPercentage > 10.0)
                return 1.5;  // 1.5x slower if memory >10%
            else if (memoryPercentage > 8.0)
                return 1.2;  // 1.2x slower if memory >8%
            else
                return 1.0;  // Full speed if memory <8%
        }

        /// <summary>
        /// Dispose resources
        /// </summary>
        public void Dispose()
        {
            _processingThrottle?.Dispose();
        }
    }
}
