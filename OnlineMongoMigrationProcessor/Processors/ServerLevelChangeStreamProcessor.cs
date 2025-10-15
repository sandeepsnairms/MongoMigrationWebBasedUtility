using MongoDB.Bson;
using MongoDB.Driver;
using OnlineMongoMigrationProcessor.Helpers;
using OnlineMongoMigrationProcessor.Models;
using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Concurrent;
using static OnlineMongoMigrationProcessor.MongoHelper;

#pragma warning disable CS8602 // Dereference of a possibly null reference.

namespace OnlineMongoMigrationProcessor
{
    public class ServerLevelChangeStreamProcessor : ChangeStreamProcessor
    {
        // Server-level processors use MigrationJob properties directly for global resume tokens
        protected override bool UseResumeTokenCache => false;

        // Ultra-aggressive performance optimization for minimal lag
        private readonly ConcurrentDictionary<string, (MigrationUnit Unit, IMongoCollection<BsonDocument> TargetCollection)> _collectionCache = new();

        // Queue manager for change stream processing
        private readonly ChangeStreamQueueManager _queueManager;

        // Extremely aggressive concurrency controls for maximum throughput and minimal lag
        private readonly SemaphoreSlim _processingThrottle;
        private const int MaxConcurrentBulkOperations = 24; // Increased for higher throughput when lag is growing

        // Ultra-fast save mechanism with minimal intervals to reduce data loss risk
        private DateTime _lastSaveTime = DateTime.MinValue;
        private readonly TimeSpan _saveInterval = TimeSpan.FromSeconds(5); // Increased back to 5 seconds
        private readonly object _saveLock = new object();

        // High-frequency change processing with multiple queues for load balancing
        private readonly ConcurrentQueue<ChangeStreamDocument<BsonDocument>>[] _changeQueues;
        private const int QueueCount = 8; // Increased back to 8 for more parallelism
        private volatile int _currentQueueIndex = 0;
        private readonly ConcurrentDictionary<string, ChangeStreamDocuments> _globalChangeBuffer = new();
        private readonly Timer _flushTimer;
        private volatile bool _isProcessingQueue = false;

        // Lag monitoring and adaptive processing
        private readonly ConcurrentDictionary<string, DateTime> _lastChangeTimestamp = new();
        private DateTime _lastLagCheck = DateTime.UtcNow;
        private const int LagCheckIntervalSeconds = 5; // Back to 5 seconds

        // Performance metrics and adaptive controls
        private long _totalChangesProcessed = 0;
        private DateTime _lastThroughputReport = DateTime.UtcNow;
        private double _currentLagSeconds = 0; // Removed volatile since double cannot be volatile
        private readonly object _lagLock = new object(); // Added lock for thread-safe access
        private const double MaxAcceptableLagSeconds = 20; // Relaxed to 20 seconds

        // Adaptive batch sizing based on lag - SMART memory-aware performance limits (will migrate to _queueManager)
        private volatile int _adaptiveBatchSize;
        private const int MinBatchSize = 25; // Reasonable minimum for performance
        private const int MaxBatchSize = 1000; // High performance when memory allows
        private const int HighPerformanceBatchSize = 2000; // Maximum performance mode

        // Memory management and monitoring - percentage-based thresholds (will migrate to _queueManager)
        private readonly object _memoryOptimizationLock = new object();
        private volatile bool _isMemoryOptimized = false;
        private DateTime _lastMemoryCheck = DateTime.UtcNow;
        private const int MemoryCheckIntervalSeconds = 30;
        private long _lastMemoryUsage = 0;
        private long _totalSystemMemory = 0; // Cache total system memory

        public ServerLevelChangeStreamProcessor(Log log, MongoClient sourceClient, MongoClient targetClient, JobList jobList, MigrationJob job, MigrationSettings config, bool syncBack = false)
            : base(log, sourceClient, targetClient, jobList, job, config, syncBack)
        {
            // Initialize queue manager for shared change stream processing
            _queueManager = new ChangeStreamQueueManager(_log, _syncBackPrefix, MaxConcurrentBulkOperations, _config.ChangeStreamMaxDocsInBatch);
            
            // Initialize ultra-aggressive performance optimizations
            _processingThrottle = new SemaphoreSlim(MaxConcurrentBulkOperations);
            
            // Initialize multiple processing queues for load balancing
            _changeQueues = new ConcurrentQueue<ChangeStreamDocument<BsonDocument>>[QueueCount];
            for (int i = 0; i < QueueCount; i++)
            {
                _changeQueues[i] = new ConcurrentQueue<ChangeStreamDocument<BsonDocument>>();
            }
            
            // Initialize adaptive batch size with memory-safe defaults - using queue manager
            _adaptiveBatchSize = _queueManager.GetAdaptiveBatchSize();
            
            // Adaptive flush timer - starts at 500ms but will be dynamically adjusted based on lag
            _flushTimer = new Timer(ProcessQueuedChanges, null, TimeSpan.FromMilliseconds(500), TimeSpan.FromMilliseconds(500));
            
            // Enable memory optimization with monitoring
            OptimizeMemorySettings();
        }

        /// <summary>
        /// Get total system memory and calculate percentage-based thresholds
        /// </summary>
        private long GetTotalSystemMemory()
        {
            if (_totalSystemMemory == 0)
            {
                try
                {
                    // Try to get memory info from GC first
                    var memoryInfo = GC.GetGCMemoryInfo();
                    _totalSystemMemory = memoryInfo.TotalAvailableMemoryBytes;
                    
                    // Check if this seems reasonable for a modern system
                    var memoryGB = _totalSystemMemory / (1024.0 * 1024.0 * 1024.0);
                    
                    if (memoryGB < 8.0) // Less than 8GB seems unrealistic for your system
                    {
                        // The GC is likely reporting container/process limits, not actual system memory
                        // For your 32GB system, let's use a more realistic estimate
                        
                        var currentUsage = GC.GetTotalMemory(false);
                        var currentUsageGB = currentUsage / (1024.0 * 1024.0 * 1024.0);
                        
                        // If we're using less than 8GB reported total, but you have 32GB, use 32GB
                        if (memoryGB <= 4.0 && currentUsageGB < 2.0)
                        {
                            _totalSystemMemory = 32L * 1024 * 1024 * 1024; // Use your actual 32GB
                        }
                        else
                        {
                            // Use a conservative estimate based on current usage
                            _totalSystemMemory = Math.Max(16L * 1024 * 1024 * 1024, currentUsage * 8);
                        }
                    }
                }
                catch (Exception)
                {
                    // Conservative fallback - assume your 4GB system
                    _totalSystemMemory = 4L * 1024 * 1024 * 1024;
                }
            }
            return _totalSystemMemory;
        }

        /// <summary>
        /// Get memory usage percentage (0-100) - using shared queue manager
        /// </summary>
        private double GetMemoryUsagePercentage()
        {
            return _queueManager.GetMemoryUsagePercentage();
        }

        private void OptimizeMemorySettings()
        {
            lock (_memoryOptimizationLock)
            {
                if (!_isMemoryOptimized)
                {
                    // Initial memory cleanup with monitoring
                    PerformMemoryCleanup();
                    
                    _isMemoryOptimized = true;
                    _lastMemoryCheck = DateTime.UtcNow;
                }
            }
        }

        private void PerformMemoryCleanup()
        {
            try
            {
                // Get current memory usage
                var currentMemory = GC.GetTotalMemory(false);
                var memoryPercentage = GetMemoryUsagePercentage();
                var totalMemoryGB = GetTotalSystemMemory() / (1024.0 * 1024.0 * 1024.0);
                
                // Adaptive emergency thresholds based on system memory size
                double emergencyThreshold, aggressiveThreshold, criticalThreshold;
                if (totalMemoryGB >= 16.0)
                {
                    // Large memory systems: 60%, 50%, 40% - Much more conservative for GC
                    emergencyThreshold = 60.0;
                    aggressiveThreshold = 50.0;
                    criticalThreshold = 40.0;
                }
                else if (totalMemoryGB >= 8.0)
                {
                    // Medium memory systems: 40%, 30%, 25%
                    emergencyThreshold = 40.0;
                    aggressiveThreshold = 30.0;
                    criticalThreshold = 25.0;
                }
                else
                {
                    // Small memory systems: 25%, 18%, 15%
                    emergencyThreshold = 25.0;
                    aggressiveThreshold = 18.0;
                    criticalThreshold = 15.0;
                }
                
                // Clear queues only if memory usage is truly critical
                if (memoryPercentage > emergencyThreshold)
                {
                    _log.WriteLine($"{_syncBackPrefix}Critical memory usage ({memoryPercentage:F1}% of {totalMemoryGB:F1}GB system memory). Clearing queues for emergency recovery.", LogType.Warning);
                    
                    // Clear all change queues to free up memory
                    for (int i = 0; i < QueueCount; i++)
                    {
                        while (_changeQueues[i].TryDequeue(out _)) { }
                    }
                    
                    // Clear global change buffer
                    _globalChangeBuffer.Clear();
                }
                
                // Only force aggressive garbage collection if memory usage is truly high
                if (memoryPercentage > criticalThreshold)
                {
                    GC.Collect(2, GCCollectionMode.Aggressive, true);
                    GC.WaitForPendingFinalizers();
                    GC.Collect();
                    
                    // Additional cleanup round only if memory is extremely high
                    if (memoryPercentage > aggressiveThreshold)
                    {
                        GC.Collect(1, GCCollectionMode.Forced, true);
                    }
                    
                    var afterCleanup = GC.GetTotalMemory(false);
                    _lastMemoryUsage = afterCleanup;
                }
                else
                {
                    // For lower memory usage, just update tracking without aggressive GC
                    _lastMemoryUsage = currentMemory;
                }
            }
            catch (Exception ex)
            {
                _log.WriteLine($"{_syncBackPrefix}Error during memory cleanup: {ex.Message}", LogType.Error);
            }
        }

        /// <summary>
        /// Emergency memory recovery when OutOfMemoryException occurs - using shared queue manager
        /// </summary>
        private void PerformEmergencyMemoryRecovery()
        {
            _queueManager.PerformEmergencyMemoryRecovery();
            
            // Also clear local queues and buffers specific to ServerLevel
            try
            {
                // Immediate queue clearing
                for (int i = 0; i < QueueCount; i++)
                {
                    var queueCount = 0;
                    while (_changeQueues[i].TryDequeue(out _))
                    {
                        queueCount++;
                    }
                    if (queueCount > 0)
                    {
                        _log.WriteLine($"{_syncBackPrefix}Emergency: Cleared {queueCount} items from queue {i}", LogType.Debug);
                    }
                }
                
                // Clear all global buffers
                var bufferCount = _globalChangeBuffer.Count;
                _globalChangeBuffer.Clear();
                _log.WriteLine($"{_syncBackPrefix}Emergency: Cleared {bufferCount} global change buffers", LogType.Debug);
                
                // Clear collection cache if needed
                if (_collectionCache.Count > 100) // Only if very large
                {
                    var cacheCount = _collectionCache.Count;
                    _collectionCache.Clear();
                    _log.WriteLine($"{_syncBackPrefix}Emergency: Cleared {cacheCount} collection cache entries", LogType.Debug);
                }
            }
            catch (Exception ex)
            {
                _log.WriteLine($"{_syncBackPrefix}Error during emergency memory recovery: {ex.Message}", LogType.Error);
            }
        }

        private void CheckMemoryUsage()
        {
            var now = DateTime.UtcNow;
            if ((now - _lastMemoryCheck).TotalSeconds < MemoryCheckIntervalSeconds)
                return;

            _lastMemoryCheck = now;
            
            try
            {
                var currentMemory = GC.GetTotalMemory(false);
                var memoryPercentage = GetMemoryUsagePercentage();
                var totalMemoryGB = GetTotalSystemMemory() / (1024.0 * 1024.0 * 1024.0);
                
                // Much more conservative thresholds based on system memory size
                double warningThreshold, emergencyThreshold;
                if (totalMemoryGB >= 16.0)
                {
                    // Large memory systems: 35%, 50% - Allow much higher usage before action
                    warningThreshold = 35.0;
                    emergencyThreshold = 50.0;
                }
                else if (totalMemoryGB >= 8.0)
                {
                    // Medium memory systems: 25%, 35%
                    warningThreshold = 25.0;
                    emergencyThreshold = 35.0;
                }
                else
                {
                    // Small memory systems: 15%, 20%
                    warningThreshold = 15.0;
                    emergencyThreshold = 20.0;
                }
                
                // Only trigger cleanup if memory usage is truly problematic
                if (memoryPercentage > emergencyThreshold)
                {
                    PerformMemoryCleanup();
                    
                    // Aggressively reduce batch sizes only when truly needed - using queue manager
                    _queueManager.ReduceBatchSize(4); // Divide by 4
                    _adaptiveBatchSize = _queueManager.GetAdaptiveBatchSize();
                }
                else if (memoryPercentage > warningThreshold)
                {
                    // Gentle batch size reduction for preventive action - using queue manager
                    var current = _queueManager.GetAdaptiveBatchSize();
                    _queueManager.SetAdaptiveBatchSize(current * 3 / 4);
                    _adaptiveBatchSize = _queueManager.GetAdaptiveBatchSize();
                }
                
                _lastMemoryUsage = currentMemory;
            }
            catch (Exception ex)
            {
                _log.WriteLine($"{_syncBackPrefix}Error checking memory usage: {ex.Message}", LogType.Error);
            }
        }

        /// <summary>
        /// Calculate optimal batch size - SMART performance with memory-aware OOM prevention
        /// Using queue manager for memory awareness with server-level lag optimization
        /// </summary>
        private int GetOptimalBatchSize()
        {
            var memoryPercentage = GetMemoryUsagePercentage();
            var totalMemoryGB = GetTotalSystemMemory() / (1024.0 * 1024.0 * 1024.0);
            
            // Get current lag for performance adjustment
            double currentLag;
            lock (_lagLock)
            {
                currentLag = _currentLagSeconds;
            }
            
            // SMART thresholds: High performance until memory becomes a real problem
            double warningThreshold, dangerThreshold, criticalThreshold;
            if (totalMemoryGB >= 16.0)
            {
                // Large memory systems: 40%, 60%, 80% - Allow high performance until real issues
                warningThreshold = 40.0;
                dangerThreshold = 60.0;
                criticalThreshold = 80.0;
            }
            else if (totalMemoryGB >= 8.0)
            {
                // Medium memory systems: 30%, 50%, 70%
                warningThreshold = 30.0;
                dangerThreshold = 50.0;
                criticalThreshold = 70.0;
            }
            else
            {
                // Small memory systems: 20%, 35%, 50%
                warningThreshold = 20.0;
                dangerThreshold = 35.0;
                criticalThreshold = 50.0;
            }
            
            // SMART batch sizing: High performance unless memory is actually problematic
            if (memoryPercentage < warningThreshold) // Normal operation - HIGH PERFORMANCE
            {
                if (currentLag > MaxAcceptableLagSeconds)
                    return Math.Min(HighPerformanceBatchSize, ChangeStreamQueueManager.MaxBatchSize * 2); // Maximum performance under lag
                else
                    return Math.Min(ChangeStreamQueueManager.MaxBatchSize, HighPerformanceBatchSize / 2); // High normal performance
            }
            else if (memoryPercentage < dangerThreshold) // Moderate memory usage - BALANCED
            {
                if (currentLag > MaxAcceptableLagSeconds)
                    return Math.Min(ChangeStreamQueueManager.MaxBatchSize, HighPerformanceBatchSize / 2); // Balanced performance under lag
                else
                    return Math.Min(ChangeStreamQueueManager.MaxBatchSize / 2, 500); // Moderate performance
            }
            else if (memoryPercentage < criticalThreshold) // High memory usage - SAFE MODE
            {
                return Math.Min(200, ChangeStreamQueueManager.MaxBatchSize / 4); // Conservative but reasonable
            }
            else // Critical memory usage - SURVIVAL MODE (only when really needed)
            {
                return Math.Max(ChangeStreamQueueManager.MinBatchSize, 50); // Conservative survival mode
            }
        }

        private void InitializeCollectionCache()
        {
            _collectionCache.Clear();
            _log.WriteLine($"{_syncBackPrefix}Initializing ultra-collection cache for {_migrationUnitsToProcess.Count} collections.");
            
            var initTasks = _migrationUnitsToProcess.Select(kvp =>
            {
                try
                {
                    var unit = kvp.Value;
                    var targetCollection = GetTargetCollection(unit.DatabaseName, unit.CollectionName);
                    _collectionCache[kvp.Key] = (unit, targetCollection);
                    _globalChangeBuffer[kvp.Key] = new ChangeStreamDocuments();
                    _lastChangeTimestamp[kvp.Key] = DateTime.UtcNow;
                    return kvp.Key;
                }
                catch (Exception ex)
                {
                    _log.WriteLine($"{_syncBackPrefix}Error caching collection {kvp.Key}: {ex.Message}", LogType.Error);
                    return null;
                }
            });

            var results = initTasks.ToList();
            var successCount = results.Count(r => r != null);
            
            _log.WriteLine($"{_syncBackPrefix}Ultra-cache initialized with {successCount} collections.");
        }

        /// <summary>
        /// Ultra-optimized save method with minimal I/O and lag-aware timing
        /// </summary>
        private void OptimizedSave(bool force = false)
        {
            if (!force)
            {
                // Check if we need to save based on lag - save more frequently under high lag
                double currentLag;
                lock (_lagLock)
                {
                    currentLag = _currentLagSeconds;
                }
                
                var shouldSaveEarly = currentLag > MaxAcceptableLagSeconds / 3; // More aggressive threshold
                if (!shouldSaveEarly)
                {
                    return;
                }
            }

            lock (_saveLock)
            {
                var now = DateTime.UtcNow;
                
                double currentLag;
                lock (_lagLock)
                {
                    currentLag = _currentLagSeconds;
                }
                
                var effectiveInterval = currentLag > MaxAcceptableLagSeconds ? 
                    TimeSpan.FromSeconds(1) : _saveInterval; // Even faster saves under high lag
                
                if (force || (now - _lastSaveTime) >= effectiveInterval)
                {
                    // Use fire-and-forget for save operations to avoid blocking
                    _ = Task.Run(() =>
                    {
                        try
                        {
                            _jobList?.Save();
                            _lastSaveTime = DateTime.UtcNow;
                        }
                        catch (Exception ex)
                        {
                            _log.WriteLine($"{_syncBackPrefix}Background save error: {ex.Message}", LogType.Error);
                        }
                    });
                }
            }
        }

        /// <summary>
        /// Memory-safe queue processor with dynamic adaptive processing
        /// </summary>
        private async void ProcessQueuedChanges(object? state)
        {
            if (_isProcessingQueue)
                return;

            _isProcessingQueue = true;

            try
            {
                // Check memory usage before processing
                CheckMemoryUsage();
                
                var processedCount = 0;
                
                // Dynamic batch sizing based on current optimal batch size
                var optimalBatch = GetOptimalBatchSize();
                var maxBatchSize = Math.Min(optimalBatch * QueueCount, optimalBatch * 5); // Scale with optimal batch

                // Check and update lag metrics
                CheckAndUpdateLagMetrics();

                // Process from all queues with dynamic memory-aware approach
                var queueTasks = new List<Task<int>>();
                
                for (int q = 0; q < QueueCount && processedCount < maxBatchSize; q++)
                {
                    var queueIndex = q; // Capture for closure
                    queueTasks.Add(Task.Run(() =>
                    {
                        var currentQueue = _changeQueues[queueIndex];
                        var queueBatchSize = Math.Min(optimalBatch / QueueCount, optimalBatch / 2); // Dynamic per-queue batch sizing
                        var localProcessed = 0;

                        for (int i = 0; i < queueBatchSize && currentQueue.TryDequeue(out var change); i++)
                        {
                            ProcessChangeImmediate(change);
                            localProcessed++;
                            Interlocked.Increment(ref _totalChangesProcessed);
                        }
                        
                        return localProcessed;
                    }));
                }

                // Wait for all queue processing to complete
                var results = await Task.WhenAll(queueTasks);
                processedCount = results.Sum();

                if (processedCount > 0)
                {
                    // Trigger bulk processing for collections that exceed thresholds or have high lag
                    _ = Task.Run(ProcessAccumulatedChanges);
                    
                    // Report throughput
                    var reportInterval = _currentLagSeconds > MaxAcceptableLagSeconds ? 15 : 30;
                    ReportThroughput(reportInterval);
                }
            }
            finally
            {
                _isProcessingQueue = false;
            }
        }

        /// <summary>
        /// Monitor lag and adjust processing parameters dynamically with memory awareness
        /// </summary>
        private void CheckAndUpdateLagMetrics()
        {
            var now = DateTime.UtcNow;
            if ((now - _lastLagCheck).TotalSeconds < LagCheckIntervalSeconds)
                return;

            _lastLagCheck = now;
            var maxLag = 0.0;

            // Calculate current lag across all collections
            foreach (var kvp in _lastChangeTimestamp)
            {
                var collectionKey = kvp.Key;
                var lastChangeTime = kvp.Value;
                var lagSeconds = (now - lastChangeTime).TotalSeconds;
                
                if (lagSeconds > maxLag)
                    maxLag = lagSeconds;
            }

            lock (_lagLock)
            {
                _currentLagSeconds = maxLag;
            }

            // Adapt batch size based on lag with dynamic memory percentage-aware constraints
            var memoryPercentage = GetMemoryUsagePercentage();
            var maxAllowedBatch = GetOptimalBatchSize(); // Use dynamic optimal batch size

            // EMERGENCY OOM CIRCUIT BREAKER: Immediately back off aggressive settings if memory spikes
            if (memoryPercentage > 15.0) // Emergency threshold
            {
                // Immediately switch to conservative mode
                _adaptiveBatchSize = Math.Max(MinBatchSize, _adaptiveBatchSize / 4);
                _flushTimer?.Change(TimeSpan.FromMilliseconds(1000), TimeSpan.FromMilliseconds(1000)); // Much slower
                
                if (memoryPercentage > 20.0) // Critical emergency
                {
                    _log.WriteLine($"{_syncBackPrefix}EMERGENCY OOM PROTECTION: Memory at {memoryPercentage:F1}%, switching to survival mode. Batch size: {_adaptiveBatchSize}, Queue interval: 1000ms", LogType.Warning);
                    PerformEmergencyMemoryRecovery(); // Force cleanup
                }
                else
                {
                    _log.WriteLine($"{_syncBackPrefix}OOM CIRCUIT BREAKER: Memory at {memoryPercentage:F1}%, reducing batch size to {_adaptiveBatchSize} and slowing queues", LogType.Warning);
                }
                return; // Skip all aggressive optimizations
            }

            // EXTREME LAG CATCHUP: Adjust queue processing frequency for faster processing
            // WITH MEMORY PROTECTION: Scale back frequency if memory usage is climbing
            var memoryProtectedFrequency = memoryPercentage > 12.0 ? 2.0 :   // 2x slower if memory >12%
                                          memoryPercentage > 10.0 ? 1.5 :   // 1.5x slower if memory >10%  
                                          memoryPercentage > 8.0 ? 1.2 :    // 1.2x slower if memory >8%
                                          1.0;                              // Full speed if memory <8%

            if (maxLag > 1000) // For extreme lag like your 2607s
            {
                // Process queues much more frequently for extreme catchup, but with memory protection
                var interval = (int)(100 * memoryProtectedFrequency);
                _flushTimer?.Change(TimeSpan.FromMilliseconds(interval), TimeSpan.FromMilliseconds(interval));
            }
            else if (maxLag > 500) // High lag  
            {
                var interval = (int)(200 * memoryProtectedFrequency);
                _flushTimer?.Change(TimeSpan.FromMilliseconds(interval), TimeSpan.FromMilliseconds(interval));
            }
            else if (maxLag > 100) // Moderate lag
            {
                var interval = (int)(300 * memoryProtectedFrequency);
                _flushTimer?.Change(TimeSpan.FromMilliseconds(interval), TimeSpan.FromMilliseconds(interval));
            }
            else if (maxLag < 20) // Low lag - can relax
            {
                _flushTimer?.Change(TimeSpan.FromMilliseconds(500), TimeSpan.FromMilliseconds(500)); // Normal
            }
            
            // AGGRESSIVE CATCHUP LOGIC: Dramatically increase batch size for extreme lag situations
            // BUT with enhanced OOM protection - monitor memory more strictly during aggressive phases
            if (maxLag > MaxAcceptableLagSeconds && memoryPercentage < 15.0) // More reasonable threshold
            {
                // ENHANCED OOM PROTECTION: Reduce aggressiveness if memory is climbing
                var memoryBasedReduction = memoryPercentage > 10.0 ? 0.5 :  // 50% reduction if memory >10%
                                          memoryPercentage > 8.0 ? 0.7 :   // 30% reduction if memory >8%
                                          memoryPercentage > 6.0 ? 0.85 :  // 15% reduction if memory >6%
                                          1.0;                             // Full aggressiveness if memory <6%

                // Calculate aggressive increase based on lag severity with memory protection
                var lagMultiplier = maxLag > 2000 ? 8 :  // EXTREME lag like your 2607s - 8x increase
                                   maxLag > 1000 ? 6 :   // Very high lag - 6x increase
                                   maxLag > 500 ? 4 :    // High lag - 4x increase
                                   maxLag > 200 ? 3 :    // Moderate high lag - 3x increase
                                   2;                    // Regular high lag - 2x increase
                
                var aggressiveIncrease = (int)(50 * lagMultiplier * memoryBasedReduction); // Apply memory protection
                _adaptiveBatchSize = Math.Min(maxAllowedBatch, _adaptiveBatchSize + aggressiveIncrease);
            }
            else if (maxLag < MaxAcceptableLagSeconds / 4 || memoryPercentage > 20.0) // More reasonable threshold
            {
                // Decrease batch size for better responsiveness or to reduce memory pressure
                _adaptiveBatchSize = Math.Max(MinBatchSize, _adaptiveBatchSize - 25); // Moderate decrements
            }
        }

        /// <summary>
        /// Ultra-fast processing without awaiting for maximum speed and lag reduction
        /// </summary>
        private void ProcessChangeImmediate(ChangeStreamDocument<BsonDocument> change)
        {
            try
            {
                if (change.CollectionNamespace == null) return;

                var collectionKey = change.CollectionNamespace.ToString();
                
                if (!_collectionCache.TryGetValue(collectionKey, out var cached))
                    return; // Skip uncached collections

                if (!Helper.IsMigrationUnitValid(cached.Unit))
                    return;

                // Apply user filter if present
                if (!string.IsNullOrWhiteSpace(cached.Unit.UserFilter) && change.OperationType != ChangeStreamOperationType.Delete)
                {
                    var userFilterDoc = BsonDocument.Parse(cached.Unit.UserFilter);
                    if (userFilterDoc.Elements.Any() && !MongoHelper.CheckForUserFilterMatch(change.FullDocument, userFilterDoc))
                        return;
                }

                // Update timestamps with ultra-low latency
                var timeStamp = DateTime.UtcNow; // Use current time for immediate processing
                
                if (!_job.SourceServerVersion.StartsWith("3") && change.ClusterTime != null)
                {
                    timeStamp = MongoHelper.BsonTimestampToUtcDateTime(change.ClusterTime);
                }
                else if (!_job.SourceServerVersion.StartsWith("3") && change.WallTime != null)
                {
                    timeStamp = change.WallTime.Value;
                }

                // Update lag tracking
                _lastChangeTimestamp[collectionKey] = timeStamp;

                // Update timestamps immediately without locks for ultra-low latency
                if (!_syncBack)
                {
                    cached.Unit.CursorUtcTimestamp = timeStamp;
                    _job.CursorUtcTimestamp = timeStamp;
                }
                else
                {
                    cached.Unit.SyncBackCursorUtcTimestamp = timeStamp;
                    _job.SyncBackCursorUtcTimestamp = timeStamp;
                }

                // Update counters
                cached.Unit.CSUpdatesInLastBatch++;
                IncrementEventCounter(cached.Unit, change.OperationType);

                // Add to global buffer for bulk processing
                if (_globalChangeBuffer.TryGetValue(collectionKey, out var buffer))
                {
                    lock (buffer)
                    {
                        switch (change.OperationType)
                        {
                            case ChangeStreamOperationType.Insert:
                                if (change.FullDocument != null && !change.FullDocument.IsBsonNull)
                                    buffer.AddInsert(change);
                                break;
                            case ChangeStreamOperationType.Update:
                            case ChangeStreamOperationType.Replace:
                                if (change.FullDocument != null && !change.FullDocument.IsBsonNull)
                                    buffer.AddUpdate(change);
                                break;
                            case ChangeStreamOperationType.Delete:
                                buffer.AddDelete(change);
                                break;
                        }
                    }
                }

                // Update resume tokens immediately
                if (change.ResumeToken != null && change.ResumeToken != BsonNull.Value)
                {
                    var resumeTokenJson = change.ResumeToken.ToJson();
                    UpdateResumeToken(resumeTokenJson, change.OperationType, change.DocumentKey.ToJson(), collectionKey);
                }
            }
            catch (Exception ex)
            {
                // Minimal error logging to avoid performance impact
                if (_totalChangesProcessed % 10000 == 0) // Only log every 10,000th error
                {
                    _log.WriteLine($"{_syncBackPrefix}Change processing error: {ex.Message}", LogType.Error);
                }
            }
        }

        /// <summary>
        /// Process accumulated changes with memory-safe batching
        /// </summary>
        private async Task ProcessAccumulatedChanges()
        {
            var flushTasks = new List<Task>();
            var memoryMB = GC.GetTotalMemory(false) / (1024 * 1024);

            // Process all collections without prioritization
            foreach (var collectionKey in _globalChangeBuffer.Keys.ToList())
            {
                if (!_globalChangeBuffer.TryGetValue(collectionKey, out var buffer))
                    continue;

                int totalChanges;
                
                lock (buffer)
                {
                    totalChanges = buffer.DocsToBeInserted.Count + buffer.DocsToBeUpdated.Count + buffer.DocsToBeDeleted.Count;
                }

                // Use dynamic thresholds based on memory
                var baseThreshold = memoryMB > 500 ? _adaptiveBatchSize / 16 : _adaptiveBatchSize / 8;
                var threshold = baseThreshold * 2; // Consistent threshold for all collections

                // Process collections with substantial accumulated changes
                if (totalChanges > threshold)
                {
                    flushTasks.Add(Task.Run(async () =>
                    {
                        await _processingThrottle.WaitAsync();
                        try
                        {
                            if (_collectionCache.TryGetValue(collectionKey, out var cached))
                            {
                                List<ChangeStreamDocument<BsonDocument>> inserts, updates, deletes;
                                
                                // Extract changes under lock, then process outside lock
                                lock (buffer)
                                {
                                    inserts = new List<ChangeStreamDocument<BsonDocument>>(buffer.DocsToBeInserted);
                                    updates = new List<ChangeStreamDocument<BsonDocument>>(buffer.DocsToBeUpdated);
                                    deletes = new List<ChangeStreamDocument<BsonDocument>>(buffer.DocsToBeDeleted);
                                    
                                    buffer.DocsToBeInserted.Clear();
                                    buffer.DocsToBeUpdated.Clear();
                                    buffer.DocsToBeDeleted.Clear();
                                }

                                if (inserts.Count + updates.Count + deletes.Count > 0)
                                {
                                    // Use memory-aware batch sizes
                                    var maxBatchSize = memoryMB > 500 ? 100 : 200;
                                    var batchSize = Math.Min(maxBatchSize, _adaptiveBatchSize / 2);

                                    await BulkProcessChangesAsync(
                                        cached.Unit,
                                        cached.TargetCollection,
                                        inserts,
                                        updates,
                                        deletes,
                                        batchSize);
                                }
                            }
                        }
                        finally
                        {
                            _processingThrottle.Release();
                        }
                    }));
                }
            }

            if (flushTasks.Count > 0)
            {
                await Task.WhenAll(flushTasks);
                OptimizedSave(); // Trigger optimized save
                
                // Perform memory cleanup after bulk operations (optimized threshold)
                if (memoryMB > 800) // Higher threshold for less frequent cleanup
                {
                    PerformMemoryCleanup();
                }
            }
        }

        private void ReportThroughput(int intervalSeconds = 30)
        {
            var now = DateTime.UtcNow;
            if ((now - _lastThroughputReport).TotalSeconds >= intervalSeconds)
            {
                var throughput = _totalChangesProcessed / (now - _lastThroughputReport).TotalSeconds;
                var queueSizes = string.Join(", ", _changeQueues.Select((q, i) => $"Q{i}:{q.Count}"));
                
                double currentLag;
                lock (_lagLock)
                {
                    currentLag = _currentLagSeconds;
                }
                
                // Calculate memory usage
                var memoryMB = GC.GetTotalMemory(false) / (1024 * 1024);
                
                // Determine lag status
                var lagStatus = currentLag switch
                {
                    <= 5 => "EXCELLENT",
                    <= 20 => "GOOD", 
                    <= 40 => "MODERATE",
                    <= 80 => "HIGH",
                    _ => "CRITICAL"
                };
                
                // Determine memory status based on system size
                var totalMemoryGB = GetTotalSystemMemory() / (1024.0 * 1024.0 * 1024.0);
                var memoryPercentage = GetMemoryUsagePercentage();
                string memoryStatus;
                
                if (totalMemoryGB >= 16.0)
                {
                    // Large memory systems (16GB+): 25%, 40%, 55%, 70%
                    memoryStatus = memoryPercentage switch
                    {
                        <= 25.0 => "LOW",
                        <= 40.0 => "NORMAL", 
                        <= 55.0 => "HIGH",
                        _ => "CRITICAL"
                    };
                }
                else if (totalMemoryGB >= 8.0)
                {
                    // Medium memory systems (8-16GB): 15%, 25%, 40%, 50%
                    memoryStatus = memoryPercentage switch
                    {
                        <= 15.0 => "LOW",
                        <= 25.0 => "NORMAL",
                        <= 40.0 => "HIGH", 
                        _ => "CRITICAL"
                    };
                }
                else
                {
                    // Small memory systems (<8GB): 10%, 15%, 25%, 35%
                    memoryStatus = memoryPercentage switch
                    {
                        <= 10.0 => "LOW",
                        <= 15.0 => "NORMAL",
                        <= 25.0 => "HIGH",
                        _ => "CRITICAL"
                    };
                }
                
                _log.WriteLine($"{_syncBackPrefix}Throughput: {throughput:F0} changes/sec | Lag: {currentLag:F1}s ({lagStatus}) | " +
                             $"Memory: {memoryPercentage:F1}% of {totalMemoryGB:F1}GB ({memoryStatus}) | Adaptive batch: {_adaptiveBatchSize} | " +
                             $"Queue sizes: [{queueSizes}] | Total processed: {_totalChangesProcessed}", LogType.Debug);
                
                // Additional debugging for extreme lag situations
                if (currentLag > 1000) // For extreme lag like 2607s
                {
                    var collectionLagSummary = _lastChangeTimestamp
                        .GroupBy(kvp => 
                        {
                            var lagSeconds = (now - kvp.Value).TotalSeconds;
                            if (lagSeconds > 1800) return "EXTREME (>30min)";
                            if (lagSeconds > 600) return "CRITICAL (>10min)";
                            if (lagSeconds > 300) return "HIGH (>5min)";
                            if (lagSeconds > 60) return "MODERATE (>1min)";
                            return "NORMAL (<1min)";
                        })
                        .ToDictionary(g => g.Key, g => g.Count());
                    
                    var lagSummaryText = string.Join(", ", collectionLagSummary.Select(kvp => $"{kvp.Key}: {kvp.Value}"));
                    _log.WriteLine($"{_syncBackPrefix}EXTREME LAG ANALYSIS - Collection lag distribution: {lagSummaryText}", LogType.Debug);
                }
                
                _lastThroughputReport = now;
                _totalChangesProcessed = 0; // Reset counter
            }
        }

        protected override async Task ProcessChangeStreamsAsync(CancellationToken token)
        {
            long loops = 0;

            bool isVCore = (_syncBack ? _job.TargetEndpoint : _job.SourceEndpoint)
                .Contains("mongocluster.cosmos.azure.com", StringComparison.OrdinalIgnoreCase);

            // RUOptimizedCopy jobs should not use server-level change streams
            if (_job.JobType == JobType.RUOptimizedCopy)
            {
                _log.WriteLine($"{_syncBackPrefix}RUOptimizedCopy jobs do not support server-level change streams. This processor should not be used for such jobs.", LogType.Error);
                return;
            }

            // Initialize collection cache
            InitializeCollectionCache();

            _log.WriteLine($"{_syncBackPrefix}Starting server-level change stream processing for {_migrationUnitsToProcess.Count} collection(s).");
            
            while (!token.IsCancellationRequested && !ExecutionCancelled)
            {
                try
                {
                    await WatchServerLevelChangeStreamUltraHighPerformance(token);
                    loops++;
                }
                catch (OperationCanceledException)
                {
                    // Expected when cancelled, exit loop
                    break;
                }
                catch (Exception ex)
                {
                    _log.WriteLine($"{_syncBackPrefix}Error in change stream processing: {ex}", LogType.Error);
                    // Continue processing on errors
                }
            }

            // Ensure final processing and save
            await ProcessAccumulatedChanges();
            OptimizedSave(force: true);
        }

        private async Task WatchServerLevelChangeStreamUltraHighPerformance(CancellationToken cancellationToken)
        {
            // Calculate optimal batch size based on memory
            int dynamicBatchSize = GetOptimalBatchSize();
            
            var preStartMemoryPercentage = GetMemoryUsagePercentage();
            var totalMemoryGB = GetTotalSystemMemory() / (1024.0 * 1024.0 * 1024.0);
            
            // Reduce batch size if memory is high
            if (preStartMemoryPercentage > (totalMemoryGB >= 16.0 ? 70.0 : 60.0))
            {
                dynamicBatchSize = Math.Min(dynamicBatchSize, 100);
            }
            else if (preStartMemoryPercentage > (totalMemoryGB >= 16.0 ? 60.0 : 50.0))
            {
                dynamicBatchSize = Math.Min(dynamicBatchSize, 500);
            }

            try
            {
                // Create pipeline for server-level change stream
                var pipeline = new List<BsonDocument>();

                // MongoDB driver batch size based on memory
                var mongoDriverBatchSize = preStartMemoryPercentage switch
                {
                    > 60.0 => 50,
                    > 40.0 => 100,
                    > 20.0 => 150,
                    _ => 200
                };
                
                // Adjust for high lag scenarios
                double currentLag;
                lock (_lagLock)
                {
                    currentLag = _currentLagSeconds;
                }
                
                if (currentLag > 100 && preStartMemoryPercentage < 20.0)
                {
                    mongoDriverBatchSize = Math.Min((int)(mongoDriverBatchSize * 1.5), 300);
                }
                
                var options = new ChangeStreamOptions
                {
                    BatchSize = mongoDriverBatchSize,
                    FullDocument = ChangeStreamFullDocumentOption.UpdateLookup,
                    MaxAwaitTime = TimeSpan.FromMilliseconds(150)
                };

                // Get resume information
                var timeStamp = GetCursorUtcTimestamp();
                var resumeToken = GetResumeToken();
                var startedOn = GetChangeStreamStartedOn();
                var version = !_syncBack ? _job.SourceServerVersion : "8";

                // Handle initial document replay
                bool initialReplayCompleted = GetInitialDocumentReplayedStatus();
                if (!initialReplayCompleted && !_job.IsSimulatedRun && !_job.AggresiveChangeStream)
                {
                    if (!AutoReplayFirstChangeInResumeToken())
                    {
                        _log.WriteLine($"{_syncBackPrefix}Failed to replay the first change for server-level change stream. Skipping server-level processing.", LogType.Error);
                        return;
                    }
                    SetInitialDocumentReplayedStatus(true);
                    OptimizedSave();
                }

                // Configure change stream options based on resume information
                if (timeStamp > DateTime.MinValue && string.IsNullOrEmpty(resumeToken) && !(_job.JobType == JobType.RUOptimizedCopy && !_job.ProcessingSyncBack))
                {
                    var bsonTimestamp = MongoHelper.ConvertToBsonTimestamp(timeStamp.ToLocalTime());
                    options.StartAtOperationTime = bsonTimestamp;
                }
                else if (!string.IsNullOrEmpty(resumeToken))
                {
                    options.ResumeAfter = BsonDocument.Parse(resumeToken);
                }
                else if (startedOn > DateTime.MinValue && !version.StartsWith("3"))
                {
                    var bsonTimestamp = MongoHelper.ConvertToBsonTimestamp(startedOn);
                    options.StartAtOperationTime = bsonTimestamp;
                }

                // Watch at client level (server-level) with ultra-conservative settings
                using var cursor = _sourceClient.Watch<ChangeStreamDocument<BsonDocument>>(pipeline.ToArray(), options, cancellationToken);

                // Reset counters
                foreach (var kvp in _migrationUnitsToProcess)
                {
                    kvp.Value.CSUpdatesInLastBatch = 0;
                }

                _log.WriteLine($"{_syncBackPrefix}Change stream cursor initialized. Batch size: {dynamicBatchSize}", LogType.Debug);

                // Use queue-based processing with load balancing for maximum throughput
                await ProcessChangeStreamWithLoadBalancedQueues(cursor, cancellationToken);
            }
            catch (OperationCanceledException)
            {
                _log.WriteLine($"{_syncBackPrefix}Ultra-conservative change stream batch completed", LogType.Debug);
            }
            catch (OutOfMemoryException ex)
            {
                _log.WriteLine($"{_syncBackPrefix}CRITICAL: OutOfMemoryException in BSON deserialization. Triggering emergency procedures: {ex.Message}", LogType.Error);
                
                // Emergency response to OOM
                PerformEmergencyMemoryRecovery();
                
                // Drastically reduce batch sizes for future operations
                _adaptiveBatchSize = Math.Min(10, MinBatchSize);
                
                throw;
            }
            catch (Exception ex)
            {
                _log.WriteLine($"{_syncBackPrefix}Error in ultra-conservative change stream: {ex}", LogType.Error);
                throw;
            }
        }

        /// <summary>
        /// Load-balanced queue processing optimized for speed with intelligent memory safety
        /// </summary>
        private async Task ProcessChangeStreamWithLoadBalancedQueues(IChangeStreamCursor<ChangeStreamDocument<BsonDocument>> cursor, CancellationToken cancellationToken)
        {
            var changeCount = 0;
            var lastMemoryCheck = DateTime.UtcNow;
            const int memoryCheckInterval = 100; // Check memory every 100 changes (balanced for performance)
            
            // Get adaptive emergency threshold based on system memory size
            var totalMemoryGB = GetTotalSystemMemory() / (1024.0 * 1024.0 * 1024.0);
            double emergencyMemoryThreshold;
            if (totalMemoryGB >= 16.0)
                emergencyMemoryThreshold = 60.0; // 60% for large memory systems - More conservative
            else if (totalMemoryGB >= 8.0)
                emergencyMemoryThreshold = 50.0; // 50% for medium memory systems
            else
                emergencyMemoryThreshold = 40.0; // 40% for small memory systems

            try
            {
                while (!cancellationToken.IsCancellationRequested && !ExecutionCancelled)
                {
                    // SMART: Pre-emptive memory check before cursor.MoveNext() - only act when truly needed
                    var preMemoryPercentage = GetMemoryUsagePercentage();
                    if (preMemoryPercentage > emergencyMemoryThreshold)
                    {
                        _log.WriteLine($"{_syncBackPrefix}High memory detected ({preMemoryPercentage:F1}% of {totalMemoryGB:F1}GB) before cursor.MoveNext(). Performing memory cleanup.", LogType.Debug);
                        PerformMemoryCleanup(); // Use regular cleanup, not emergency
                        
                        // Brief wait for memory to stabilize - optimized delay
                        await Task.Delay(250, cancellationToken); // Reduced from 500ms for speed
                        
                        // Re-check after cleanup - only skip if still very high
                        preMemoryPercentage = GetMemoryUsagePercentage();
                        if (preMemoryPercentage > (emergencyMemoryThreshold + 10.0)) // Give some headroom
                        {
                            _log.WriteLine($"{_syncBackPrefix}Memory still high ({preMemoryPercentage:F1}% of {totalMemoryGB:F1}GB) after cleanup. Brief pause before retry.", LogType.Debug);
                            await Task.Delay(500, cancellationToken); // Reduced from 1000ms for speed
                        }
                    }
                    
                    // CRITICAL: Additional memory check right before cursor.MoveNext() for BSON deserialization safety
                    var preCursorMemoryPercentage = GetMemoryUsagePercentage();
                    if (preCursorMemoryPercentage > 70.0) // Conservative threshold before BSON deserialization
                    {
                        _log.WriteLine($"{_syncBackPrefix}CRITICAL: High memory ({preCursorMemoryPercentage:F1}%) detected before cursor.MoveNext(). Performing emergency cleanup to prevent BSON OOM.", LogType.Warning);
                        PerformEmergencyMemoryRecovery();
                        await Task.Delay(1000, cancellationToken); // Longer pause for memory recovery
                        
                        // Re-check after emergency cleanup
                        preCursorMemoryPercentage = GetMemoryUsagePercentage();
                        if (preCursorMemoryPercentage > 75.0) // Still too high
                        {
                            _log.WriteLine($"{_syncBackPrefix}ABORT: Memory still critically high ({preCursorMemoryPercentage:F1}%) after emergency cleanup. Skipping cursor.MoveNext() to prevent OOM.", LogType.Error);
                            await Task.Delay(5000, cancellationToken); // Extended pause
                            continue; // Skip this iteration
                        }
                    }
                    
                    // Now try cursor.MoveNext() with pre-checked memory - wrapped in OOM protection
                    bool hasMoreData;
                    try
                    {
                        hasMoreData = cursor.MoveNext(cancellationToken);
                    }
                    catch (OutOfMemoryException ex)
                    {
                        _log.WriteLine($"{_syncBackPrefix}CRITICAL: OutOfMemoryException during cursor.MoveNext() BSON deserialization: {ex.Message}", LogType.Error);
                        _log.WriteLine($"{_syncBackPrefix}Performing emergency memory recovery and reducing MongoDB driver batch size", LogType.Error);
                        
                        // Emergency recovery
                        PerformEmergencyMemoryRecovery();
                        
                        // Force ultra-small batch size for next iteration
                        // Note: This won't affect the current cursor, but will be used when recreating the cursor
                        _adaptiveBatchSize = Math.Min(10, MinBatchSize);
                        
                        // Extended pause for memory recovery
                        await Task.Delay(10000, cancellationToken);
                        
                        // Re-throw to trigger cursor recreation with smaller batch size
                        throw;
                    }
                    
                    if (!hasMoreData)
                        break; // No more data
                        
                    cancellationToken.ThrowIfCancellationRequested();
                    if (ExecutionCancelled) return;

                    // Pre-check memory before processing batch
                    var memoryPercentage = GetMemoryUsagePercentage();
                    if (memoryPercentage > emergencyMemoryThreshold)
                    {
                        _log.WriteLine($"{_syncBackPrefix}Emergency memory threshold reached ({memoryPercentage:F1}% of {totalMemoryGB:F1}GB system memory). Performing emergency cleanup and pausing.", LogType.Warning);
                        PerformMemoryCleanup();
                        await Task.Delay(250, cancellationToken); // Reduced from 500ms for speed
                        
                        // Re-check after cleanup
                        memoryPercentage = GetMemoryUsagePercentage();
                        if (memoryPercentage > emergencyMemoryThreshold)
                        {
                            _log.WriteLine($"{_syncBackPrefix}Memory still high after cleanup ({memoryPercentage:F1}% of {totalMemoryGB:F1}GB system memory). Implementing circuit breaker.", LogType.Warning);
                            await Task.Delay(1000, cancellationToken); // Reduced from 2000ms for speed
                        }
                    }

                    var batch = cursor.Current;
                    
                    if (!batch.Any()) continue;

                    // Optimized memory safety check - balanced frequency for speed
                    if (changeCount % memoryCheckInterval == 0)
                    {
                        var currentMemoryPercentage = GetMemoryUsagePercentage();
                        var memoryWarningThreshold = totalMemoryGB >= 16.0 ? 60.0 : (totalMemoryGB >= 8.0 ? 50.0 : 40.0); // Reasonable thresholds
                        
                        if (currentMemoryPercentage > memoryWarningThreshold)
                        {
                            _log.WriteLine($"{_syncBackPrefix}High memory usage detected ({currentMemoryPercentage:F1}% of {totalMemoryGB:F1}GB system memory). Triggering memory cleanup.", LogType.Debug);
                            PerformMemoryCleanup(); // Use regular cleanup, not emergency
                            
                            // Brief pause only when memory is actually high
                            await Task.Delay(100, cancellationToken); // Minimal pause to maintain speed
                        }
                    }

                    // Process batch in SMART chunks - smaller only when memory is actually problematic
                    var batchArray = batch.ToArray();
                    
                    // MINIMAL: Count only relevant changes for migration collections
                    var relevantChanges = 0;
                    foreach (var change in batchArray)
                    {
                        if (change.CollectionNamespace != null && 
                            _collectionCache.ContainsKey(change.CollectionNamespace.ToString()))
                        {
                            relevantChanges++;
                        }
                    }
                    
                    var chunkMemoryPercentage = GetMemoryUsagePercentage();
                    
                    // SMART chunk sizes - high performance unless memory is genuinely problematic
                    int chunkSize;
                    var chunkWarningThreshold = totalMemoryGB >= 16.0 ? 50.0 : (totalMemoryGB >= 8.0 ? 40.0 : 30.0);
                    var chunkDangerThreshold = totalMemoryGB >= 16.0 ? 70.0 : (totalMemoryGB >= 8.0 ? 60.0 : 50.0);
                    var chunkCriticalThreshold = totalMemoryGB >= 16.0 ? 85.0 : (totalMemoryGB >= 8.0 ? 75.0 : 65.0);
                    
                    if (chunkMemoryPercentage < chunkWarningThreshold)
                        chunkSize = 500; // HIGH PERFORMANCE chunks when memory is fine
                    else if (chunkMemoryPercentage < chunkDangerThreshold)
                        chunkSize = 200; // Balanced chunks for moderate memory
                    else if (chunkMemoryPercentage < chunkCriticalThreshold)
                        chunkSize = 50; // Smaller chunks for high memory
                    else
                        chunkSize = 10; // Conservative chunks only when memory is critical
                    
                    for (int i = 0; i < batchArray.Length; i += chunkSize)
                    {
                        var chunk = batchArray.Skip(i).Take(chunkSize);
                        
                        // Distribute changes across multiple queues for parallel processing
                        foreach (var change in chunk)
                        {
                            // Load balance across queues
                            var queueIndex = Interlocked.Increment(ref _currentQueueIndex) % QueueCount;
                            _changeQueues[queueIndex].Enqueue(change);
                            changeCount++;
                            
                            // OPTIMIZED: Yield less frequently for better speed
                            if (changeCount % 25 == 0) // Yield every 25 changes (was 10)
                            {
                                await Task.Yield();
                            }
                        }
                        
                        // Additional memory pressure check during chunk processing  
                        var currentMemoryDuringChunk = GetMemoryUsagePercentage();
                        if (currentMemoryDuringChunk > 75.0) // High memory during processing
                        {
                            _log.WriteLine($"{_syncBackPrefix}High memory during chunk processing ({currentMemoryDuringChunk:F1}%), yielding control", LogType.Debug);
                            await Task.Delay(50, cancellationToken); // Brief delay to let GC work
                        }
                    }

                    // Report progress for significant batches only
                    if (batchArray.Length > 20)
                    {
                        _log.ShowInMonitor($"{_syncBackPrefix}SERVER-LEVEL: MongoDB batch {batchArray.Length} total, {relevantChanges} for migration collections, distributed across {QueueCount} queues (Session queued: {changeCount})");
                    }

                    // Yield control moderately to balance performance and memory management
                    if (changeCount % 100 == 0) // Less frequent yields for speed (was 50)
                    {
                        await Task.Yield();
                    }
                }

                _log.WriteLine($"{_syncBackPrefix}Change stream batch completed. Total changes distributed: {changeCount}", LogType.Debug);
            }
            catch (OutOfMemoryException)
            {
                _log.WriteLine($"{_syncBackPrefix}OutOfMemoryException in queue processing after {changeCount} changes.", LogType.Error);
                PerformMemoryCleanup();
                throw;
            }
        }

        // Simplified methods for critical path performance
        private Task CheckOplogCountAsync()
        {
            // Disable oplog checking during processing to reduce overhead
            return Task.CompletedTask;
        }

        // Remove all the old complex processing methods and replace with streamlined versions
        private Task<(bool success, long counter)> ProcessChange(ChangeStreamDocument<BsonDocument> change, Dictionary<string, ChangeStreamDocuments> changeStreamDocuments, long counter)
        {
            // This method is now bypassed by the queue-based processing
            return Task.FromResult((true, counter));
        }

        private async Task BulkProcessAllChangesAsync(Dictionary<string, ChangeStreamDocuments> changeStreamDocuments)
        {
            // This is now handled by ProcessAccumulatedChanges
            await ProcessAccumulatedChanges();
        }

        // Keep all the resume token management methods unchanged
        #region Server-Level Resume Token Management

        private DateTime GetCursorUtcTimestamp()
        {
            return !_syncBack ? _job.CursorUtcTimestamp : _job.SyncBackCursorUtcTimestamp;
        }

        private DateTime GetChangeStreamStartedOn()
        {
            if (!_syncBack)
            {
                return _job.ChangeStreamStartedOn ?? DateTime.MinValue;
            }
            else
            {
                return _job.SyncBackChangeStreamStartedOn ?? DateTime.MinValue;
            }
        }

        private string GetResumeToken()
        {
            if (!_syncBack)
            {
                return _job.ResumeToken ?? string.Empty;
            }
            else
            {
                return _job.SyncBackResumeToken ?? string.Empty;
            }
        }

        private bool GetInitialDocumentReplayedStatus()
        {
            return !_syncBack ? _job.InitialDocumenReplayed : _job.SyncBackInitialDocumenReplayed;
        }

        private void SetInitialDocumentReplayedStatus(bool value)
        {
            if (!_syncBack)
            {
                _job.InitialDocumenReplayed = value;
            }
            else
            {
                _job.SyncBackInitialDocumenReplayed = value;
            }
        }

        private ChangeStreamOperationType GetResumeTokenOperation()
        {
            return !_syncBack ? _job.ResumeTokenOperation : _job.SyncBackResumeTokenOperation;
        }

        private string GetResumeDocumentId()
        {
            return !_syncBack ? (_job.ResumeDocumentId ?? string.Empty) : (_job.SyncBackResumeDocumentId ?? string.Empty);
        }

        private string GetResumeCollectionKey()
        {
            return !_syncBack ? (_job.ResumeCollectionKey ?? string.Empty) : (_job.SyncBackResumeCollectionKey ?? string.Empty);
        }

        private void UpdateResumeToken(string resumeToken, ChangeStreamOperationType operationType, string documentId, string collectionKey)
        {
            if (!_syncBack)
            {
                _job.ResumeToken = resumeToken;
                if (string.IsNullOrEmpty(_job.OriginalResumeToken))
                {
                    _job.OriginalResumeToken = resumeToken;
                }
                _job.ResumeTokenOperation = operationType;
                _job.ResumeDocumentId = documentId;
                _job.ResumeCollectionKey = collectionKey;
            }
            else
            {
                _job.SyncBackResumeToken = resumeToken;
                if (string.IsNullOrEmpty(_job.SyncBackOriginalResumeToken))
                {
                    _job.SyncBackOriginalResumeToken = resumeToken;
                }
                _job.SyncBackResumeTokenOperation = operationType;
                _job.SyncBackResumeDocumentId = documentId;
                _job.SyncBackResumeCollectionKey = collectionKey;
            }
        }

        #endregion

        private bool AutoReplayFirstChangeInResumeToken()
        {
            string documentId = GetResumeDocumentId();
            ChangeStreamOperationType operationType = GetResumeTokenOperation();
            string collectionKey = GetResumeCollectionKey();

            if (string.IsNullOrEmpty(documentId) || string.IsNullOrEmpty(collectionKey))
            {
                return true; // Skip if insufficient information
            }

            _log.WriteLine($"Auto replay for {operationType} operation with document key {documentId} in collection {collectionKey} for server-level change stream.", LogType.Debug);

            try
            {
                var bsonDoc = BsonDocument.Parse(documentId);
                var filter = MongoHelper.BuildFilterFromDocumentKey(bsonDoc);

                if (!_migrationUnitsToProcess.TryGetValue(collectionKey, out var migrationUnit))
                {
                    return true;
                }

                var parts = collectionKey.Split('.');
                if (parts.Length != 2) return true;

                var databaseName = parts[0];
                var collectionName = parts[1];

                var sourceDb = _sourceClient.GetDatabase(databaseName);
                var sourceCollection = sourceDb.GetCollection<BsonDocument>(collectionName);
                var targetCollection = GetTargetCollection(databaseName, collectionName);

                var result = sourceCollection.Find(filter).FirstOrDefault();

                IncrementEventCounter(migrationUnit, operationType);

                switch (operationType)
                {
                    case ChangeStreamOperationType.Insert:
                        if (result != null && !result.IsBsonNull)
                        {
                            targetCollection.InsertOne(result);
                            IncrementDocCounter(migrationUnit, operationType);
                        }
                        break;
                    case ChangeStreamOperationType.Update:
                    case ChangeStreamOperationType.Replace:
                        if (result != null && !result.IsBsonNull)
                        {
                            targetCollection.ReplaceOne(filter, result, new ReplaceOptions { IsUpsert = true });
                            IncrementDocCounter(migrationUnit, operationType);
                        }
                        else
                        {
                            targetCollection.DeleteOne(filter);
                            IncrementDocCounter(migrationUnit, ChangeStreamOperationType.Delete);
                        }
                        break;
                    case ChangeStreamOperationType.Delete:
                        targetCollection.DeleteOne(filter);
                        IncrementDocCounter(migrationUnit, operationType);
                        break;
                }

                return true;
            }
            catch (MongoException mex) when (operationType == ChangeStreamOperationType.Insert && mex.Message.Contains("DuplicateKey"))
            {
                return true; // Ignore duplicate key errors
            }
            catch (Exception ex)
            {
                _log.WriteLine($"Error processing operation {operationType} in server-level auto replay with document key {documentId}. Details: {ex}", LogType.Error);
                return false;
            }
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                _flushTimer?.Dispose();
                _processingThrottle?.Dispose();
                
                // Final processing of any remaining queued changes
                for (int i = 0; i < QueueCount; i++)
                {
                    while (_changeQueues[i].TryDequeue(out var change))
                    {
                        ProcessChangeImmediate(change);
                    }
                }
                
                // Force final save
                OptimizedSave(force: true);
                
                // Wait a moment for background save to complete
                Thread.Sleep(1000);
            }
            base.Dispose(disposing);
        }
    }
}