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
    public class CollectionLevelChangeStreamProcessor : ChangeStreamProcessor
    {
        // Queue manager for change stream processing
        private readonly ChangeStreamQueueManager _queueManager;

        // Lag monitoring and adaptive processing for collection-level streams
        private readonly Dictionary<string, DateTime> _lastChangeTimestamp = new();
        private readonly Dictionary<string, double> _collectionLagSeconds = new();
        private DateTime _lastLagCheck = DateTime.UtcNow;
        private const int LagCheckIntervalSeconds = 3; // More frequent lag checking
        private const double MaxAcceptableLagSeconds = 20; // Tighter lag tolerance

        // Adaptive batch sizing based on lag - balanced for memory safety (will migrate to _queueManager)
        private volatile int _adaptiveBatchSize;
        private const int MinBatchSize = 50; // Reasonable minimum for throughput
        private const int MaxBatchSize = 500; // Reduced to prevent OutOfMemoryException
        private const int MaxQueueSize = 50000; // Increased buffer for high-volume collections

        // Queue-based processing for better parallelism (like server-level)
        private readonly ConcurrentDictionary<string, ConcurrentQueue<ChangeStreamDocument<BsonDocument>>> _collectionQueues = new();
        private readonly ConcurrentDictionary<string, Timer> _collectionFlushTimers = new();
        private readonly ConcurrentDictionary<string, bool> _isProcessingQueue = new();
        private readonly SemaphoreSlim _processingThrottle = new SemaphoreSlim(50); // Increased for higher throughput

        public CollectionLevelChangeStreamProcessor(Log log, MongoClient sourceClient, MongoClient targetClient, JobList jobList, MigrationJob job, MigrationSettings config, bool syncBack = false)
            : base(log, sourceClient, targetClient, jobList, job, config, syncBack)
        {
            // Initialize queue manager for shared change stream processing
            _queueManager = new ChangeStreamQueueManager(_log, _syncBackPrefix, 50, _config.ChangeStreamMaxDocsInBatch);
            
            // Initialize adaptive batch size - using queue manager
            _adaptiveBatchSize = _queueManager.GetAdaptiveBatchSize();
            
            // Queues will be initialized lazily when collections are first processed
        }

        protected override async Task ProcessChangeStreamsAsync(CancellationToken token)
        {
            bool isVCore = (_syncBack ? _job.TargetEndpoint : _job.SourceEndpoint)
                .Contains("mongocluster.cosmos.azure.com", StringComparison.OrdinalIgnoreCase);
                      
            int index = 0;

            // Get the latest sorted keys with lag-aware prioritization
            var sortedKeys = GetLagAwareSortedKeys();

            _log.WriteLine($"{_syncBackPrefix}Starting collection-level change stream processing for {sortedKeys.Count} collection(s). Each round-robin batch will process {Math.Min(_concurrentProcessors, sortedKeys.Count)} collections.");

            long loops = 0;
            bool oplogSuccess = true;

            while (!token.IsCancellationRequested && !ExecutionCancelled)
            {
                var totalKeys = sortedKeys.Count;

                while (index < totalKeys && !token.IsCancellationRequested && !ExecutionCancelled)
                {
                    var tasks = new List<Task>();
                    var collectionProcessed = new List<string>();

                    // Determine the batch with lag-aware prioritization
                    var batchKeys = sortedKeys.Skip(index).Take(_concurrentProcessors).ToList();
                    var batchUnits = batchKeys
                        .Select(k => _migrationUnitsToProcess.TryGetValue(k, out var unit) ? unit : null)
                        .Where(u => u != null)
                        .Cast<MigrationUnit>() // Cast to non-nullable
                        .ToList();

                    // Calculate adaptive timing based on lag
                    UpdateLagMetrics();
                    int seconds = CalculateAdaptiveBatchDuration(batchUnits);

                    foreach (var key in batchKeys)
                    {
                        if (_migrationUnitsToProcess.TryGetValue(key, out var unit))
                        {
                            collectionProcessed.Add(key);
                            unit.CSLastBatchDurationSeconds = seconds;
                            tasks.Add(Task.Run(() => ProcessCollectionChangeStream(unit, true, seconds), token));
                        }
                    }

                    // Calculate average lag only for active collections (those with recent updates)
                    var activeCollections = _migrationUnitsToProcess
                        .Where(kvp => kvp.Value.CSUpdatesInLastBatch > 0)
                        .Select(kvp => kvp.Key)
                        .ToList();
                    
                    var avgLag = activeCollections.Count > 0
                        ? activeCollections.Average(key => _collectionLagSeconds.GetValueOrDefault(key, 0))
                        : 0;
                    
                    _log.WriteLine($"{_syncBackPrefix}Processing change streams for collections: {string.Join(", ", collectionProcessed)}. Batch Duration {seconds} seconds | Avg Lag: {avgLag:F1}s (Active: {activeCollections.Count}/{_migrationUnitsToProcess.Count})", LogType.Debug);

                    await Task.WhenAll(tasks);

                    index += _concurrentProcessors;

                    // Minimal pause for maximum throughput - only 10ms
                    Thread.Sleep(10);
                }

                index = 0;
                // Re-sort with lag-aware prioritization after all processing is complete
                sortedKeys = GetLagAwareSortedKeys();

                loops++;
                
                // Reduce oplog checking frequency for better performance
                if (loops % 8 == 0 && oplogSuccess && !isVCore && !_syncBack)
                {
                    foreach (var unit in _migrationUnitsToProcess)
                    {
                        if (unit.Value.CursorUtcTimestamp > DateTime.MinValue)
                        {
                            long secondsSinceEpoch = new DateTimeOffset(unit.Value.CursorUtcTimestamp.ToLocalTime()).ToUnixTimeSeconds();

                            _ = Task.Run(() =>
                            {
                                oplogSuccess = MongoHelper.GetPendingOplogCountAsync(_log, _sourceClient, secondsSinceEpoch, unit.Key);
                            });
                            if (!oplogSuccess)
                                break;
                        }
                    }
                }
            }
        }

        /// <summary>
        /// Get collections sorted by lag and activity for prioritized processing
        /// Only apply lag multiplier to active collections
        /// </summary>
        private List<string> GetLagAwareSortedKeys()
        {
            return _migrationUnitsToProcess
                .OrderByDescending(kvp =>
                {
                    var key = kvp.Key;
                    var unit = kvp.Value;
                    
                    // Only apply lag multiplier to collections with recent activity
                    if (unit.CSUpdatesInLastBatch > 0)
                    {
                        var lagMultiplier = _collectionLagSeconds.GetValueOrDefault(key, 0) > MaxAcceptableLagSeconds ? 10 : 1;
                        return unit.CSNormalizedUpdatesInLastBatch * lagMultiplier;
                    }
                    
                    // Idle collections get zero priority
                    return 0;
                })
                .Select(kvp => kvp.Key)
                .ToList();
        }

        /// <summary>
        /// Ensure queue and timer are initialized for a collection
        /// </summary>
        private void EnsureQueueInitialized(string collectionKey)
        {
            if (!_collectionQueues.ContainsKey(collectionKey))
            {
                _collectionQueues[collectionKey] = new ConcurrentQueue<ChangeStreamDocument<BsonDocument>>();
                _isProcessingQueue[collectionKey] = false;

                // Initialize timer with lag-based interval
                var flushInterval = TimeSpan.FromMilliseconds(100); // Start with 100ms, will adjust based on lag
                _collectionFlushTimers[collectionKey] = new Timer(
                    _ => 
                    {
                        // Fire and forget - don't await to avoid timer callback issues
                        _ = Task.Run(() => ProcessQueuedChangesAsync(collectionKey));
                    },
                    null,
                    flushInterval,
                    flushInterval);
                
                _log.ShowInMonitor($"{_syncBackPrefix}Initialized queue for {collectionKey}");
            }
        }

        /// <summary>
        /// Update lag metrics for all collections
        /// </summary>
        private void UpdateLagMetrics()
        {
            var now = DateTime.UtcNow;
            if ((now - _lastLagCheck).TotalSeconds < LagCheckIntervalSeconds)
                return;

            _lastLagCheck = now;

            foreach (var kvp in _migrationUnitsToProcess)
            {
                var key = kvp.Key;
                var unit = kvp.Value;
                
                var lastChangeTime = _lastChangeTimestamp.GetValueOrDefault(key, now);
                var lagSeconds = (now - lastChangeTime).TotalSeconds;
                
                _collectionLagSeconds[key] = lagSeconds;

                if (lagSeconds > MaxAcceptableLagSeconds)
                {
                    _log.ShowInMonitor($"{_syncBackPrefix}High lag detected for {key}: {lagSeconds:F1} seconds");
                }
            }
        }

        /// <summary>
        /// Process queued changes for a specific collection with parallel batch processing
        /// </summary>
        private async Task ProcessQueuedChangesAsync(string collectionKey)
        {
            // Prevent concurrent processing of the same queue
            if (!_isProcessingQueue.TryUpdate(collectionKey, true, false))
                return;

            try
            {
                if (!_collectionQueues.TryGetValue(collectionKey, out var queue))
                    return;

                if (!_migrationUnitsToProcess.TryGetValue(collectionKey, out var mu))
                    return;

                // Process in multiple parallel batches if queue is large
                const int OptimalBatchSize = 1000; // Process 1000 items per batch
                var queueSize = queue.Count;
                
                // If queue is empty, exit without changing metrics
                // CSUpdatesInLastBatch should retain its value until new changes are processed
                if (queueSize == 0)
                {
                    _isProcessingQueue[collectionKey] = false;
                    return;
                }

                // For large queues, process multiple batches in parallel
                int numberOfBatches = Math.Min(5, (queueSize / OptimalBatchSize) + 1); // Up to 5 parallel batches
                var batchTasks = new List<Task<int>>(); // Track items processed per batch
                int totalItemsToProcess = 0;

                for (int i = 0; i < numberOfBatches; i++)
                {
                    // Dequeue a batch of changes
                    var batchChanges = new List<ChangeStreamDocument<BsonDocument>>();
                    while (queue.TryDequeue(out var change) && batchChanges.Count < OptimalBatchSize)
                    {
                        batchChanges.Add(change);
                    }

                    if (batchChanges.Count == 0)
                        break;

                    totalItemsToProcess += batchChanges.Count;

                    // Process this batch in parallel
                    var task = ProcessChangeBatchAsync(collectionKey, batchChanges, mu);
                    batchTasks.Add(task);
                }

                if (batchTasks.Count > 0)
                {
                    _log.ShowInMonitor($"{_syncBackPrefix}Processing {batchTasks.Count} parallel batches for {collectionKey} (total {totalItemsToProcess} items)");
                    var processedCounts = await Task.WhenAll(batchTasks);
                    
                    // Accumulate the count processed in this flush cycle
                    mu.CSUpdatesInLastBatch += processedCounts.Sum();
                    
                    // Calculate normalized rate
                    var elapsedSeconds = (DateTime.UtcNow - _lastChangeTimestamp.GetValueOrDefault(collectionKey, DateTime.UtcNow)).TotalSeconds;
                    if (elapsedSeconds > 0)
                    {
                        mu.CSNormalizedUpdatesInLastBatch = (long)(mu.CSUpdatesInLastBatch / elapsedSeconds);
                    }
                    else
                    {
                        mu.CSNormalizedUpdatesInLastBatch = mu.CSUpdatesInLastBatch;
                    }
                }

                _isProcessingQueue[collectionKey] = false;
            }
            catch (Exception ex)
            {
                _log.WriteLine($"{_syncBackPrefix}Error processing queued changes for {collectionKey}. Details: {ex}", LogType.Error);
                _isProcessingQueue[collectionKey] = false;
            }
        }

        /// <summary>
        /// Process a single batch of changes
        /// </summary>
        /// <returns>Number of items successfully processed</returns>
        private async Task<int> ProcessChangeBatchAsync(string collectionKey, List<ChangeStreamDocument<BsonDocument>> changes, MigrationUnit mu)
        {
            if (changes.Count == 0)
                return 0;

            try
            {
                // Get target collection
                var parts = collectionKey.Split('.');
                if (parts.Length != 2)
                    return 0;

                var databaseName = parts[0];
                var collectionName = parts[1];

                IMongoDatabase targetDb;
                IMongoCollection<BsonDocument> targetCollection;

                if (!_syncBack)
                {
                    if (!_job.IsSimulatedRun)
                    {
                        targetDb = _targetClient.GetDatabase(databaseName);
                        targetCollection = targetDb.GetCollection<BsonDocument>(collectionName);
                    }
                    else
                    {
                        // In simulated run, create a dummy collection reference for metrics tracking
                        // The actual writes will be skipped in MongoHelper methods
                        targetDb = _targetClient.GetDatabase(databaseName);
                        targetCollection = targetDb.GetCollection<BsonDocument>(collectionName);
                    }
                }
                else
                {
                    targetDb = _sourceClient.GetDatabase(databaseName);
                    targetCollection = targetDb.GetCollection<BsonDocument>(collectionName);
                }

                // Categorize changes and track the last change for resume token
                var changeStreamDocuments = new ChangeStreamDocuments();
                ChangeStreamDocument<BsonDocument>? lastChange = null;
                
                foreach (var change in changes)
                {
                    lastChange = change; // Track the last change for resume token
                    
                    switch (change.OperationType)
                    {
                        case ChangeStreamOperationType.Insert:
                            changeStreamDocuments.DocsToBeInserted.Add(change);
                            break;
                        case ChangeStreamOperationType.Update:
                        case ChangeStreamOperationType.Replace:
                            changeStreamDocuments.DocsToBeUpdated.Add(change);
                            break;
                        case ChangeStreamOperationType.Delete:
                            changeStreamDocuments.DocsToBeDeleted.Add(change);
                            break;
                    }
                }

                // Wait for semaphore to throttle concurrent operations
                await _processingThrottle.WaitAsync();
                
                try
                {
                    // Process with optimized sub-batches
                    var subBatchSize = Math.Max(10, Math.Min(100, changes.Count / 5));

                    await BulkProcessChangesAsync(
                        mu,
                        targetCollection,
                        insertEvents: changeStreamDocuments.DocsToBeInserted,
                        updateEvents: changeStreamDocuments.DocsToBeUpdated,
                        deleteEvents: changeStreamDocuments.DocsToBeDeleted,
                        batchSize: subBatchSize);

                    // Don't update CSUpdatesInLastBatch here - it's aggregated in ProcessQueuedChangesAsync
                    
                    // Update lag tracking
                    _lastChangeTimestamp[collectionKey] = DateTime.UtcNow;

                    // Update resume token and timestamp from last change in this batch
                    if (lastChange != null && lastChange.ResumeToken != null && lastChange.ResumeToken != BsonNull.Value)
                    {
                        if (!_syncBack)
                            mu.ResumeToken = lastChange.ResumeToken.ToJson();
                        else
                            mu.SyncBackResumeToken = lastChange.ResumeToken.ToJson();

                        _resumeTokenCache.AddOrUpdate(
                            collectionKey,
                            lastChange.ResumeToken.ToJson(),
                            (_, _) => lastChange.ResumeToken.ToJson());
                    }

                    // Update timestamp from last change
                    if (lastChange != null)
                    {
                        DateTime timeStamp = DateTime.UtcNow;
                        if (!_job.SourceServerVersion.StartsWith("3") && lastChange.ClusterTime != null)
                        {
                            timeStamp = MongoHelper.BsonTimestampToUtcDateTime(lastChange.ClusterTime);
                        }
                        else if (!_job.SourceServerVersion.StartsWith("3") && lastChange.WallTime != null)
                        {
                            timeStamp = lastChange.WallTime.Value;
                        }

                        if (!_syncBack)
                            mu.CursorUtcTimestamp = timeStamp;
                        else
                            mu.SyncBackCursorUtcTimestamp = timeStamp;
                    }
                    
                    // Return count of items processed successfully
                    return changes.Count;
                }
                finally
                {
                    _processingThrottle.Release();
                }
            }
            catch (Exception ex)
            {
                _log.WriteLine($"{_syncBackPrefix}Error processing batch for {collectionKey}. Details: {ex}", LogType.Error);
                return 0; // Return 0 on error
            }
        }

        /// <summary>
        /// Calculate adaptive batch duration based on collection lag
        /// </summary>
        private int CalculateAdaptiveBatchDuration(List<MigrationUnit> batchUnits)
        {
            var totalUpdatesInBatch = batchUnits.Sum(u => u.CSNormalizedUpdatesInLastBatch);
            
            // Only consider active collections (with recent updates) for total calculation
            var totalUpdatesInAll = _migrationUnitsToProcess
                .Where(kvp => kvp.Value.CSUpdatesInLastBatch > 0)
                .Sum(kvp => kvp.Value.CSNormalizedUpdatesInLastBatch);

            // Calculate base time factor
            float timeFactor = totalUpdatesInAll > 0 ? (float)totalUpdatesInBatch / totalUpdatesInAll : 1;

            // Check if any collection in batch has high lag (only for active collections)
            var maxLagInBatch = batchUnits
                .Where(u => u.CSUpdatesInLastBatch > 0) // Only active collections
                .Select(unit =>
                {
                    var key = $"{unit.DatabaseName}.{unit.CollectionName}";
                    return _collectionLagSeconds.GetValueOrDefault(key, 0);
                })
                .DefaultIfEmpty(0)
                .Max();

            // ULTRA-AGGRESSIVE reduction for high lag - process as fast as possible
            if (maxLagInBatch > 100) // Extreme lag
            {
                timeFactor *= 0.1f; // Process 10x faster
            }
            else if (maxLagInBatch > MaxAcceptableLagSeconds) // High lag
            {
                timeFactor *= 0.2f; // Process 5x faster
            }
            else if (maxLagInBatch > MaxAcceptableLagSeconds / 2) // Medium lag
            {
                timeFactor *= 0.4f; // Process 2.5x faster
            }

            var baseDuration = GetBatchDurationInSeconds(timeFactor);
            var minDuration = Math.Max(1, _config.ChangeStreamBatchDurationMin); // Use configured minimum
            var maxDuration = Math.Max(minDuration, _config.ChangeStreamBatchDuration / 4); // Use 1/4 of configured max
            return Math.Max(minDuration, Math.Min(baseDuration, maxDuration));
        }

        private void ProcessCollectionChangeStream(MigrationUnit mu, bool IsCSProcessingRun = false, int seconds = 0)
        {
            try
            {
                string databaseName = mu.DatabaseName;
                string collectionName = mu.CollectionName;
                string collectionKey = $"{databaseName}.{collectionName}";

                IMongoDatabase sourceDb;
                IMongoDatabase targetDb;

                IMongoCollection<BsonDocument>? sourceCollection = null;
                IMongoCollection<BsonDocument>? targetCollection = null;

                if (!_syncBack)
                {
                    sourceDb = _sourceClient.GetDatabase(databaseName);
                    sourceCollection = sourceDb.GetCollection<BsonDocument>(collectionName);

                    if (!_job.IsSimulatedRun)
                    {
                        targetDb = _targetClient.GetDatabase(databaseName);
                        targetCollection = targetDb.GetCollection<BsonDocument>(collectionName);
                    }
                }
                else
                {
                    // For sync back, we use the source collection as the target and vice versa
                    targetDb = _sourceClient.GetDatabase(databaseName);
                    targetCollection = targetDb.GetCollection<BsonDocument>(collectionName);

                    sourceDb = _targetClient.GetDatabase(databaseName);
                    sourceCollection = sourceDb.GetCollection<BsonDocument>(collectionName);
                }

                try
                {
                    // Memory-safe batch sizing with reduced limits to prevent OOM - using queue manager
                    var collectionLag = _collectionLagSeconds.GetValueOrDefault(collectionKey, 0);
                    var currentAdaptive = _queueManager.GetAdaptiveBatchSize();
                    var adaptiveBatchSize = ChangeStreamQueueManager.MinBatchSize;
                    
                    if (collectionLag > 100) // Extreme lag
                    {
                        adaptiveBatchSize = Math.Min(200, currentAdaptive); // Reduced from 3000
                    }
                    else if (collectionLag > MaxAcceptableLagSeconds) // High lag
                    {
                        adaptiveBatchSize = Math.Min(150, currentAdaptive); // Reduced from 2000
                    }
                    else if (collectionLag > MaxAcceptableLagSeconds / 2) // Medium lag
                    {
                        adaptiveBatchSize = Math.Min(100, currentAdaptive); // Reduced from 1000
                    }
                    else
                    {
                        adaptiveBatchSize = Math.Min(75, currentAdaptive); // Reduced from 500
                    }

                    // Memory-safe await time - prevent extremely large batches
                    var maxAwaitTime = collectionLag > 100 ? 100 :  // Increased from 25ms
                                      collectionLag > 50 ? 150 :    // Increased from 50ms
                                      collectionLag > 20 ? 200 :    // Increased from 75ms
                                      250;                          // Increased from 100ms

                    ChangeStreamOptions options = new ChangeStreamOptions 
                    { 
                        BatchSize = adaptiveBatchSize, 
                        FullDocument = ChangeStreamFullDocumentOption.UpdateLookup,
                        MaxAwaitTime = TimeSpan.FromMilliseconds(maxAwaitTime)
                    };

                    DateTime startedOn;
                    DateTime timeStamp;
                    string resumeToken = string.Empty;
                    string? version = string.Empty;
                    if (!_syncBack)
                    {
                        timeStamp = mu.CursorUtcTimestamp;
                        resumeToken = mu.ResumeToken ?? string.Empty;
                        version = _job.SourceServerVersion;
                        if (mu.ChangeStreamStartedOn.HasValue)
                        {
                            startedOn = mu.ChangeStreamStartedOn.Value;
                        }
                        else
                        {
                            startedOn = DateTime.MinValue;
                        }
                    }
                    else
                    {
                        timeStamp = mu.SyncBackCursorUtcTimestamp;
                        resumeToken = mu.SyncBackResumeToken ?? string.Empty;
                        version = "8";
                        if (mu.SyncBackChangeStreamStartedOn.HasValue)
                        {
                            startedOn = mu.SyncBackChangeStreamStartedOn.Value;
                        }
                        else
                        {
                            startedOn = DateTime.MinValue;
                        }
                    }

                    if (!mu.InitialDocumenReplayed && !_job.IsSimulatedRun && !_job.AggresiveChangeStream)
                    {
                        if (targetCollection == null)
                        {
                            var targetDb2 = _targetClient.GetDatabase(databaseName);
                            targetCollection = targetDb2.GetCollection<BsonDocument>(collectionName);
                        }
                        if (AutoReplayFirstChangeInResumeToken(mu.ResumeDocumentId, mu.ResumeTokenOperation, sourceCollection!, targetCollection!, mu))
                        {
                            mu.InitialDocumenReplayed = true;
                            _jobList?.Save();
                        }
                        else
                        {
                            _log.WriteLine($"{_syncBackPrefix}Failed to replay the first change for {sourceCollection!.CollectionNamespace}. Skipping change stream processing for this collection.", LogType.Warning);
                            throw new Exception($"Failed to replay the first change for {sourceCollection!.CollectionNamespace}. Skipping change stream processing for this collection.");
                        }
                    }

                    if (timeStamp > DateTime.MinValue && !mu.ResetChangeStream && resumeToken == null && !(_job.JobType == JobType.RUOptimizedCopy && !_job.ProcessingSyncBack))
                    {
                        var bsonTimestamp = MongoHelper.ConvertToBsonTimestamp(timeStamp.ToLocalTime());
                        options = new ChangeStreamOptions 
                        { 
                            BatchSize = adaptiveBatchSize, 
                            FullDocument = ChangeStreamFullDocumentOption.UpdateLookup, 
                            StartAtOperationTime = bsonTimestamp,
                            MaxAwaitTime = TimeSpan.FromMilliseconds(maxAwaitTime)
                        };
                    }
                    else if (!string.IsNullOrEmpty(resumeToken) && !mu.ResetChangeStream)
                    {
                        options = new ChangeStreamOptions 
                        { 
                            BatchSize = adaptiveBatchSize, 
                            FullDocument = ChangeStreamFullDocumentOption.UpdateLookup, 
                            ResumeAfter = BsonDocument.Parse(resumeToken),
                            MaxAwaitTime = TimeSpan.FromMilliseconds(maxAwaitTime)
                        };
                    }
                    else if (string.IsNullOrEmpty(resumeToken) && version.StartsWith("3"))
                    {
                        options = new ChangeStreamOptions 
                        { 
                            BatchSize = adaptiveBatchSize, 
                            FullDocument = ChangeStreamFullDocumentOption.UpdateLookup,
                            MaxAwaitTime = TimeSpan.FromMilliseconds(maxAwaitTime)
                        };
                    }
                    else if (startedOn > DateTime.MinValue && !version.StartsWith("3"))
                    {
                        var bsonTimestamp = MongoHelper.ConvertToBsonTimestamp((DateTime)startedOn);
                        options = new ChangeStreamOptions 
                        { 
                            BatchSize = adaptiveBatchSize, 
                            FullDocument = ChangeStreamFullDocumentOption.UpdateLookup, 
                            StartAtOperationTime = bsonTimestamp,
                            MaxAwaitTime = TimeSpan.FromMilliseconds(maxAwaitTime)
                        };
                        if (mu.ResetChangeStream)
                        {
                            ResetCounters(mu);
                        }

                        mu.ResetChangeStream = false;
                    }

                    if (seconds == 0)
                        seconds = GetBatchDurationInSeconds(.5f);

                    // Respect configured minimum duration
                    var minDuration = Math.Max(1, _config.ChangeStreamBatchDurationMin);
                    var maxDuration = Math.Max(minDuration, _config.ChangeStreamBatchDuration / 2);
                    var effectiveTimeout = Math.Max(minDuration, Math.Min(seconds, maxDuration));
                    var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(effectiveTimeout));
                    CancellationToken cancellationToken = cancellationTokenSource.Token;

                    _log.ShowInMonitor($"{_syncBackPrefix}Monitoring change stream with optimized batch for {sourceCollection!.CollectionNamespace}. Batch Duration {effectiveTimeout} seconds | Lag: {_collectionLagSeconds.GetValueOrDefault(collectionKey, 0):F1}s");

                    if (_job.IsSimulatedRun && targetCollection == null)
                    {
                        targetCollection = sourceCollection;
                    }

                    WatchCollectionAsync(mu, options, sourceCollection!, targetCollection!, cancellationToken, effectiveTimeout).GetAwaiter().GetResult();
                }
                catch (OperationCanceledException)
                {
                    // A new batch will be started. do nothing
                }
                catch (OutOfMemoryException ex)
                {
                    _log.WriteLine($"{_syncBackPrefix}OutOfMemory error processing change stream for {sourceCollection!.CollectionNamespace}. Reducing batch size and retrying...", LogType.Error);
                    _log.ShowInMonitor($"{_syncBackPrefix}OOM during change stream processing. Collection: {collectionKey}, Current adaptive batch: {_adaptiveBatchSize}. Details: {ex}");
                    
                    // Reduce adaptive batch size to prevent recurrence - using queue manager
                    _queueManager.ReduceBatchSize(2);
                    _adaptiveBatchSize = _queueManager.GetAdaptiveBatchSize();
                    
                    // Force garbage collection to recover memory - using queue manager
                    _queueManager.PerformEmergencyMemoryRecovery();
                    
                    _log.WriteLine($"{_syncBackPrefix}Reduced adaptive batch size to {_adaptiveBatchSize}. Memory cleaned. Will retry with smaller batches.");
                }
                catch (MongoCommandException ex) when (ex.ToString().Contains("Resume of change stream was not possible"))
                {
                    _log.WriteLine($"{_syncBackPrefix}Oplog is full. Error processing change stream for {sourceCollection.CollectionNamespace}. Details: {ex}", LogType.Error);
                    _log.ShowInMonitor($"{_syncBackPrefix}Oplog is full. Error processing change stream for {sourceCollection.CollectionNamespace}. Details: {ex}");
                }
                catch (MongoCommandException ex) when (ex.Message.Contains("Expired resume token") || ex.Message.Contains("cursor"))
                {
                    _log.WriteLine($"{_syncBackPrefix}Resume token has expired or cursor is invalid for {sourceCollection.CollectionNamespace}.", LogType.Error);
                    _log.ShowInMonitor($"{_syncBackPrefix}Resume token has expired or cursor is invalid for {sourceCollection.CollectionNamespace}.");
                }
            }
            catch (Exception ex)
            {
                _log.WriteLine($"{_syncBackPrefix}Error processing change stream for {mu.DatabaseName}.{mu.CollectionName}. Details: {ex}", LogType.Error);
            }
        }

        private async Task WatchCollectionAsync(
             MigrationUnit mu,
             ChangeStreamOptions options,
             IMongoCollection<BsonDocument> sourceCollection,
             IMongoCollection<BsonDocument> targetCollection,
             CancellationToken cancellationToken,
             int timeoutInSec)
        {
            long counter = 0;
            string collectionKey = $"{sourceCollection.Database.DatabaseNamespace.DatabaseName}.{sourceCollection.CollectionNamespace.CollectionName}";

            BsonDocument userFilterDoc = string.IsNullOrWhiteSpace(mu.UserFilter)
                ? new BsonDocument()
                : BsonDocument.Parse(mu.UserFilter);

            //  reset metrics to 0
            mu.CSUpdatesInLastBatch = 0;
            mu.CSNormalizedUpdatesInLastBatch = 0;

            // Ensure queue is initialized for this collection
            EnsureQueueInitialized(collectionKey);

            // Get the queue for this collection
            if (!_collectionQueues.TryGetValue(collectionKey, out var queue))
            {
                _log.WriteLine($"{_syncBackPrefix}Queue not found for {collectionKey}", LogType.Error);
                return;
            }

            // Adjust timer based on lag
            var collectionLag = _collectionLagSeconds.GetValueOrDefault(collectionKey, 0);
            TimeSpan flushInterval;
            
            if (collectionLag > 100) // Extreme lag
            {
                flushInterval = TimeSpan.FromMilliseconds(50); // Flush every 50ms
            }
            else if (collectionLag > MaxAcceptableLagSeconds) // High lag
            {
                flushInterval = TimeSpan.FromMilliseconds(100); // Flush every 100ms
            }
            else if (collectionLag > MaxAcceptableLagSeconds / 2) // Medium lag
            {
                flushInterval = TimeSpan.FromMilliseconds(200); // Flush every 200ms
            }
            else
            {
                flushInterval = TimeSpan.FromMilliseconds(500); // Normal - flush every 500ms
            }

            // Update timer interval
            if (_collectionFlushTimers.TryGetValue(collectionKey, out var timer))
            {
                timer.Change(flushInterval, flushInterval);
            }

            try
            {
                var pipeline = (_job.JobType == JobType.RUOptimizedCopy)
                    ? new List<BsonDocument>
                      {
                  new("$match", new BsonDocument
                      {
                          { "operationType", new BsonDocument("$in", new BsonArray { "insert", "update", "replace", "delete" }) },
                          { "ns.coll", sourceCollection.CollectionNamespace.CollectionName }
                      }),
                  new("$project", new BsonDocument
                  {
                      { "operationType", 1 },
                      { "_id", 1 },
                      { "fullDocument", 1 },
                      { "ns", 1 },
                      { "documentKey", 1 }
                  })
                      }
                    : new List<BsonDocument>();

                using var cursor = await sourceCollection.WatchAsync<ChangeStreamDocument<BsonDocument>>(pipeline, options, cancellationToken);

                DateTime lastActivity = DateTime.UtcNow;
                // Ultra-aggressive idle timeout for maximum responsiveness
                TimeSpan idleTimeout = TimeSpan.FromSeconds(Math.Min(timeoutInSec, 2)); // Max 2 seconds idle

                while (!cancellationToken.IsCancellationRequested)
                {
                    // Ultra-responsive unblocking: return control every 1 second max for high-throughput processing
                    var moveNextTask = cursor.MoveNextAsync(cancellationToken);
                    var completedTask = await Task.WhenAny(moveNextTask, Task.Delay(TimeSpan.FromSeconds(1), cancellationToken));

                    if (completedTask != moveNextTask)
                    {
                        // Trigger immediate flush before returning
                        await ProcessQueuedChangesAsync(collectionKey);
                        return;
                    }

                    if (!await moveNextTask)
                    {
                        // Trigger immediate flush before returning
                        await ProcessQueuedChangesAsync(collectionKey);
                        return;
                    }

                    if (!cursor.Current.Any())
                    {
                        if (DateTime.UtcNow - lastActivity > idleTimeout)
                        {

                            // Trigger immediate flush before returning
                            await ProcessQueuedChangesAsync(collectionKey);
                            return;
                        }
                        continue;
                    }

                    lastActivity = DateTime.UtcNow;

                    foreach (var change in cursor.Current)
                    {
                        cancellationToken.ThrowIfCancellationRequested();
                        if (ExecutionCancelled)
                        {
                            // Trigger immediate flush before returning
                            await ProcessQueuedChangesAsync(collectionKey);
                            return;
                        }

                        _resumeTokenCache.TryGetValue($"{sourceCollection!.CollectionNamespace}", out string? token);
                        var lastProcessedToken = token ?? string.Empty;

                        if (lastProcessedToken == change.ResumeToken.ToJson() && _job.JobType != JobType.RUOptimizedCopy)
                        {
                            mu.CSUpdatesInLastBatch = 0;
                            mu.CSNormalizedUpdatesInLastBatch = 0;
                            // Trigger immediate flush before returning
                            await ProcessQueuedChangesAsync(collectionKey);
                            return;
                        }

                        // Check queue size and trigger early flush to maintain throughput
                        // Start flushing at 80% capacity to prevent hitting the limit
                        var flushThreshold = (int)(MaxQueueSize * 0.8); // 40,000 items
                        
                        if (queue.Count >= flushThreshold)
                        {
                            if (queue.Count >= MaxQueueSize)
                            {
                                _log.ShowInMonitor($"{_syncBackPrefix}Queue near capacity for {collectionKey} ({queue.Count} items). Pausing change stream to allow processing...");
                            }
                            
                            // Trigger flush without blocking - let it run in background
                            _ = Task.Run(() => ProcessQueuedChangesAsync(collectionKey));
                            
                            // If at max limit, pause change stream to let queue drain
                            if (queue.Count >= MaxQueueSize)
                            {
                                // Wait for queue to drain below 50% before resuming
                                var drainTarget = (int)(MaxQueueSize * 0.5); // 25,000 items
                                var maxWaitTime = TimeSpan.FromSeconds(5); // Don't wait forever
                                var startWait = DateTime.UtcNow;
                                
                                while (queue.Count > drainTarget && 
                                       (DateTime.UtcNow - startWait) < maxWaitTime &&
                                       !cancellationToken.IsCancellationRequested)
                                {
                                    await Task.Delay(200, cancellationToken); // Check every 200ms
                                }
                                
                                _log.ShowInMonitor($"{_syncBackPrefix}Queue drained for {collectionKey}. Current size: {queue.Count} items. Resuming change stream...");
                            }
                        }

                        // Enqueue the change for processing
                        queue.Enqueue(change);
                        counter++;
                        
                        // Increment event counter when change is received
                        IncrementEventCounter(mu, change.OperationType);

                        // Update lag tracking
                        _lastChangeTimestamp[collectionKey] = DateTime.UtcNow;

                        // Update resume token
                        _resumeTokenCache.AddOrUpdate(
                            $"{sourceCollection!.CollectionNamespace}",
                            change.ResumeToken.ToJson(),
                            (_, _) => change.ResumeToken.ToJson());
                    }
                }
            }
            catch (OperationCanceledException)
            {
                // Trigger immediate flush before returning
                await ProcessQueuedChangesAsync(collectionKey);
            }
            catch (OutOfMemoryException ex)
            {
                _log.WriteLine($"{_syncBackPrefix}OutOfMemory error for {collectionKey}. Batch size was too large. Flushing queue and reducing batch size...", LogType.Warning);
                _log.ShowInMonitor($"{_syncBackPrefix}OOM Details: {ex}");
                
                // Trigger immediate flush before returning
                await ProcessQueuedChangesAsync(collectionKey);
                
                // Reduce adaptive batch size globally to prevent future OOM - using queue manager
                _queueManager.ReduceBatchSize(2);
                _adaptiveBatchSize = _queueManager.GetAdaptiveBatchSize();
                _log.WriteLine($"{_syncBackPrefix}Reduced adaptive batch size to {_adaptiveBatchSize} to prevent OOM recurrence.");
                
                throw; // Re-throw to trigger retry with smaller batch
            }
            catch (Exception)
            {
                // Trigger immediate flush before returning
                await ProcessQueuedChangesAsync(collectionKey);
                throw;
            }
            finally
            {
                // Trigger immediate flush before returning
                await ProcessQueuedChangesAsync(collectionKey);
            }
        }


        // This method retrieves the event associated with the ResumeToken
        private bool AutoReplayFirstChangeInResumeToken(string? documentId, ChangeStreamOperationType opType, IMongoCollection<BsonDocument> sourceCollection, IMongoCollection<BsonDocument> targetCollection, MigrationUnit mu)
        {
            if (documentId == null || string.IsNullOrEmpty(documentId))
            {
                _log.WriteLine($"Auto replay is empty for {sourceCollection.CollectionNamespace}.", LogType.Debug);
                return true; // Skip if no document ID is provided
            }
            else
            {
                _log.WriteLine($"Auto replay for {opType} operation with _id {documentId} in {sourceCollection.CollectionNamespace}.", LogType.Debug);
            }

            var bsonDoc = BsonDocument.Parse(documentId);
            var filter = MongoHelper.BuildFilterFromDocumentKey(bsonDoc);
            var result = sourceCollection.Find(filter).FirstOrDefault(); // Retrieve the document for the resume token

            try
            {
                IncrementEventCounter(mu, opType);
                switch (opType)
                {
                    case ChangeStreamOperationType.Insert:
                        if (result == null || result.IsBsonNull)
                        {
                            _log.WriteLine($"No document found for insert operation with _id {documentId} in {sourceCollection.CollectionNamespace}. Skipping insert.", LogType.Debug);
                            return true; // Skip if no document found
                        }
                        targetCollection.InsertOne(result);
                        IncrementDocCounter(mu, opType);
                        return true;
                    case ChangeStreamOperationType.Update:
                    case ChangeStreamOperationType.Replace:
                        if (result == null || result.IsBsonNull)
                        {
                            _log.WriteLine($"Processing {opType} operation for {sourceCollection.CollectionNamespace} with _id {documentId}. No document found on source, deleting it from target.", LogType.Debug);
                            var deleteTTLFilter = MongoHelper.BuildFilterFromDocumentKey(bsonDoc);
                            try
                            {
                                targetCollection.DeleteOne(deleteTTLFilter);
                                IncrementDocCounter(mu, ChangeStreamOperationType.Delete);
                            }
                            catch
                            { }
                            return true;
                        }
                        else
                        {
                            targetCollection.ReplaceOne(filter, result, new ReplaceOptions { IsUpsert = true });
                            IncrementDocCounter(mu, opType);
                            return true;
                        }
                    case ChangeStreamOperationType.Delete:
                        var deleteFilter = Builders<BsonDocument>.Filter.Eq("_id", documentId);
                        targetCollection.DeleteOne(deleteFilter);
                        IncrementDocCounter(mu, opType);
                        return true;
                    default:
                        _log.WriteLine($"Unhandled operation type: {opType}", LogType.Error);
                        return false;
                }
            }
            catch (MongoException mex) when (opType == ChangeStreamOperationType.Insert && mex.Message.Contains("DuplicateKey"))
            {
                // Ignore duplicate key errors for inserts, typically caused by reprocessing of the same change stream
                return true;
            }
            catch (Exception ex)
            {
                _log.WriteLine($"Error processing operation {opType} on {sourceCollection.CollectionNamespace} with _id {documentId}. Details: {ex}", LogType.Error);
                return false; // Return false to indicate failure in processing
            }
        }

        /// <summary>
        /// Cleanup timers and process remaining queued changes
        /// </summary>
        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                // Dispose all timers
                foreach (var timer in _collectionFlushTimers.Values)
                {
                    timer?.Dispose();
                }
                _collectionFlushTimers.Clear();

                // Process any remaining queued changes
                foreach (var collectionKey in _collectionQueues.Keys)
                {
                    ProcessQueuedChangesAsync(collectionKey).GetAwaiter().GetResult();
                }

                // Dispose semaphore
                _processingThrottle?.Dispose();
            }

            base.Dispose(disposing);
        }
    }
}