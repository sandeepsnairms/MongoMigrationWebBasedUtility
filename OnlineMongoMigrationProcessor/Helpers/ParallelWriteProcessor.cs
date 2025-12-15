using MongoDB.Bson;
using MongoDB.Driver;
using OnlineMongoMigrationProcessor.Helpers.Mongo;
using OnlineMongoMigrationProcessor.Models;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using static OnlineMongoMigrationProcessor.Helpers.Mongo.MongoHelper;

namespace OnlineMongoMigrationProcessor.Helpers
{
    /// <summary>
    /// Handles parallel write operations with sequence preservation, retry logic, and resource-based thread calculation
    /// </summary>
    internal class ParallelWriteProcessor
    {
        private readonly Log _log;
        private readonly string _logPrefix;
        private readonly int _maxThreads;
        private const int MAX_RETRIES = 10;
        private const int INITIAL_RETRY_DELAY_MS = 500;
        private const int MAX_RETRY_DELAY_MS = 300000; // 5 minutes max delay

        public ParallelWriteProcessor(Log log, string logPrefix = "")
        {
            _log = log;
            _logPrefix = logPrefix;
            _maxThreads = CalculateOptimalThreadCount();
            
            _log.WriteLine($"{_logPrefix}ParallelWriteProcessor initialized with {_maxThreads} threads based on system resources", LogType.Debug);
        }

        /// <summary>
        /// Calculate optimal number of threads based on available CPU cores and RAM
        /// </summary>
        private int CalculateOptimalThreadCount()
        {
            try
            {
                // Get CPU core count
                int processorCount = Environment.ProcessorCount;
                
                // Estimate available RAM - simplified approach without PerformanceCounter
                // Use GC memory info as a proxy for available memory
                GCMemoryInfo gcInfo = GC.GetGCMemoryInfo();
                long totalMemoryBytes = gcInfo.TotalAvailableMemoryBytes;
                float availableRamGB = totalMemoryBytes / (1024f * 1024f * 1024f);
                
                // Formula: 
                // - Database writes are I/O-bound, not CPU-bound
                // - Use 2x CPU cores to better utilize I/O wait time
                // - Limit by RAM: assume each thread needs ~200MB for buffers and processing
                int cpuBasedThreads = processorCount * 2; // 2x multiplier for I/O-bound operations
                int ramBasedLimit = (int)(availableRamGB * 1024 / 200); // Each thread ~200MB
                
                // Use the minimum of CPU-based (2x) and RAM-based limits
                // But ensure at least 2 threads and cap at 32 for safety
                int optimalThreads = Math.Min(cpuBasedThreads, ramBasedLimit);
                optimalThreads = Math.Max(2, Math.Min(32, optimalThreads));
                
                _log.WriteLine($"{_logPrefix}Thread calculation: CPU cores={processorCount}, CPU-based threads (2x)={cpuBasedThreads}, Available RAM={availableRamGB:F2}GB, " +
                              $"RAM-based limit={ramBasedLimit}, Optimal threads={optimalThreads}", LogType.Debug);
                
                return optimalThreads;
            }
            catch (Exception ex)
            {
                _log.WriteLine($"{_logPrefix}Error calculating optimal threads, defaulting to 4. Error: {ex.Message}", LogType.Warning);
                return 4; // Safe default
            }
        }

        /// <summary>
        /// Process write operations in parallel while maintaining sequence order
        /// </summary>
        public async Task<WriteResult> ProcessWritesAsync<TMigration>(
            TMigration mu,
            IMongoCollection<BsonDocument> collection,
            List<ChangeStreamDocument<BsonDocument>> insertEvents,
            List<ChangeStreamDocument<BsonDocument>> updateEvents,
            List<ChangeStreamDocument<BsonDocument>> deleteEvents,
            CounterDelegate<TMigration> counterDelegate,
            int batchSize = 50,
            bool isAggressive = false,
            bool isAggressiveComplete = true,
            string jobId = "",
            MongoClient? targetClient = null,
            bool isSimulatedRun = false)
        {
            var result = new WriteResult();
            
            // Create a combined, ordered sequence of all operations
            var orderedOperations = CreateOrderedOperationSequence(insertEvents, updateEvents, deleteEvents);
            
            _log.WriteLine($"{_logPrefix}[DEBUG] ParallelWriteProcessor.ProcessWritesAsync started for {collection.CollectionNamespace}: {orderedOperations.Count} total operations, batchSize={batchSize}, maxThreads={_maxThreads}", LogType.Debug);
            
            if (!orderedOperations.Any())
            {
                _log.WriteLine($"{_logPrefix}[DEBUG] No operations to process for {collection.CollectionNamespace}, returning empty result", LogType.Debug);
                return result; // No operations to process
            }

            _log.WriteLine($"{_logPrefix}[DEBUG] Processing {orderedOperations.Count} operations across {_maxThreads} threads for {collection.CollectionNamespace.FullName}", LogType.Debug);

            // Partition operations into batches for parallel processing
            var operationBatches = PartitionOperations(orderedOperations, _maxThreads);
            _log.WriteLine($"{_logPrefix}[DEBUG] Partitioned operations into {operationBatches.Count} batches for {collection.CollectionNamespace}", LogType.Debug);
            
            // Process batches in parallel with sequence preservation
            var tasks = new List<Task<WriteResult>>();
            
            for (int i = 0; i < operationBatches.Count; i++)
            {
                var batch = operationBatches[i];
                var batchIndex = i; // Capture for closure
                _log.WriteLine($"{_logPrefix}[DEBUG] Creating task for batch {batchIndex} with {batch.Count} operations for {collection.CollectionNamespace}", LogType.Debug);
                
                var task = Task.Run(async () =>
                {
                    try
                    {
                        _log.WriteLine($"{_logPrefix}[DEBUG] Starting batch {batchIndex} processing for {collection.CollectionNamespace}", LogType.Debug);
                        var batchResult = await ProcessBatchWithRetryAsync(
                            mu,
                            collection,
                            batch,
                            counterDelegate,
                            batchSize,
                            isAggressive,
                            isAggressiveComplete,
                            jobId,
                            targetClient,
                            isSimulatedRun);
                        _log.WriteLine($"{_logPrefix}[DEBUG] Completed batch {batchIndex} processing for {collection.CollectionNamespace}: success={batchResult.Success}, processed={batchResult.Processed}, failures={batchResult.Failures}", LogType.Debug);
                        return batchResult;
                    }
                    catch (InvalidOperationException ex) when (ex.Message.Contains("CRITICAL"))
                    {
                        // CRITICAL errors must propagate to stop the entire job
                        _log.WriteLine($"{_logPrefix}CRITICAL error in batch {batchIndex} for {collection.CollectionNamespace}: {ex.Message}", LogType.Error);
                        throw; // Re-throw to stop the job
                    }
                    catch (Exception ex)
                    {
                        _log.WriteLine($"{_logPrefix}Exception in batch {batchIndex} processing for {collection.CollectionNamespace}: {ex}", LogType.Error);
                        
                        // Return a failed result instead of throwing to allow other batches to continue
                        return new WriteResult
                        {
                            Success = false,
                            Processed = 0,
                            Failures = batch.Count,
                            Skipped = 0,
                            Errors = new List<string> { $"Batch {batchIndex} failed: {ex.Message}" }
                        };
                    }
                });
                
                tasks.Add(task);
            }

            // Wait for all batches to complete
            try
            {
                _log.WriteLine($"{_logPrefix}[DEBUG] Waiting for all {tasks.Count} batch tasks to complete for {collection.CollectionNamespace}", LogType.Debug);
                var batchResults = await Task.WhenAll(tasks);
                _log.WriteLine($"{_logPrefix}[DEBUG] All batch tasks completed for {collection.CollectionNamespace}, aggregating results", LogType.Debug);
                
                // Aggregate results
                foreach (var batchResult in batchResults)
                {
                    result.Processed += batchResult.Processed;
                    result.Failures += batchResult.Failures;
                    result.Skipped += batchResult.Skipped;
                    result.WriteLatencyMS += batchResult.WriteLatencyMS;
                    
                    if (!batchResult.Success)
                    {
                        result.Success = false;
                        result.Errors.AddRange(batchResult.Errors);
                    }
                }
                
                _log.WriteLine($"{_logPrefix}[DEBUG] Result aggregation completed for {collection.CollectionNamespace}: totalProcessed={result.Processed}, totalFailures={result.Failures}, totalSkipped={result.Skipped}, success={result.Success}", LogType.Debug);
                
                if (result.Failures > 0)
                {
                    _log.WriteLine($"{_logPrefix}Parallel processing completed with {result.Failures} failures for {collection.CollectionNamespace.FullName}", LogType.Warning);
                }
                else
                {
                    _log.WriteLine($"{_logPrefix}Parallel processing completed successfully: {result.Processed} processed, {result.Skipped} skipped for {collection.CollectionNamespace.FullName}", LogType.Debug);
                }
            }
            catch (Exception ex)
            {
                result.Success = false;
                result.Errors.Add($"Critical error in parallel processing: {ex.Message}");
                _log.WriteLine($"{_logPrefix}Critical error in parallel write processing for {collection.CollectionNamespace.FullName}: {ex}", LogType.Error);
                throw; // Re-throw to stop the job
            }
            
            _log.WriteLine($"{_logPrefix}[DEBUG] ParallelWriteProcessor.ProcessWritesAsync completed for {collection.CollectionNamespace}", LogType.Debug);
            return result;
        }

        /// <summary>
        /// Create an ordered sequence of operations based on timestamps to maintain write order
        /// </summary>
        private List<OrderedOperation> CreateOrderedOperationSequence(
            List<ChangeStreamDocument<BsonDocument>> insertEvents,
            List<ChangeStreamDocument<BsonDocument>> updateEvents,
            List<ChangeStreamDocument<BsonDocument>> deleteEvents)
        {
            var operations = new List<OrderedOperation>();
            long sequenceNumber = 0;

            // Add all operations with their sequence numbers based on ClusterTime
            foreach (var evt in insertEvents.Where(e => e != null).OrderBy(e => e.ClusterTime ?? new BsonTimestamp(0, 0)))
            {
                operations.Add(new OrderedOperation
                {
                    SequenceNumber = sequenceNumber++,
                    OperationType = ChangeStreamOperationType.Insert,
                    Event = evt,
                    Timestamp = evt.ClusterTime ?? new BsonTimestamp(0, 0)
                });
            }

            foreach (var evt in updateEvents.Where(e => e != null).OrderBy(e => e.ClusterTime ?? new BsonTimestamp(0, 0)))
            {
                operations.Add(new OrderedOperation
                {
                    SequenceNumber = sequenceNumber++,
                    OperationType = ChangeStreamOperationType.Update,
                    Event = evt,
                    Timestamp = evt.ClusterTime ?? new BsonTimestamp(0, 0)
                });
            }

            foreach (var evt in deleteEvents.Where(e => e != null).OrderBy(e => e.ClusterTime ?? new BsonTimestamp(0, 0)))
            {
                operations.Add(new OrderedOperation
                {
                    SequenceNumber = sequenceNumber++,
                    OperationType = ChangeStreamOperationType.Delete,
                    Event = evt,
                    Timestamp = evt.ClusterTime ?? new BsonTimestamp(0, 0)
                });
            }

            // Sort by timestamp to maintain chronological order
            return operations.OrderBy(op => op.Timestamp).ThenBy(op => op.SequenceNumber).ToList();
        }

        /// <summary>
        /// Partition operations into batches for parallel processing while maintaining sequence
        /// </summary>
        private List<List<OrderedOperation>> PartitionOperations(List<OrderedOperation> operations, int partitionCount)
        {
            var partitions = new List<List<OrderedOperation>>();
            
            // Group operations by document key to ensure operations on the same document stay together
            var groupedByDocument = operations
                .GroupBy(op => op.Event.DocumentKey.ToJson())
                .ToList();
            
            // Distribute document groups across partitions using round-robin
            for (int i = 0; i < partitionCount; i++)
            {
                partitions.Add(new List<OrderedOperation>());
            }
            
            int partitionIndex = 0;
            foreach (var documentGroup in groupedByDocument)
            {
                // Add all operations for this document to the same partition
                partitions[partitionIndex].AddRange(documentGroup.OrderBy(op => op.SequenceNumber));
                partitionIndex = (partitionIndex + 1) % partitionCount;
            }
            
            // Remove empty partitions
            return partitions.Where(p => p.Any()).ToList();
        }

        /// <summary>
        /// Process a batch of operations with retry logic and exponential backoff
        /// </summary>
        private async Task<WriteResult> ProcessBatchWithRetryAsync<TMigration>(
            TMigration mu,
            IMongoCollection<BsonDocument> collection,
            List<OrderedOperation> operations,
            CounterDelegate<TMigration> counterDelegate,
            int batchSize,
            bool isAggressive,
            bool isAggressiveComplete,
            string jobId,
            MongoClient? targetClient,
            bool isSimulatedRun)
        {
            var result = new WriteResult { Success = true };
            
            if (!operations.Any())
            {
                return result;
            }

            string databaseName = collection.Database.DatabaseNamespace.DatabaseName;
            string collectionName = collection.CollectionNamespace.CollectionName;
            
            // Initialize aggressive change stream helper if needed
            AggressiveChangeStreamHelper? aggressiveHelper = null;
            if (isAggressive && !isAggressiveComplete && targetClient != null)
            {
                aggressiveHelper = new AggressiveChangeStreamHelper(targetClient, _log, jobId);
            }

            // Group operations by type for bulk processing
            var insertOps = operations.Where(op => op.OperationType == ChangeStreamOperationType.Insert).Select(op => op.Event).ToList();
            var updateOps = operations.Where(op => op.OperationType == ChangeStreamOperationType.Update || op.OperationType == ChangeStreamOperationType.Replace).Select(op => op.Event).ToList();
            var deleteOps = operations.Where(op => op.OperationType == ChangeStreamOperationType.Delete).Select(op => op.Event).ToList();

            // Process each operation type with retry logic
            if (insertOps.Any())
            {
                var insertResult = await ProcessInsertsWithRetryAsync(
                    mu, collection, insertOps, counterDelegate, batchSize,
                    isAggressive, isAggressiveComplete, aggressiveHelper, databaseName, collectionName, isSimulatedRun);
                result.Processed += insertResult.Processed;
                result.Failures += insertResult.Failures;
                result.Skipped += insertResult.Skipped;
                result.WriteLatencyMS += insertResult.WriteLatencyMS;
                result.Success &= insertResult.Success;
                result.Errors.AddRange(insertResult.Errors);
            }

            if (updateOps.Any())
            {
                var updateResult = await ProcessUpdatesWithRetryAsync(
                    mu, collection, updateOps, counterDelegate, batchSize,
                    isAggressive, isAggressiveComplete, aggressiveHelper, databaseName, collectionName, isSimulatedRun);
                result.Processed += updateResult.Processed;
                result.Failures += updateResult.Failures;
                result.Skipped += updateResult.Skipped;
                result.WriteLatencyMS += updateResult.WriteLatencyMS;
                result.Success &= updateResult.Success;
                result.Errors.AddRange(updateResult.Errors);
            }

            if (deleteOps.Any())
            {
                var deleteResult = await ProcessDeletesWithRetryAsync(
                    mu, collection, deleteOps, counterDelegate, batchSize,
                    isAggressive, isAggressiveComplete, aggressiveHelper, databaseName, collectionName, isSimulatedRun);
                result.Processed += deleteResult.Processed;
                result.Failures += deleteResult.Failures;
                result.Skipped += deleteResult.Skipped;
                result.WriteLatencyMS += deleteResult.WriteLatencyMS;
                result.Success &= deleteResult.Success;
                result.Errors.AddRange(deleteResult.Errors);
            }

            return result;
        }

        /// <summary>
        /// Process inserts with retry logic
        /// </summary>
        private async Task<WriteResult> ProcessInsertsWithRetryAsync<TMigration>(
         TMigration mu,
         IMongoCollection<BsonDocument> collection,
         List<ChangeStreamDocument<BsonDocument>> events,
         CounterDelegate<TMigration> counterDelegate,
         int batchSize,
         bool isAggressive,
         bool isAggressiveComplete,
         AggressiveChangeStreamHelper? aggressiveHelper,
         string databaseName,
         string collectionName,
         bool isSimulatedRun ) // <-- New parameter
        {
            var result = new WriteResult { Success = true };

            foreach (var batch in events.Chunk(batchSize))
            {
                // Deduplicate inserts by document key
                var deduplicatedInserts = batch
                    .Where(e => e.FullDocument != null && e.FullDocument.Contains("_id"))
                    .GroupBy(e => e.DocumentKey.ToJson())
                    .Select(g => g.First())
                    .ToList();

                // Remove from aggressive temp collection if needed
                if (isAggressive && !isAggressiveComplete && aggressiveHelper != null)
                {
                    var documentKeysToRemove = deduplicatedInserts
                        .Where(insertEvent => insertEvent.DocumentKey != null)
                        .Select(insertEvent => insertEvent.DocumentKey)
                        .ToList();

                    if (documentKeysToRemove.Count > 0)
                    {
                        await aggressiveHelper.RemoveDocumentKeysAsync(databaseName, collectionName, documentKeysToRemove);
                    }
                }

                var insertDocs = deduplicatedInserts
                    .Select(e =>
                    {
                        var doc = e.FullDocument;
                        var id = doc["_id"];
                        if (id.IsObjectId)
                            doc["_id"] = id.AsObjectId;
                        return doc;
                    })
                    .ToList();

                // Retry loop with exponential backoff
                var writeStopwatch = System.Diagnostics.Stopwatch.StartNew();
                try
                {
                    for (int attempt = 0; attempt <= MAX_RETRIES; attempt++)
                    {
                        try
                        {
                            long insertCount = 0;

                            if (insertDocs.Any())
                            {
                                if (!isSimulatedRun)
                                {
                                    if (!MongoHelper.IsCosmosRUEndpoint(collection))
                                    {
                                        // Use BulkWriteAsync
                                        var insertModels = insertDocs.Select(d => new InsertOneModel<BsonDocument>(d)).ToList();
                                        var writeResult = await collection.BulkWriteAsync(insertModels, new BulkWriteOptions { IsOrdered = false });
                                        insertCount = writeResult.InsertedCount;
                                    }
                                    else
                                    {
                                        // Use InsertManyAsync
                                        await collection.InsertManyAsync(insertDocs, new InsertManyOptions { IsOrdered = false });
                                        insertCount = insertDocs.Count;
                                    }
                                }
                                else
                                {
                                    insertCount = insertDocs.Count;
                                }

                                counterDelegate(mu, CounterType.Processed, ChangeStreamOperationType.Insert, (int)insertCount);
                                result.Processed += (int)insertCount;
                            }

                            break; // Success - exit retry loop
                        }
                        catch (MongoBulkWriteException<BsonDocument> ex)
                    {
                        long insertCount = ex.Result?.InsertedCount ?? 0;
                        result.Processed += (int)insertCount;
                        counterDelegate(mu, CounterType.Processed, ChangeStreamOperationType.Insert, (int)insertCount);

                        var duplicateKeyErrors = ex.WriteErrors.Where(err => err.Code == 11000).ToList();
                        result.Skipped += duplicateKeyErrors.Count;
                        counterDelegate(mu, CounterType.Skipped, ChangeStreamOperationType.Insert, duplicateKeyErrors.Count);

                        var otherErrors = ex.WriteErrors.Where(err => err.Code != 11000).ToList();
                        if (otherErrors.Any())
                        {
                            result.Failures += otherErrors.Count;
                            _log.WriteLine($"{_logPrefix}Insert errors in {collection.CollectionNamespace.FullName}: {string.Join(", ", otherErrors.Select(e => e.Message))}", LogType.Warning);
                        }
                        break; // Exit retry loop after handling BulkWriteException
                    }
                    catch (MongoCommandException cmdEx) when (cmdEx.Message.Contains("Could not acquire lock") || cmdEx.Message.Contains("deadlock"))
                    {
                        if (attempt < MAX_RETRIES)
                        {
                            int delay = CalculateRetryDelay(attempt);
                            _log.WriteLine($"{_logPrefix}Deadlock detected for {collection.CollectionNamespace.FullName}. Retry {attempt + 1}/{MAX_RETRIES} after {delay}ms...", LogType.Warning);
                            await Task.Delay(delay);
                        }
                        else
                        {
                            string errorMsg = $"CRITICAL: Unable to process insert batch for {collection.CollectionNamespace.FullName} after {MAX_RETRIES} retry attempts due to persistent deadlock. Data loss prevention requires job termination.";
                            _log.WriteLine($"{_logPrefix}{errorMsg}", LogType.Error);
                            result.Success = false;
                            result.Errors.Add(errorMsg);
                            throw new InvalidOperationException(errorMsg);
                        }
                    }
                    catch (Exception ex) when (IsTransientException(ex))
                    {
                        if (attempt < MAX_RETRIES)
                        {
                            int delay = CalculateRetryDelay(attempt);
                            string errorType = GetTransientErrorType(ex);
                            _log.WriteLine($"{_logPrefix}{errorType} detected for {collection.CollectionNamespace.FullName}. Retry {attempt + 1}/{MAX_RETRIES} after {delay}ms... Error: {ex.Message}", LogType.Warning);
                            await Task.Delay(delay);
                        }
                        else
                        {
                            string errorMsg = $"CRITICAL: Unable to process insert batch for {collection.CollectionNamespace.FullName} after {MAX_RETRIES} retry attempts. Error: {ex.Message}";
                            _log.WriteLine($"{_logPrefix}{errorMsg}", LogType.Error);
                            result.Success = false;
                            result.Errors.Add(errorMsg);
                            throw new InvalidOperationException(errorMsg);
                        }
                    }
                    }
                }
                finally
                {
                    writeStopwatch.Stop();
                    result.WriteLatencyMS += writeStopwatch.ElapsedMilliseconds;
                }
            }

            return result;
        }


        /// <summary>
        /// Process updates with retry logic
        /// </summary>
        private async Task<WriteResult> ProcessUpdatesWithRetryAsync<TMigration>(
            TMigration mu,
            IMongoCollection<BsonDocument> collection,
            List<ChangeStreamDocument<BsonDocument>> events,
            CounterDelegate<TMigration> counterDelegate,
            int batchSize,
            bool isAggressive,
            bool isAggressiveComplete,
            AggressiveChangeStreamHelper? aggressiveHelper,
            string databaseName,
            string collectionName,
            bool isSimulatedRun)
        {
            var result = new WriteResult { Success = true };

            foreach (var batch in events.Chunk(batchSize))
            {
                // Group by document key, take latest update
                var groupedUpdates = batch
                    .Where(e => e.FullDocument != null && e.FullDocument.Contains("_id"))
                    .GroupBy(e => e.DocumentKey.ToJson())
                    .Select(g => g.OrderByDescending(e => e.ClusterTime ?? new BsonTimestamp(0, 0)).First())
                    .ToList();

                // Remove from aggressive temp collection if needed
                if (isAggressive && !isAggressiveComplete && aggressiveHelper != null)
                {
                    var documentKeysToRemove = groupedUpdates
                        .Where(updateEvent => updateEvent.DocumentKey != null)
                        .Select(updateEvent => updateEvent.DocumentKey)
                        .ToList();

                    if (documentKeysToRemove.Count > 0)
                    {
                        await aggressiveHelper.RemoveDocumentKeysAsync(databaseName, collectionName, documentKeysToRemove);
                    }
                }

                var updateModels = groupedUpdates
                    .Select(e =>
                    {
                        var filter = Builders<BsonDocument>.Filter.Eq("_id", e.DocumentKey["_id"]);
                        return new ReplaceOneModel<BsonDocument>(filter, e.FullDocument) { IsUpsert = true };
                    })
                    .ToList();

                // Retry loop with exponential backoff
                var writeStopwatch = System.Diagnostics.Stopwatch.StartNew();
                try
                {
                    for (int attempt = 0; attempt <= MAX_RETRIES; attempt++)
                    {
                        try
                        {
                            if (updateModels.Any())
                            {
                                long updateCount = 0;
                                
                                if (!isSimulatedRun)
                                {
                                    var writeResult = await collection.BulkWriteAsync(updateModels, new BulkWriteOptions { IsOrdered = false });
                                    updateCount = writeResult.ModifiedCount + writeResult.Upserts.Count;
                                }
                                else
                                {
                                    updateCount = updateModels.Count;
                                }
                                
                                counterDelegate(mu, CounterType.Processed, ChangeStreamOperationType.Update, (int)updateCount);
                                result.Processed += (int)updateCount;
                            }
                            break; // Success - exit retry loop
                        }
                        catch (MongoCommandException cmdEx) when (cmdEx.Message.Contains("Could not acquire lock") || cmdEx.Message.Contains("deadlock"))
                    {
                        if (attempt < MAX_RETRIES)
                        {
                            int delay = CalculateRetryDelay(attempt);
                            _log.WriteLine($"{_logPrefix}Deadlock detected for {collection.CollectionNamespace.FullName}. Retry {attempt + 1}/{MAX_RETRIES} after {delay}ms...", LogType.Warning);
                            await Task.Delay(delay);
                        }
                        else
                        {
                            string errorMsg = $"CRITICAL: Unable to process update batch for {collection.CollectionNamespace.FullName} after {MAX_RETRIES} retry attempts due to persistent deadlock.";
                            _log.WriteLine($"{_logPrefix}{errorMsg}", LogType.Error);
                            result.Success = false;
                            result.Errors.Add(errorMsg);
                            throw new InvalidOperationException(errorMsg);
                        }
                    }
                    catch (Exception ex) when (IsTransientException(ex))
                    {
                        if (attempt < MAX_RETRIES)
                        {
                            int delay = CalculateRetryDelay(attempt);
                            string errorType = GetTransientErrorType(ex);
                            _log.WriteLine($"{_logPrefix}{errorType} detected for {collection.CollectionNamespace.FullName}. Retry {attempt + 1}/{MAX_RETRIES} after {delay}ms... Error: {ex.Message}", LogType.Warning);
                            await Task.Delay(delay);
                        }
                        else
                        {
                            string errorMsg = $"CRITICAL: Unable to process update batch for {collection.CollectionNamespace.FullName} after {MAX_RETRIES} retry attempts. Error: {ex.Message}";
                            _log.WriteLine($"{_logPrefix}{errorMsg}", LogType.Error);
                            result.Success = false;
                            result.Errors.Add(errorMsg);
                            throw new InvalidOperationException(errorMsg);
                        }
                    }
                    }
                }
                finally
                {
                    writeStopwatch.Stop();
                    result.WriteLatencyMS += writeStopwatch.ElapsedMilliseconds;
                }
            }

            return result;
        }

        /// <summary>
        /// Process deletes with retry logic
        /// </summary>
        private async Task<WriteResult> ProcessDeletesWithRetryAsync<TMigration>(
            TMigration mu,
            IMongoCollection<BsonDocument> collection,
            List<ChangeStreamDocument<BsonDocument>> events,
            CounterDelegate<TMigration> counterDelegate,
            int batchSize,
            bool isAggressive,
            bool isAggressiveComplete,
            AggressiveChangeStreamHelper? aggressiveHelper,
            string databaseName,
            string collectionName,
            bool isSimulatedRun)
        {
            var result = new WriteResult { Success = true };
            var writeStopwatch = System.Diagnostics.Stopwatch.StartNew();

            try
            {
                // Use the refactored MongoHelper.ProcessDeletesAsync which includes aggressive mode handling
                int failures = await MongoHelper.ProcessDeletesAsync(
                    mu,
                    collection,
                    events,
                    counterDelegate,
                    _log,
                    _logPrefix,
                    batchSize,
                    isAggressive,
                    isAggressiveComplete,
                    string.Empty, // jobId - not needed as aggressiveHelper is passed
                    null, // targetClient - not needed as aggressiveHelper is passed
                    isSimulatedRun);

                if (failures > 0)
                {
                    result.Failures += failures;
                    _log.WriteLine($"{_logPrefix}ProcessDeletesAsync reported {failures} failures", LogType.Debug);
                }
            }
            catch (InvalidOperationException ex) when (ex.Message.Contains("CRITICAL"))
            {
                // Critical error - propagate up
                result.Success = false;
                result.Errors.Add(ex.Message);
                throw;
            }
            catch (Exception ex)
            {
                result.Success = false;
                result.Failures++;
                result.Errors.Add($"Delete processing error: {ex.Message}");
                _log.WriteLine($"{_logPrefix}Error processing deletes: {ex.Message}", LogType.Error);
            }
            finally
            {
                writeStopwatch.Stop();
                result.WriteLatencyMS += writeStopwatch.ElapsedMilliseconds;
            }

            return result;
        }

        /// <summary>
        /// Calculate retry delay with exponential backoff
        /// </summary>
        private int CalculateRetryDelay(int attempt)
        {
            int delay = INITIAL_RETRY_DELAY_MS * (int)Math.Pow(2, attempt);
            return Math.Min(delay, MAX_RETRY_DELAY_MS);
        }

        /// <summary>
        /// Check if exception is transient and should be retried
        /// </summary>
        private bool IsTransientException(Exception ex)
        {
            return ex is MongoConnectionException ||
                   ex is MongoExecutionTimeoutException ||
                   ex is TimeoutException ||
                   (ex is MongoCommandException cmdEx && 
                    (cmdEx.Message.Contains("operation exceeded time limit") ||
                     cmdEx.Message.Contains("network error") ||
                     cmdEx.Message.Contains("connection")));
        }

        /// <summary>
        /// Get human-readable error type for logging
        /// </summary>
        private string GetTransientErrorType(Exception ex)
        {
            if (ex is MongoConnectionException) return "Connection error";
            if (ex is MongoExecutionTimeoutException) return "Execution timeout";
            if (ex is TimeoutException) return "Timeout";
            if (ex is MongoCommandException) return "Command error";
            return "Transient error";
        }

        /// <summary>
        /// Represents an ordered operation in the write sequence
        /// </summary>
        private class OrderedOperation
        {
            public long SequenceNumber { get; set; }
            public ChangeStreamOperationType OperationType { get; set; }
            public ChangeStreamDocument<BsonDocument> Event { get; set; } = null!;
            public BsonTimestamp Timestamp { get; set; } = new BsonTimestamp(0, 0);
        }
    }

    /// <summary>
    /// Result of write processing (single batch or aggregated)
    /// </summary>
    public class WriteResult
    {
        public bool Success { get; set; } = true;
        public int Processed { get; set; }
        public int Failures { get; set; }
        public int Skipped { get; set; }
        public List<string> Errors { get; set; } = new List<string>();
        public long WriteLatencyMS { get; set; } = 0;
    }
}
