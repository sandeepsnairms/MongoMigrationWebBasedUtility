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
using static OnlineMongoMigrationProcessor.MongoHelper;

#pragma warning disable CS8602 // Dereference of a possibly null reference.

namespace OnlineMongoMigrationProcessor
{
    public class ServerLevelChangeStreamProcessor : ChangeStreamProcessor
    {
        // Server-level processors use MigrationJob properties directly for global resume tokens
        protected override bool UseResumeTokenCache => false;

        public ServerLevelChangeStreamProcessor(Log log, MongoClient sourceClient, MongoClient targetClient, JobList jobList, MigrationJob job, MigrationSettings config, bool syncBack = false)
            : base(log, sourceClient, targetClient, jobList, job, config, syncBack)
        {
        }

        protected override async Task ProcessChangeStreamsAsync(CancellationToken token)
        {
            long loops = 0;
            bool oplogSuccess = true;

            bool isVCore = (_syncBack ? _job.TargetEndpoint : _job.SourceEndpoint)
               .Contains("mongocluster.cosmos.azure.com", StringComparison.OrdinalIgnoreCase);

            // RUOptimizedCopy jobs should not use server-level change streams
            if (_job.JobType == JobType.RUOptimizedCopy)
            {
                _log.WriteLine($"{_syncBackPrefix}RUOptimizedCopy jobs do not support server-level change streams. This processor should not be used for such jobs.", LogType.Error);
                return;

            }

            _log.WriteLine($"{_syncBackPrefix}Starting server-level change stream processing for {_migrationUnitsToProcess.Count} collection(s).");
            while (!token.IsCancellationRequested && !ExecutionCancelled)
            {
                try
                {
                    int seconds = GetBatchDurationInSeconds(1.0f); // Use full duration for server-level
                    var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(seconds));
                    CancellationToken cancellationToken = cancellationTokenSource.Token;

                    _log.WriteLine($"{_syncBackPrefix}Processing server-level change stream. Batch Duration {seconds} seconds");

                    await WatchServerLevelChangeStream(cancellationToken);

                    loops++;
                    // Check oplog count periodically
                    if (loops % 4 == 0 && oplogSuccess && !isVCore && !_syncBack)
                    {
                        foreach (var unit in _migrationUnitsToProcess)
                        {
                            if (_job.CursorUtcTimestamp > DateTime.MinValue)
                            {
                                // Convert DateTime to Unix timestamp (seconds since Jan 1, 1970)
                                long secondsSinceEpoch = new DateTimeOffset(_job.CursorUtcTimestamp.ToLocalTime()).ToUnixTimeSeconds();

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
                catch (OperationCanceledException)
                {
                    // Expected when batch times out, continue to next batch
                }
                catch (Exception ex)
                {
                    _log.WriteLine($"{_syncBackPrefix}Error in server-level change stream processing: {ex}", LogType.Error);
                    // Continue processing on errors
                }
            }
        }

        private async Task WatchServerLevelChangeStream(CancellationToken cancellationToken)
        {

            long counter = 0;
            var changeStreamDocuments = new Dictionary<string, ChangeStreamDocuments>();
            string collectionKey = string.Empty;

            try
            {
                // Calculate MaxAwaitTime from the cancellation token timeout
                // Use 80% of cancellation timeout or 5 seconds minimum to ensure cursor returns before cancellation
                int maxAwaitSeconds = 5;
                if (cancellationToken.CanBeCanceled)
                {
                    try
                    {
                        // Try to get timeout from CancellationTokenSource if available
                        // Fall back to 5 seconds if we can't determine it
                        var timeout = GetBatchDurationInSeconds(1.0f);
                        maxAwaitSeconds = Math.Max(5, (int)(timeout * 0.8));
                    }
                    catch
                    {
                        maxAwaitSeconds = 5;
                    }
                }

                // Create pipeline for server-level change stream
                List<BsonDocument> pipeline = new List<BsonDocument>();

                // Set up options - use global resume token from MigrationJob
                var options = new ChangeStreamOptions
                {
                    BatchSize = 500,
                    FullDocument = ChangeStreamFullDocumentOption.UpdateLookup,
                    MaxAwaitTime = TimeSpan.FromSeconds(maxAwaitSeconds)
                };

                // Get resume information from MigrationJob for server-level streams

                DateTime timeStamp = GetCursorUtcTimestamp();
                string resumeToken = GetResumeToken();
                DateTime startedOn = GetChangeStreamStartedOn();
                string? version = string.Empty;

                if (!_syncBack)
                {
                    version = _job.SourceServerVersion;
                }
                else
                {
                    version = "8"; // hard code for target
                }

                // Handle initial document replay for server-level streams
                bool initialReplayCompleted = GetInitialDocumentReplayedStatus();
                if (!initialReplayCompleted && !_job.IsSimulatedRun && !_job.AggresiveChangeStream)
                {
                    if (!AutoReplayFirstChangeInResumeToken())
                    {
                        _log.WriteLine($"{_syncBackPrefix}Failed to replay the first change for server-level change stream. Skipping server-level processing.", LogType.Error);
                        return;
                    }
                    SetInitialDocumentReplayedStatus(true);
                    _jobList?.Save();
                }


                if (timeStamp > DateTime.MinValue && resumeToken == null && !(_job.JobType == JobType.RUOptimizedCopy && !_job.ProcessingSyncBack)) //skip CursorUtcTimestamp if its reset 
                {
                    var bsonTimestamp = MongoHelper.ConvertToBsonTimestamp(timeStamp.ToLocalTime());
                    options = new ChangeStreamOptions { BatchSize = 500, FullDocument = ChangeStreamFullDocumentOption.UpdateLookup, StartAtOperationTime = bsonTimestamp, MaxAwaitTime = TimeSpan.FromSeconds(maxAwaitSeconds) };
                }
                else if (!string.IsNullOrEmpty(resumeToken))  //both version  having resume token
                {
                    options = new ChangeStreamOptions { BatchSize = 500, FullDocument = ChangeStreamFullDocumentOption.UpdateLookup, ResumeAfter = BsonDocument.Parse(resumeToken), MaxAwaitTime = TimeSpan.FromSeconds(maxAwaitSeconds) };
                }
                else if (string.IsNullOrEmpty(resumeToken) && version.StartsWith("3")) //for Mongo 3.6
                {
                    options = new ChangeStreamOptions { BatchSize = 500, FullDocument = ChangeStreamFullDocumentOption.UpdateLookup, MaxAwaitTime = TimeSpan.FromSeconds(maxAwaitSeconds) };
                }
                else if (startedOn > DateTime.MinValue && !version.StartsWith("3"))  //newer version
                {
                    var bsonTimestamp = MongoHelper.ConvertToBsonTimestamp((DateTime)startedOn);
                    options = new ChangeStreamOptions { BatchSize = 500, FullDocument = ChangeStreamFullDocumentOption.UpdateLookup, StartAtOperationTime = bsonTimestamp, MaxAwaitTime = TimeSpan.FromSeconds(maxAwaitSeconds) };
                }

                var pipelineArray = pipeline.ToArray();

                // Watch at client level (server-level)
                using var cursor = _sourceClient.Watch<ChangeStreamDocument<BsonDocument>>(pipelineArray, options, cancellationToken);


                // Initialize change stream documents for each collection
                foreach (var kvp in _migrationUnitsToProcess)
                {
                    kvp.Value.CSUpdatesInLastBatch = 0;
                    changeStreamDocuments[kvp.Key] = new ChangeStreamDocuments();
                }

                if (_job.SourceServerVersion.StartsWith("3"))
                {
                    foreach (var change in cursor.ToEnumerable(cancellationToken))
                    {
                        cancellationToken.ThrowIfCancellationRequested();
                        if (ExecutionCancelled) return;

                        var result = await ProcessChange(change, changeStreamDocuments, counter);
                        if (!result.success)
                            return;
                        counter = result.counter;
                    }
                }
                else
                {
                    while (cursor.MoveNext(cancellationToken))
                    {
                        cancellationToken.ThrowIfCancellationRequested();
                        if (ExecutionCancelled) return;

                        foreach (var change in cursor.Current)
                        {

                            collectionKey = change.CollectionNamespace.ToString();

                            cancellationToken.ThrowIfCancellationRequested();
                            if (ExecutionCancelled) return;

                            var result = await ProcessChange(change, changeStreamDocuments, counter);
                            if (!result.success)
                                return;
                            counter = result.counter;
                        }

                        if (ExecutionCancelled)
                            return;
                    }
                }
            }
            catch (OperationCanceledException)
            {
                // Expected when batch timeout occurs
            }
            catch (Exception ex)
            {
                _log.WriteLine($"{_syncBackPrefix}Error watching server-level change stream: {ex}", LogType.Error);
                throw;
            }
            finally
            {
                try
                {
                    if (changeStreamDocuments != null)
                        await BulkProcessAllChangesAsync(changeStreamDocuments);

                }
                catch (Exception ex)
                {
                    _log.WriteLine($"{_syncBackPrefix}Error processing changes in batch for {collectionKey}. Details: {ex}", LogType.Error);
                }
            }
        }

        private async Task<(bool success, long counter)> ProcessChange(ChangeStreamDocument<BsonDocument> change, Dictionary<string, ChangeStreamDocuments> changeStreamDocuments, long counter)
        {
            try
            {
                // Extract namespace information
                if (change.CollectionNamespace == null)
                {
                    return (true, counter); // Skip if no namespace info
                }

                var databaseName = change.CollectionNamespace.DatabaseNamespace.DatabaseName;
                var collectionName = change.CollectionNamespace.CollectionName;
                var collectionKey = $"{databaseName}.{collectionName}";

                // Check if this change belongs to one of our collections with SourceStatus.OK
                if (!_migrationUnitsToProcess.TryGetValue(collectionKey, out var migrationUnit))
                {
                    return (true, counter); // Skip changes for collections not in our job
                }

                if (!Helper.IsMigrationUnitValid(migrationUnit))
                {
                    return (true, counter); // Skip changes for collections with errors
                }

                // Check user filter condition               
                var userFilterDoc=MongoHelper.GetFilterDoc(migrationUnit.UserFilter);

                if (change.OperationType != ChangeStreamOperationType.Delete)
                {
                    if (userFilterDoc.Elements.Count() > 0
                        && !MongoHelper.CheckForUserFilterMatch(change.FullDocument, userFilterDoc))
                        return (true, counter); // Skip if doesn't match user filter
                }

                counter++;

                DateTime timeStamp = DateTime.MinValue;
                if (!_job.SourceServerVersion.StartsWith("3") && change.ClusterTime != null)
                {
                    timeStamp = MongoHelper.BsonTimestampToUtcDateTime(change.ClusterTime);
                }
                else if (!_job.SourceServerVersion.StartsWith("3") && change.WallTime != null)
                {
                    timeStamp = change.WallTime.Value;
                }

                // Log change details
                if (timeStamp == DateTime.MinValue)
                    _log.ShowInMonitor($"{_syncBackPrefix}{change.OperationType} operation detected in {collectionKey} for _id: {change.DocumentKey["_id"]}. Sequence in batch #{counter}");
                else
                    _log.ShowInMonitor($"{_syncBackPrefix}{change.OperationType} operation detected in {collectionKey} for _id: {change.DocumentKey["_id"]} with TS (UTC): {timeStamp}. Sequence in batch #{counter}");

                // Get target collection
                IMongoCollection<BsonDocument> targetCollection = GetTargetCollection(databaseName, collectionName);

                // Process the change
                ProcessChangeEvent(change, targetCollection, collectionKey, changeStreamDocuments[collectionKey], _job.IsSimulatedRun, migrationUnit);

                migrationUnit.CSUpdatesInLastBatch++;
                // Update timestamps
                if (!_syncBack)
                {
                    migrationUnit.CursorUtcTimestamp = timeStamp;
                    _job.CursorUtcTimestamp = timeStamp;
                }
                else
                {
                    migrationUnit.SyncBackCursorUtcTimestamp = timeStamp;
                    _job.SyncBackCursorUtcTimestamp = timeStamp;
                }

                // NOTE: Resume tokens are NOT persisted here anymore
                // They will only be persisted after successful batch write completion
                // This ensures we can recover from the last successful checkpoint
                // Store the token in ChangeStreamDocuments metadata for later persistence
                if (change.ResumeToken != null && change.ResumeToken != BsonNull.Value)
                {
                    // Token will be retrieved from changeStreamDocuments.LatestResumeToken after successful write
                }

                // Check if we need to flush changes for any collection
                foreach (var kvp in changeStreamDocuments)
                {
                    var docs = kvp.Value;
                    int totalChanges = docs.DocsToBeInserted.Count + docs.DocsToBeUpdated.Count + docs.DocsToBeDeleted.Count;
                    
                    if (totalChanges > _config.ChangeStreamMaxDocsInBatch)
                    {
                        var collKey = kvp.Key;
                        if (_migrationUnitsToProcess.TryGetValue(collKey, out var mu))
                        {
                            // BACKPRESSURE: Check global pending writes before mid-batch flush
                            int pending = GetGlobalPendingWriteCount();
                            while (pending >= MAX_GLOBAL_PENDING_WRITES)
                            {
                                await WaitWithExponentialBackoffAsync(pending, collKey);
                                
                                // Check if memory recovered after wait
                                if (IsMemoryExhausted(out long currentMB, out long maxMB, out double percent))
                                {
                                    _log.ShowInMonitor($"{_syncBackPrefix}Memory pressure detected: {currentMB}MB / {maxMB}MB ({percent:F1}%)");
                                    await WaitForMemoryRecoveryAsync(collKey);
                                }
                                
                                pending = GetGlobalPendingWriteCount();
                            }
                            
                            var targetColl = GetTargetCollection(mu.DatabaseName, mu.CollectionName);

                            _log.ShowInMonitor($"{_syncBackPrefix}Change stream max batch size exceeded. Flushing {totalChanges} changes for {collKey}");

                            IncrementGlobalPendingWrites();
                            try
                            {
                                await BulkProcessChangesAsync(
                                    mu,
                                    targetColl,
                                    insertEvents: docs.DocsToBeInserted,
                                    updateEvents: docs.DocsToBeUpdated,
                                    deleteEvents: docs.DocsToBeDeleted);

                                // CRITICAL: Only update resume token AFTER successful mid-batch flush
                                // This ensures we can recover from the last successful checkpoint on failure
                                if (!string.IsNullOrEmpty(docs.LatestResumeToken))
                                {
                                    UpdateResumeToken(docs.LatestResumeToken, docs.LatestOperationType, docs.LatestDocumentKey, collKey);
                                    _log.WriteLine($"{_syncBackPrefix}Mid-batch checkpoint updated for {collKey}: Resume token persisted after successful flush", LogType.Debug);
                                }

                                _jobList?.Save();

                                // Clear the lists and metadata after successful processing
                                docs.DocsToBeInserted.Clear();
                                docs.DocsToBeUpdated.Clear();
                                docs.DocsToBeDeleted.Clear();
                                docs.ClearMetadata(); // Clear to start fresh for next batch
                            }
                            finally
                            {
                                DecrementGlobalPendingWrites();
                            }
                        }
                    }
                }

                if (ExecutionCancelled)
                    return (false, counter);

                return (true, counter);
            }
            catch (Exception ex)
            {
                _log.WriteLine($"{_syncBackPrefix}Error processing server-level change: {ex}", LogType.Error);
                return (false, counter);
            }
        }

        private async Task BulkProcessAllChangesAsync(Dictionary<string, ChangeStreamDocuments> changeStreamDocuments)
        {
            foreach (var kvp in changeStreamDocuments)
            {
                var collectionKey = kvp.Key;
                var docs = kvp.Value;

                int totalChanges = docs.DocsToBeInserted.Count + docs.DocsToBeUpdated.Count + docs.DocsToBeDeleted.Count;
                
                if (totalChanges > 0)
                {
                    if (_migrationUnitsToProcess.TryGetValue(collectionKey, out var migrationUnit))
                    {
                        // NO BACKPRESSURE HERE: This method REDUCES memory pressure by writing accumulated changes
                        // Blocking writes here would prevent the very thing that frees up memory
                        // Backpressure is applied at read/accumulation points only
                        var targetCollection = GetTargetCollection(migrationUnit.DatabaseName, migrationUnit.CollectionName);

                        IncrementGlobalPendingWrites();
                        try
                        {
                            await BulkProcessChangesAsync(
                                migrationUnit,
                                targetCollection,
                                insertEvents: docs.DocsToBeInserted,
                                updateEvents: docs.DocsToBeUpdated,
                                deleteEvents: docs.DocsToBeDeleted);

                            // CRITICAL: Only update resume token AFTER successful batch write
                            // This ensures we can recover from the last successful checkpoint on failure
                            if (!string.IsNullOrEmpty(docs.LatestResumeToken))
                            {
                                UpdateResumeToken(docs.LatestResumeToken, docs.LatestOperationType, docs.LatestDocumentKey, collectionKey);
                                _log.WriteLine($"{_syncBackPrefix}Checkpoint updated for {collectionKey}: Resume token persisted after successful batch write", LogType.Debug);
                            }
                        }
                        finally
                        {
                            DecrementGlobalPendingWrites();
                        }
                    }
                }
            }

            _jobList?.Save();
        }

        // Server-level equivalent of AutoReplayFirstChangeInResumeToken
        private bool AutoReplayFirstChangeInResumeToken()
        {
            string documentId = GetResumeDocumentId();
            ChangeStreamOperationType operationType = GetResumeTokenOperation();
            string collectionKey = GetResumeCollectionKey();

            if (string.IsNullOrEmpty(documentId))
            {
                _log.WriteLine($"Auto replay is empty for server-level change stream.");
                return true; // Skip if no document ID is provided
            }

            if (string.IsNullOrEmpty(collectionKey))
            {
                _log.WriteLine($"Auto replay collection key is empty for server-level change stream. Cannot determine target collection.");
                return true; // Skip if no collection key is provided
            }

            _log.WriteLine($"Auto replay for {operationType} operation with document key {documentId} in collection {collectionKey} for server-level change stream.");

            var bsonDoc = BsonDocument.Parse(documentId);
            var filter = MongoHelper.BuildFilterFromDocumentKey(bsonDoc);

            // Validate that the collection key is in our migration units
            if (!_migrationUnitsToProcess.TryGetValue(collectionKey, out var migrationUnit))
            {
                _log.WriteLine($"Collection {collectionKey} for server-level auto replay is not in migration units. Skipping replay.");
                return true;
            }

            var parts = collectionKey.Split('.');
            if (parts.Length != 2)
            {
                _log.WriteLine($"Invalid collection key format for server-level auto replay: {collectionKey}. Expected format: database.collection");
                return true;
            }

            var databaseName = parts[0];
            var collectionName = parts[1];

            IMongoDatabase sourceDb;
            IMongoDatabase targetDb;
            IMongoCollection<BsonDocument> sourceCollection;
            IMongoCollection<BsonDocument> targetCollection;

            if (!_syncBack)
            {
                sourceDb = _sourceClient.GetDatabase(databaseName);
                sourceCollection = sourceDb.GetCollection<BsonDocument>(collectionName);

                if (!_job.IsSimulatedRun)
                {
                    targetDb = _targetClient.GetDatabase(databaseName);
                    targetCollection = targetDb.GetCollection<BsonDocument>(collectionName);
                }
                else
                {
                    targetCollection = sourceCollection; // Use source as placeholder for simulated runs
                }
            }
            else
            {
                // For sync back, target is source and vice versa
                targetDb = _sourceClient.GetDatabase(databaseName);
                targetCollection = targetDb.GetCollection<BsonDocument>(collectionName);

                sourceDb = _targetClient.GetDatabase(databaseName);
                sourceCollection = sourceDb.GetCollection<BsonDocument>(collectionName);
            }

            var result = sourceCollection.Find(filter).FirstOrDefault();

            try
            {
                IncrementEventCounter(migrationUnit, operationType);

                switch (operationType)
                {
                    case ChangeStreamOperationType.Insert:
                        if (result == null || result.IsBsonNull)
                        {
                            _log.WriteLine($"No document found for insert operation with document key {documentId} in {collectionKey}. Skipping insert.");
                            return true;
                        }
                        targetCollection.InsertOne(result);
                        IncrementDocCounter(migrationUnit, operationType);
                        return true;

                    case ChangeStreamOperationType.Update:
                    case ChangeStreamOperationType.Replace:
                        if (result == null || result.IsBsonNull)
                        {
                            _log.WriteLine($"Processing {operationType} operation for {collectionKey} with document key {documentId}. No document found on source, deleting it from target.");
                            try
                            {
                                targetCollection.DeleteOne(filter);
                                IncrementDocCounter(migrationUnit, ChangeStreamOperationType.Delete);
                            }
                            catch { }
                            return true;
                        }
                        else
                        {
                            targetCollection.ReplaceOne(filter, result, new ReplaceOptions { IsUpsert = true });
                            IncrementDocCounter(migrationUnit, operationType);
                            return true;
                        }

                    case ChangeStreamOperationType.Delete:
                        targetCollection.DeleteOne(filter);
                        IncrementDocCounter(migrationUnit, operationType);
                        return true;

                    default:
                        _log.WriteLine($"Unhandled operation type: {operationType}", LogType.Error);
                        return false;
                }
            }
            catch (MongoException mex) when (operationType == ChangeStreamOperationType.Insert && mex.Message.Contains("DuplicateKey"))
            {
                // Ignore duplicate key errors for inserts
                return true;
            }
            catch (Exception ex)
            {
                _log.WriteLine($"Error processing operation {operationType} in server-level auto replay with document key {documentId}. Details: {ex}", LogType.Error);
                return false;
            }
        }

        private void ProcessChangeEvent(ChangeStreamDocument<BsonDocument> change, IMongoCollection<BsonDocument> targetCollection, string collNameSpace, ChangeStreamDocuments changeStreamDocuments, bool isWriteSimulated, MigrationUnit mu)
        {
            BsonValue idValue = BsonNull.Value;

            try
            {
                if (!change.DocumentKey.TryGetValue("_id", out idValue))
                {
                    _log.WriteLine($"{_syncBackPrefix}Error processing operation {change.OperationType} on {collNameSpace}. Change stream event is missing _id in DocumentKey.", LogType.Error);
                    return;
                }

                switch (change.OperationType)
                {
                    case ChangeStreamOperationType.Insert:
                        IncrementEventCounter(mu, change.OperationType);
                        // Accumulate inserts even in simulation mode so counters get updated
                        if (change.FullDocument != null && !change.FullDocument.IsBsonNull)
                            changeStreamDocuments.AddInsert(change);
                        break;
                    case ChangeStreamOperationType.Update:
                    case ChangeStreamOperationType.Replace:
                        IncrementEventCounter(mu, change.OperationType);
                        var filter = Builders<BsonDocument>.Filter.Eq("_id", idValue);
                        if (change.FullDocument == null || change.FullDocument.IsBsonNull)
                        {
                            // Skip actual delete operation in simulation mode
                            if (!isWriteSimulated)
                            {
                                _log.WriteLine($"{_syncBackPrefix}Processing {change.OperationType} operation for {collNameSpace} with _id {idValue}. No document found on source, deleting it from target.");
                                var deleteTTLFilter = Builders<BsonDocument>.Filter.Eq("_id", idValue);
                                try
                                {
                                    targetCollection.DeleteOne(deleteTTLFilter);
                                    IncrementEventCounter(mu, ChangeStreamOperationType.Delete);
                                }
                                catch
                                { }
                            }
                        }
                        else
                        {
                            // Accumulate updates even in simulation mode so counters get updated
                            changeStreamDocuments.AddUpdate(change);
                        }
                        break;
                    case ChangeStreamOperationType.Delete:
                        IncrementEventCounter(mu, change.OperationType);
                        // Accumulate deletes even in simulation mode so counters get updated
                        changeStreamDocuments.AddDelete(change);
                        break;
                    default:
                        _log.WriteLine($"{_syncBackPrefix}Unhandled operation type: {change.OperationType}", LogType.Error);
                        break;
                }
            }
            catch (Exception ex)
            {
                _log.WriteLine($"{_syncBackPrefix}Error processing operation {change.OperationType} on {collNameSpace} with _id {idValue}. Details: {ex}", LogType.Error);
            }
        }

        #region Server-Level Resume Token Management

        private DateTime GetCursorUtcTimestamp()
        {
            if (!_syncBack)
                return _job.SyncBackCursorUtcTimestamp;
            else
                return _job.CursorUtcTimestamp;
        }

        private DateTime GetChangeStreamStartedOn()
        {
            if (!_syncBack)
            {
                if (_job.ChangeStreamStartedOn.HasValue)
                    return _job.ChangeStreamStartedOn.Value;
                else
                    return DateTime.MinValue;
            }
            else
            {
                if (_job.SyncBackChangeStreamStartedOn.HasValue)
                    return _job.SyncBackChangeStreamStartedOn.Value;
                else
                    return DateTime.MinValue;

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
            if (!_syncBack)
            {
                return _job.InitialDocumenReplayed;
            }
            else
            {
                return _job.SyncBackInitialDocumenReplayed;
            }
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
            if (!_syncBack)
            {
                return _job.ResumeTokenOperation;
            }
            else
            {
                return _job.SyncBackResumeTokenOperation;
            }
        }

        private string GetResumeDocumentId()
        {
            if (!_syncBack)
            {
                return _job.ResumeDocumentId ?? string.Empty;
            }
            else
            {
                return _job.SyncBackResumeDocumentId ?? string.Empty;
            }
        }

        private string GetResumeCollectionKey()
        {
            if (!_syncBack)
            {
                return _job.ResumeCollectionKey ?? string.Empty;
            }
            else
            {
                return _job.SyncBackResumeCollectionKey ?? string.Empty;
            }
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
    }
}