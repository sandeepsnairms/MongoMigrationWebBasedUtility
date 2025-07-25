using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Linq;
using System.Reflection;
using System.Reflection.Metadata;
using System.Threading;
using System.Threading.Tasks;
using System.Xml;
using System.Xml.Linq;
using static System.Reflection.Metadata.BlobBuilder;

#pragma warning disable CS8602 // Dereference of a possibly null reference.

namespace OnlineMongoMigrationProcessor
{
    internal class MongoChangeStreamProcessor
    {
        private int _concurrentProcessors;
        private int _processorRunMaxDurationInSec; // Max Duration to watch the change stream in seconds
        private int _processorRunMinDurationInSec; // Max Duration to watch the change stream in seconds

        private MongoClient _sourceClient;
        private MongoClient _targetClient;
        private JobList? _jobList;
        private MigrationJob? _job;
        private MigrationSettings? _config;
        private bool _syncBack = false;
        private string _syncBackPrefix = string.Empty;
        private bool _isCSProcessing = false;
        private Log _log;

        private ConcurrentDictionary<string, string> _resumeTokenCache = new ConcurrentDictionary<string, string>();
        private ConcurrentDictionary<string, MigrationUnit> _migrationUnitsToProcess = new ConcurrentDictionary<string, MigrationUnit>();

        private readonly object _processingLock = new object();

        public bool ExecutionCancelled { get; set; }        

        public MongoChangeStreamProcessor(Log log,MongoClient sourceClient, MongoClient targetClient, JobList jobs,MigrationJob job, MigrationSettings config, bool syncBack = false)
        {
            _log = log;
            _sourceClient = sourceClient;
            _targetClient = targetClient;
            _jobList = jobs;
            _job = job;
            _config = config;
            _syncBack = syncBack;
            if (_syncBack)
                _syncBackPrefix = "Sync Back: ";

            _concurrentProcessors = _config?.ChangeStreamMaxCollsInBatch ?? 5;
            _processorRunMaxDurationInSec = _config?.ChangeStreamBatchDuration ?? 120;
            _processorRunMinDurationInSec= _config?.ChangeStreamBatchDurationMin ?? 30;
        }

        public bool AddCollectionsToProcess(MigrationUnit item, CancellationTokenSource cts)
        {
            string key = $"{item.DatabaseName}.{item.CollectionName}";
            if (item.SourceStatus != CollectionStatus.OK || item.DumpComplete != true || item.RestoreComplete != true)
            {
                _log.WriteLine($"{_syncBackPrefix}Cannot add {key} to change streams to process.", LogType.Error);
                return false;
            }            
            if (!_migrationUnitsToProcess.ContainsKey(key))
            {
                _migrationUnitsToProcess.TryAdd(key, item);
                _log.WriteLine($"{_syncBackPrefix}Change stream for {key} added to queue.");
                
                var result=RunCSPostProcessingAsync(cts);
                return true;
            }
            else
            {
                return false;
            }
        }

        public async Task RunCSPostProcessingAsync(CancellationTokenSource cts)
        {
            lock (_processingLock)
            {
                if (_isCSProcessing)
                {
                    return; //already processing    
                }
                _isCSProcessing = true;
            }

            bool isVCore = (_syncBack ? _job.TargetEndpoint : _job.SourceEndpoint)
               .Contains("mongocluster.cosmos.azure.com", StringComparison.OrdinalIgnoreCase);

            try
            {               
                cts = new CancellationTokenSource();
                var token = cts.Token;

                int index = 0;

                _migrationUnitsToProcess.Clear();
                foreach (var migrationUnit in _job.MigrationUnits)
                {
                    if (migrationUnit.SourceStatus == CollectionStatus.OK && migrationUnit.DumpComplete == true && migrationUnit.RestoreComplete == true)
                    {
                        _migrationUnitsToProcess[$"{migrationUnit.DatabaseName}.{migrationUnit.CollectionName}"] = migrationUnit;

                    }                  
                }

                _job.CSPostProcessingStarted = true;
                _jobList?.Save(); // persist state

                if (_migrationUnitsToProcess.Count == 0)
                {
                    _log.WriteLine($"{_syncBackPrefix}No change streams to process.");
                    
                    _isCSProcessing = false;
                    return;
                }
                                

                // Get the latest sorted keys
                var sortedKeys = _migrationUnitsToProcess
                    .OrderByDescending(kvp => kvp.Value.CSNormalizedUpdatesInLastBatch)
                    .Select(kvp => kvp.Key)
                    .ToList();

                _log.WriteLine($"{_syncBackPrefix}Starting change stream processing for {sortedKeys.Count} collection(s). Each round-robin batch will process {Math.Min(_concurrentProcessors, sortedKeys.Count)} collections. Max duration per batch {_processorRunMaxDurationInSec} seconds.");

                

                long loops=0;
                bool oplogSucess=true;

                while (!token.IsCancellationRequested && !ExecutionCancelled)
                {
                    _job.CurrentlyActive = true;
                    var totalKeys = sortedKeys.Count;

                    while (index < totalKeys && !token.IsCancellationRequested && !ExecutionCancelled)
                    {
                        var tasks = new List<Task>();
                        var collectionProcessed = new List<string>();

                        // Determine the batch
                        var batchKeys = sortedKeys.Skip(index).Take(_concurrentProcessors).ToList();
                        var batchUnits = batchKeys
                            .Select(k => _migrationUnitsToProcess.TryGetValue(k, out var unit) ? unit : null)
                            .Where(u => u != null)
                            .ToList();


                        //total of batchUnits.All(u => u.CSUpdatesInLastBatch)
                        long totalUpdatesInBatch = batchUnits.Sum(u => u.CSNormalizedUpdatesInLastBatch);

                        //total of  _migrationUnitsToProcess
                        long totalUpdatesInAll = _migrationUnitsToProcess.Sum(kvp => kvp.Value.CSNormalizedUpdatesInLastBatch);
                                                
                        float timeFactor = totalUpdatesInAll > 0 ? (float)totalUpdatesInBatch / totalUpdatesInAll : 1;

                        int seconds = GetBatchDurationInSeconds(timeFactor);
                        foreach (var key in batchKeys)
                        {
                            if (_migrationUnitsToProcess.TryGetValue(key, out var unit))
                            {
                                collectionProcessed.Add(key);
                                unit.CSLastBatchDurationSeconds = seconds; // Store the factor for each unit
                                tasks.Add(Task.Run(() => ProcessCollectionChangeStream(unit, true, seconds), token));
                            }
                        }
                                                

                        _log.WriteLine($"{_syncBackPrefix}Processing change streams for collections: {string.Join(", ", collectionProcessed)}. Batch Duration {seconds} seconds");

                        await Task.WhenAll(tasks);

                        index += _concurrentProcessors;
                        
                        // Pause briefly before next batch
                        Thread.Sleep(100);
                    }

                    index = 0;
                    // Sort the dictionary after all processing is complete
                    sortedKeys = _migrationUnitsToProcess
                        .OrderByDescending(kvp => kvp.Value.CSNormalizedUpdatesInLastBatch)
                        .Select(kvp => kvp.Key)
                        .ToList();

                    loops++;
                    // every 4 loops, check for oplog count, doesn't work on vcore
                    if (loops%4==0 && oplogSucess && !isVCore && !_syncBack)
                    {
                        foreach (var unit in _migrationUnitsToProcess)
                        {
                            if (unit.Value.CursorUtcTimestamp > DateTime.MinValue)
                            {
                                // Convert DateTime to Unix timestamp (seconds since Jan 1, 1970)
                                long secondsSinceEpoch = new DateTimeOffset(unit.Value.CursorUtcTimestamp.ToLocalTime()).ToUnixTimeSeconds();

                                Task.Run(() =>
                                {
                                    oplogSucess = MongoHelper.GetPendingOplogCountAsync(_log, _sourceClient, secondsSinceEpoch, unit.Key);
                                });
                                if (!oplogSucess)
                                    break;
                            }
                        }
                    }
                    // _log.WriteLine($"sorted keys: {string.Join(", ", sortedKeys.ToArray())}");
                }


                _log.WriteLine($"{_syncBackPrefix}Change stream processing completed.");                
                //_job.CurrentlyActive = false;//causes failure do not undo
                _jobList?.Save();

            }
            catch (OperationCanceledException)
            {
                _log.WriteLine($"{_syncBackPrefix}Change stream processing was cancelled.");

            }
            catch (Exception ex)
            {
                _log.WriteLine($"{_syncBackPrefix}Error during change stream processing: {ex.ToString()}", LogType.Error);

            }
            finally
            {
                lock (_processingLock)
                {
                    _isCSProcessing = false;
                }

            }
        }

        private void ProcessCollectionChangeStream(MigrationUnit item, bool IsCSProcessingRun=false, int seconds = 0)
        {
            try
            {                    

                string databaseName = item.DatabaseName;
                string collectionName = item.CollectionName;

                IMongoDatabase sourceDb;
                IMongoDatabase targetDb;

                IMongoCollection<BsonDocument> sourceCollection=null;
                IMongoCollection<BsonDocument> targetCollection=null;

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
                    ChangeStreamOptions options=null;

                    DateTime startedOn;
                    DateTime timeStamp;
                    string resumeToken = string.Empty;
                    string? version = string.Empty;
                    if (!_syncBack)
                    {
                        timeStamp = item.CursorUtcTimestamp;
                        resumeToken= item.ResumeToken ?? string.Empty;
                        version = _job.SourceServerVersion;
                        if (item.ChangeStreamStartedOn.HasValue)
                        {
                            startedOn = item.ChangeStreamStartedOn.Value;
                        }
                        else
                        {
                            startedOn = DateTime.MinValue; // Example default value
                        }
                    }
                    else
                    {
                        timeStamp= item.SyncBackCursorUtcTimestamp;
                        resumeToken = item.SyncBackResumeToken ?? string.Empty;
                        version = "8"; //hard code for target
                        if (item.SyncBackChangeStreamStartedOn.HasValue)
                        {
                            startedOn = item.SyncBackChangeStreamStartedOn.Value;
                        }
                        else
                        {
                            startedOn = DateTime.MinValue; // Example default value
                        }
                    }

                    if (!item.InitialDocumenReplayed && !_job.IsSimulatedRun)
                    {
                        if (AutoReplayFirstChangeInResumeToken(item.ResumeDocumentId, item.ResumeTokenOperation, sourceCollection, targetCollection))
                        {
                            // If the first change was replayed, we can proceed
                            item.InitialDocumenReplayed = true;
                            _jobList?.Save();

                        }
                        else
                        {
                            _log.WriteLine($"{_syncBackPrefix}Failed to replay the first change for {sourceCollection.CollectionNamespace}. Skipping change stream processing for this collection.", LogType.Error);
                            throw new Exception($"Failed to replay the first change for {sourceCollection.CollectionNamespace}. Skipping change stream processing for this collection.");
                        }
                    }

                    if (timeStamp > DateTime.MinValue && !item.ResetChangeStream && resumeToken == null) //skip CursorUtcTimestamp if its reset 
                    {
                        var bsonTimestamp = MongoHelper.ConvertToBsonTimestamp(timeStamp.ToLocalTime());
                        options = new ChangeStreamOptions { BatchSize = 100, FullDocument = ChangeStreamFullDocumentOption.UpdateLookup, StartAtOperationTime = bsonTimestamp };
                    }
                    else if (!string.IsNullOrEmpty(resumeToken) && !item.ResetChangeStream) //skip resume token if its reset
                    {
                        options = new ChangeStreamOptions { BatchSize = 100, FullDocument = ChangeStreamFullDocumentOption.UpdateLookup, ResumeAfter = BsonDocument.Parse(resumeToken) };

                    }
                    else if (string.IsNullOrEmpty(resumeToken) && version.StartsWith("3"))
                    {
                        options = new ChangeStreamOptions { BatchSize = 100, FullDocument = ChangeStreamFullDocumentOption.UpdateLookup };
                    }
                    else if (startedOn > DateTime.MinValue && !version.StartsWith("3"))
                    {
                        var bsonTimestamp = MongoHelper.ConvertToBsonTimestamp((DateTime)startedOn);
                        options = new ChangeStreamOptions { BatchSize = 100, FullDocument = ChangeStreamFullDocumentOption.UpdateLookup, StartAtOperationTime = bsonTimestamp };
                        item.ResetChangeStream = false; //reset the start time after setting resume token
                    }
                   

                    if(seconds==0)
                        seconds = GetBatchDurationInSeconds(.5f); //get seconds from config or use default

                    var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(seconds));
                    CancellationToken cancellationToken = cancellationTokenSource.Token;

                    _log.AddVerboseMessage($"{_syncBackPrefix}Monitoring change stream with new batch for {sourceCollection.CollectionNamespace}. Batch Duration {seconds} seconds");
                    
                    WatchCollection(item, options, sourceCollection, targetCollection, cancellationToken);

                                           
                }
                catch (OperationCanceledException)
                {
                    // A new batch will be started. do nothing
                }
                catch (MongoCommandException ex) when (ex.ToString().Contains("Resume of change stream was not possible"))
                {
                    // Handle other potential exceptions
                    _log.WriteLine($"{_syncBackPrefix}Oplog is full. Error processing change stream for {sourceCollection.CollectionNamespace}. Details : {ex.ToString()}", LogType.Error);
                    _log.AddVerboseMessage($"{_syncBackPrefix}Oplog is full. Error processing change stream for {sourceCollection.CollectionNamespace}. Details : {ex.ToString()}");

                    //ExecutionCancelled= true; // do not cancel as some collections may not be having any chnages annd  others may be processing.
                }
                catch (MongoCommandException ex) when (ex.Message.Contains("Expired resume token") || ex.Message.Contains("cursor"))
                {
                    _log.WriteLine($"{_syncBackPrefix}Resume token expired or cursor invalid for {sourceCollection.CollectionNamespace}.", LogType.Error);
                    _log.AddVerboseMessage($"{_syncBackPrefix}Resume token expired or cursor invalid for {sourceCollection.CollectionNamespace}.");

                    //ExecutionCancelled = true; // do not cancel as some collections may not be having any chnages annd  others may be processing.              
                }
                finally
                {
                    
                }

            }
            catch (Exception ex)
            {
                _log.WriteLine($"{_syncBackPrefix}Error processing change stream. Details : {ex.ToString()}", LogType.Error);
                
            }
        }

        private int GetBatchDurationInSeconds(float timeFactor=1)
        {
            // Create a CancellationTokenSource with a timeout (e.g., 120 seconds)
            int seconds = (int)(_processorRunMaxDurationInSec * timeFactor);
            if (seconds < _processorRunMinDurationInSec)
                seconds = _processorRunMinDurationInSec; // Ensure at least 15 second
            return seconds;
        }

        private void WatchCollection(MigrationUnit item, ChangeStreamOptions options, IMongoCollection<BsonDocument> sourceCollection,IMongoCollection<BsonDocument> targetCollection, CancellationToken cancellationToken)
        {
            bool isVCore = (_syncBack ? _job.TargetEndpoint : _job.SourceEndpoint)
                .Contains("mongocluster.cosmos.azure.com", StringComparison.OrdinalIgnoreCase);

            long counter = 0;

            ChnageStreamsDocuments chnageStreamsDocuments = new ChnageStreamsDocuments();

            try
            {
                using var cursor = sourceCollection.Watch(options, cancellationToken);

                string lastProcessedToken = string.Empty;
                if (_job.SourceServerVersion.StartsWith("3"))
                {

                    foreach (var change in cursor.ToEnumerable(cancellationToken))
                    {
                        cancellationToken.ThrowIfCancellationRequested();
                        if (ExecutionCancelled) return;

                        lastProcessedToken = string.Empty;
                        _resumeTokenCache.TryGetValue($"{sourceCollection.CollectionNamespace}", out lastProcessedToken);
                        if (lastProcessedToken == change.ResumeToken.ToJson())
                        {
                            item.CSUpdatesInLastBatch = 0;
                            item.CSNormalizedUpdatesInLastBatch = 0;
                            return; // Skip processing if the event has already been processed
                        }

                        if (!ProcessCursor(change, cursor, targetCollection, sourceCollection.CollectionNamespace.ToString(),item, chnageStreamsDocuments, ref counter))
                            return;
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

                            cancellationToken.ThrowIfCancellationRequested();
                            if (ExecutionCancelled) return;

                            lastProcessedToken = string.Empty;
                            _resumeTokenCache.TryGetValue($"{sourceCollection.CollectionNamespace}", out lastProcessedToken);
                            //Console.WriteLine(counter.ToString() + " : " + lastProcessedToken + " --?  " + change.ResumeToken.ToJson());
                            if (lastProcessedToken == change.ResumeToken.ToJson())
                            {
                                item.CSUpdatesInLastBatch = 0;
                                item.CSNormalizedUpdatesInLastBatch = 0;
                                return; // Skip processing if the event has already been processed
                            }

                            if (!ProcessCursor(change, cursor, targetCollection, sourceCollection.CollectionNamespace.ToString(), item, chnageStreamsDocuments, ref counter))
                                return;

                        }

                        if (ExecutionCancelled)
                            return;
                    }
                }
            }
            catch (OperationCanceledException)
            {
                // A new batch will be started. do nothing
            }
            catch (Exception ex)
            {
                throw;
            }
            finally
            {
                try
                {
                    BulkProcessChangesAsync(
                        targetCollection,
                        insertEvents: chnageStreamsDocuments.DocsToBeInserted,
                        updateEvents: chnageStreamsDocuments.DocsToBeUpdated,
                        deleteEvents: chnageStreamsDocuments.DocsToBeDeleted).GetAwaiter().GetResult();

                    item.CSUpdatesInLastBatch = counter;
                    item.CSNormalizedUpdatesInLastBatch = (long)(counter / (item.CSLastBatchDurationSeconds > 0 ? item.CSLastBatchDurationSeconds : 1));
                    _jobList?.Save();
                }
                catch (Exception ex)
                {
                    _log.WriteLine($"{_syncBackPrefix}Error processing changes in batch for {sourceCollection.CollectionNamespace}. Details : {ex.ToString()}", LogType.Error);

                }
            }
        }
        

        // This method retrieves the event associated with the ResumeToken
        private bool AutoReplayFirstChangeInResumeToken(BsonValue? documentId, ChangeStreamOperationType opType, IMongoCollection<BsonDocument> sourceCollection, IMongoCollection<BsonDocument> targetCollection)
        {


            if(documentId == null || documentId.IsBsonNull)
            {
                _log.WriteLine($"Auto replay is empty {sourceCollection.CollectionNamespace}.");
                return true; // Skip if no document ID is provided
            }
            else
            {
                _log.WriteLine($"Auto replay for {opType} operation with _id {documentId} in {sourceCollection.CollectionNamespace}. ");
            }
            
            var filter = Builders<BsonDocument>.Filter.Eq("_id", documentId); // Assuming _id is your resume token
            var result = sourceCollection.Find(filter).FirstOrDefault(); // Retrieve the document for the resume token

            try
            {
                switch (opType)
                {
                    case ChangeStreamOperationType.Insert:
                        if(result == null || result.IsBsonNull)
                        {
                            _log.WriteLine($"No document found for insert operation with _id {documentId} in {sourceCollection.CollectionNamespace}. Skipping insert.");
                            return true; // Skip if no document found
                        }
                        targetCollection.InsertOne(result);
                        return true;
                    case ChangeStreamOperationType.Update:
                    case ChangeStreamOperationType.Replace:
                        if (result == null || result.IsBsonNull)
                        {
                            _log.WriteLine($"Processing {opType} operation for {sourceCollection.CollectionNamespace} having _id {documentId}. No document found on source, deleting it from target.");
                            var deleteTTLFilter = Builders<BsonDocument>.Filter.Eq("_id", documentId);
                            try
                            {
                                targetCollection.DeleteOne(deleteTTLFilter);
                            }
                            catch
                            { }
                            return true;
                        }
                        else
                        {
                            targetCollection.ReplaceOne(filter, result, new ReplaceOptions { IsUpsert = true });
                            return true;
                        }
                    case ChangeStreamOperationType.Delete:
                        var deleteFilter = Builders<BsonDocument>.Filter.Eq("_id", documentId);
                        targetCollection.DeleteOne(deleteFilter);
                        return true;
                    default:
                        _log.WriteLine($"Unhandled operation type: {opType}");
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
                _log.WriteLine($"Error processing operation {opType} on {sourceCollection.CollectionNamespace} with _id {documentId}. Details : {ex.ToString()}", LogType.Error);
                return false; // Return false to indicate failure in processing
            }


            
        }


        private bool ProcessCursor(ChangeStreamDocument<BsonDocument> change, IChangeStreamCursor<ChangeStreamDocument<BsonDocument>> cursor, IMongoCollection<BsonDocument> targetCollection, string collNameSpace ,MigrationUnit item, ChnageStreamsDocuments chnageStreamsDocuments, ref long counter)
        {

            try
            {
                counter++;

                DateTime timeStamp=DateTime.MinValue;

                if (!_job.SourceServerVersion.StartsWith("3") && change.ClusterTime != null)
                {
                    timeStamp = MongoHelper.BsonTimestampToUtcDateTime(change.ClusterTime); // Convert BsonTimestamp to DateTime
                }
                else if (!_job.SourceServerVersion.StartsWith("3") && change.WallTime != null) //for 4.0 and above
                {
                    timeStamp = change.WallTime.Value; // Use WallTime for 4.0 and above
                }

                // Output change details to the console
                _log.AddVerboseMessage($"{_syncBackPrefix}{change.OperationType} operation detected in {collNameSpace} for _id: {change.DocumentKey["_id"]} having TS (UTC): {timeStamp}.  Sequence in Batch # {counter}");
                ProcessChange(change, targetCollection, collNameSpace, chnageStreamsDocuments, _job.IsSimulatedRun);

                if (!_syncBack)
                    item.CursorUtcTimestamp = timeStamp;
                else
                    item.SyncBackCursorUtcTimestamp = timeStamp; //for reverse sync
               

                if (change.ResumeToken != null && change.ResumeToken != BsonNull.Value)
                { 
                    if (!_syncBack)
                        item.ResumeToken = change.ResumeToken.ToJson();
                    else
                        item.SyncBackResumeToken = change.ResumeToken.ToJson();

                    _resumeTokenCache[$"{collNameSpace}"] = change.ResumeToken.ToJson();
                }          
                
                // Break if execution is canceled
                if (ExecutionCancelled)
                    return false;

                return true;
            }
            catch (Exception ex)
            {
                _log.WriteLine($"{_syncBackPrefix}Error processing cursor. Details : {ex.ToString()}", LogType.Error);
                
                return false;
            }
        }


        private void ProcessChange(ChangeStreamDocument<BsonDocument> change, IMongoCollection<BsonDocument> targetCollection,string collNameSpace, ChnageStreamsDocuments chnageStreamsDocuments, bool isWriteSimulated)
        {
            if (isWriteSimulated)
                return;


            BsonValue idValue = BsonNull.Value;

            try
            {
                if (!change.DocumentKey.TryGetValue("_id", out idValue))
                {
                    _log.WriteLine($"{_syncBackPrefix}Error processing operation {change.OperationType} on {collNameSpace}. Change stream event missing _id in DocumentKey.", LogType.Error);

                    return;
                }

                switch (change.OperationType)
                {
                    case ChangeStreamOperationType.Insert:
                        if (change.FullDocument != null &&  !change.FullDocument.IsBsonNull)
                            chnageStreamsDocuments.DocsToBeInserted.Add(change);
                        break;
                    case ChangeStreamOperationType.Update:
                    case ChangeStreamOperationType.Replace:
                        var filter = Builders<BsonDocument>.Filter.Eq("_id", idValue);
                        if (change.FullDocument == null || change.FullDocument.IsBsonNull)
                        {
                            _log.WriteLine($"{_syncBackPrefix}Processing {change.OperationType} operation for {collNameSpace} having _id {idValue}. No document found on source, deleting it from target.");
                            var deleteTTLFilter = Builders<BsonDocument>.Filter.Eq("_id", idValue);
                            try
                            {
                                targetCollection.DeleteOne(deleteTTLFilter);
                            }
                            catch
                            { }
                        }
                        else
                        {
                            chnageStreamsDocuments.DocsToBeUpdated.Add(change);
                        }
                        break;
                    case ChangeStreamOperationType.Delete:
                        chnageStreamsDocuments.DocsToBeDeleted.Add(change);
                        break;
                    default:
                        _log.WriteLine($"{_syncBackPrefix}Unhandled operation type: {change.OperationType}");
                        break;
                }

                if (chnageStreamsDocuments.DocsToBeInserted.Count+ chnageStreamsDocuments.DocsToBeUpdated.Count+ chnageStreamsDocuments.DocsToBeDeleted.Count > _config.ChangeStreamMaxDocsInBatch)
                {
                    _log.AddVerboseMessage($"{_syncBackPrefix} Change Stream MaxBatchSize exceeded. Flushing changes for {collNameSpace}");

                    // Process the changes in bulk if the batch size exceeds the limit
                    BulkProcessChangesAsync(
                        targetCollection,
                        insertEvents: chnageStreamsDocuments.DocsToBeInserted,
                        updateEvents: chnageStreamsDocuments.DocsToBeUpdated,
                        deleteEvents: chnageStreamsDocuments.DocsToBeDeleted).GetAwaiter().GetResult();
                    // Clear the lists after processing
                    chnageStreamsDocuments.DocsToBeInserted.Clear();
                    chnageStreamsDocuments.DocsToBeUpdated.Clear();
                    chnageStreamsDocuments.DocsToBeDeleted.Clear();
                }
            }
            catch (Exception ex)
            {
                _log.WriteLine($"{_syncBackPrefix}Error processing operation {change.OperationType} on {collNameSpace} with _id {idValue}. Details : {ex.ToString()}", LogType.Error);
                
            }
        }

        

        private async Task BulkProcessChangesAsync(
            IMongoCollection<BsonDocument> collection,
            List<ChangeStreamDocument<BsonDocument>> insertEvents,
            List<ChangeStreamDocument<BsonDocument>> updateEvents,
            List<ChangeStreamDocument<BsonDocument>> deleteEvents,
            int batchSize = 50)
        {

            if(_job.IsSimulatedRun)
            {
                _log.WriteLine($"{_syncBackPrefix}Skipping bulk processing in simulated run.");
                return;
            }

            try
            {
                // Insert operations
                foreach (var batch in insertEvents.Chunk(batchSize))
                {
                    var insertModels = batch
                        .Where(e => e.FullDocument != null)
                        .Select(e => new InsertOneModel<BsonDocument>(e.FullDocument))
                        .ToList();

                    try
                    {
                        if (insertModels.Any())
                            await collection.BulkWriteAsync(insertModels, new BulkWriteOptions { IsOrdered = false });
                    }
                    catch (MongoBulkWriteException<BsonDocument> ex)
                    {
                        // Filter out only duplicate key errors
                        var isAllDuplicateKeyErrors = ex.WriteErrors.All(err => err.Code == 11000);

                        if (!isAllDuplicateKeyErrors)
                        {
                            throw; // rethrow if it's not just duplicates
                        }
                    }

                }

                // Update operations
                foreach (var batch in updateEvents.Chunk(batchSize))
                {
                    var updateModels = batch
                        .Where(e => e.FullDocument != null)
                        .Select(e =>
                        {
                            var doc = e.FullDocument;
                            var filter = Builders<BsonDocument>.Filter.Eq("_id", doc["_id"]);

                            // Remove _id from update (Mongo doesn't allow _id modification)
                            var updateDoc = new BsonDocument(doc);
                            updateDoc.Remove("_id");

                            var updateDef = new BsonDocument("$set", updateDoc);

                            return new UpdateOneModel<BsonDocument>(filter, updateDef) { IsUpsert = true };
                        }).ToList();

                    if (updateModels.Any())
                        await collection.BulkWriteAsync(updateModels, new BulkWriteOptions { IsOrdered = false });
                }

                // Delete operations
                foreach (var batch in deleteEvents.Chunk(batchSize))
                {
                    var deleteModels = batch
                        .Select(e =>
                        {
                            var id = e.DocumentKey.GetValue("_id");
                            return new DeleteOneModel<BsonDocument>(Builders<BsonDocument>.Filter.Eq("_id", id));
                        }).ToList();

                    if (deleteModels.Any())
                        await collection.BulkWriteAsync(deleteModels, new BulkWriteOptions { IsOrdered = false });
                }
            }
            catch (MongoBulkWriteException<BsonDocument> ex)
            {
                // Filter out only duplicate key errors
                var isAllDuplicateKeyErrors = ex.WriteErrors.All(err => err.Code == 11000);

                if (!isAllDuplicateKeyErrors)
                {
                    throw; // rethrow if it's not just duplicates
                }
            }
            catch (Exception ex)
            {
                _log.WriteLine($"{_syncBackPrefix}Error processing operation  {collection.CollectionNamespace}. Details : {ex.ToString()}", LogType.Error);
                
            }
        }
    }
}


