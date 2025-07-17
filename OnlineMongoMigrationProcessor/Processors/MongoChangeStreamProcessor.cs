using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Linq;
using System.Reflection;
using System.Reflection.Metadata;
using System.Threading;
using System.Threading.Tasks;
using System.Xml;
using System.Xml.Linq;

#pragma warning disable CS8602 // Dereference of a possibly null reference.

namespace OnlineMongoMigrationProcessor
{
    internal class MongoChangeStreamProcessor
    {
        private int _concurrentProcessors;
        private int _processorRunDurationInMin; // Duration to watch the change stream in minutes


        private MongoClient _sourceClient;
        private MongoClient _targetClient;
        private JobList? _jobList;
        private MigrationJob? _job;
        private MigrationSettings? _config;
        private bool _syncBack = false;
        private string _syncBackPrefix = string.Empty;
        private bool _isCSProcessing = false;


        private Dictionary<string, string> _resumeTokenCache = new Dictionary<string, string>();
        private  Dictionary<string, bool> _processingStartedList = new Dictionary<string, bool>();
        private Dictionary<string, MigrationUnit> _migrationUnitsToProcess = new Dictionary<string, MigrationUnit>();


       

        public bool ExecutionCancelled { get; set; }        

        public MongoChangeStreamProcessor(MongoClient sourceClient, MongoClient targetClient, JobList jobs,MigrationJob job, MigrationSettings config, bool syncBack = false)
        {
            _sourceClient = sourceClient;
            _targetClient = targetClient;
            _jobList = jobs;
            _job = job;
            _config = config;
            _syncBack = syncBack;
            if (_syncBack)
                _syncBackPrefix = "Sync Back: ";

            _concurrentProcessors = _config?.ChangeStreamMaxCollsInBatch ?? 5;
            _processorRunDurationInMin = _config?.ChangeStreamBatchDuration ?? 1;
        }

        public bool AddCollectionsToProcess(MigrationUnit item, CancellationTokenSource cts)
        {
            string key = $"{item.DatabaseName}.{item.CollectionName}";
            if (item.SourceStatus != CollectionStatus.OK || item.DumpComplete != true || item.RestoreComplete != true)
            {
                Log.WriteLine($"{_syncBackPrefix}Cannot add {key} to change streams to process.", LogType.Error);
                return false;
            }            
            if (!_migrationUnitsToProcess.ContainsKey(key))
            {
                _migrationUnitsToProcess.Add(key, item);
                Log.WriteLine($"{_syncBackPrefix}Change stream for {key} added to queue.");
                Log.Save();
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
            try
            {
                if (_isCSProcessing)
                {
                    return; //already processing    
                }

                _isCSProcessing = true;

                cts = new CancellationTokenSource();
                var token = cts.Token;


                int index = 0;

                _job.CSPostProcessingStarted = true;
                _jobList?.Save(); // persist state


                _migrationUnitsToProcess.Clear();
                foreach (var migrationUnit in _job.MigrationUnits)
                {
                    if (migrationUnit.SourceStatus == CollectionStatus.OK && migrationUnit.DumpComplete==true && migrationUnit.RestoreComplete==true)
                    {
                        _migrationUnitsToProcess.Add($"{migrationUnit.DatabaseName}.{migrationUnit.CollectionName}", migrationUnit);
                    }
                }


                if (_migrationUnitsToProcess.Count == 0)
                {
                    Log.WriteLine($"{_syncBackPrefix}No change streams to process.");
                    Log.Save();
                    _isCSProcessing = false;
                    return;
                }

                var keys = _migrationUnitsToProcess.Keys.ToList();

                Log.WriteLine($"{_syncBackPrefix}Starting change stream processing for {keys.Count} collection(s). Each round-robin batch will process {Math.Min(_concurrentProcessors,keys.Count)} collections for {_processorRunDurationInMin} minute(s).");
                Log.Save();


                while (!token.IsCancellationRequested && !ExecutionCancelled)
                {
                    var tasks = new List<Task>();
                    var collectionProcessed = new List<string>();
                    for (int i = 0; i < Math.Min(_concurrentProcessors, keys.Count); i++)
                    {
                        int currentCount=keys.Count;
                        var key = keys[index];
                        var unit = _migrationUnitsToProcess[key];

                        collectionProcessed.Add(key);
                        // Run synchronous method in background
                        tasks.Add(Task.Run(() => ProcessCollectionChangeStream(unit,true), token));

                        keys = _migrationUnitsToProcess.Keys.ToList();//get latest keys after each iteration,new items could have been added.
                        if( currentCount!= keys.Count)
                        {
                            Log.WriteLine($"{_syncBackPrefix}Change stream processing updated to {keys.Count} collection(s). Each round-robin batch will process {Math.Min(_concurrentProcessors, keys.Count)} collections for {_processorRunDurationInMin} minute(s).");
                        }
                        index = (index + 1) % keys.Count;
                    }

                    Log.WriteLine($"{_syncBackPrefix}Processing change streams for collections: {string.Join(", ", collectionProcessed)}");
                    Log.Save();
                    await Task.WhenAll(tasks);

                    // Pause briefly before next iteration
                    Thread.Sleep(100);
                }

                Log.WriteLine($"{_syncBackPrefix}Change stream processing completed.");
                Log.Save();
                _isCSProcessing = false;
            }
            catch (OperationCanceledException)
            {
                Log.WriteLine($"{_syncBackPrefix}Change stream processing was cancelled.");
                Log.Save();
                _isCSProcessing = false;
            }
            catch (Exception ex)
            {
                Log.WriteLine($"{_syncBackPrefix}Error during change stream processing: {ex.ToString()}", LogType.Error);
                Log.Save();
                _isCSProcessing = false;
            }
        }

        private void ProcessCollectionChangeStream(MigrationUnit item, bool IsCSProcessingRun=false)
        {
            try
            {
                string databaseName = item.DatabaseName;
                string collectionName = item.CollectionName;

                IMongoDatabase sourceDb;
                IMongoDatabase targetDb;

                IMongoCollection<BsonDocument> sourceCollection;
                IMongoCollection<BsonDocument> targetCollection;

                if (!_syncBack)
                {
                    sourceDb = _sourceClient.GetDatabase(databaseName);
                    sourceCollection = sourceDb.GetCollection<BsonDocument>(collectionName);

                    targetDb = _targetClient.GetDatabase(databaseName);
                    targetCollection = targetDb.GetCollection<BsonDocument>(collectionName);
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

                    if (!_syncBack)
                    {

                        if (item.CursorUtcTimestamp > DateTime.MinValue && !_job.SourceServerVersion.StartsWith("3") && !item.ResetChangeStream && item.ResumeToken == null) //skip CursorUtcTimestamp if its reset 
                        {
                            var bsonTimestamp = MongoHelper.ConvertToBsonTimestamp(item.CursorUtcTimestamp.ToLocalTime());
                            options = new ChangeStreamOptions { BatchSize = 100, FullDocument = ChangeStreamFullDocumentOption.UpdateLookup, StartAtOperationTime = bsonTimestamp };
                        }
                        else if (item.ResumeToken != null && !item.ResetChangeStream) //skip resume token if its reset
                        {
                            options = new ChangeStreamOptions { BatchSize = 100, FullDocument = ChangeStreamFullDocumentOption.UpdateLookup, ResumeAfter = BsonDocument.Parse(item.ResumeToken) };
                           
                        }
                        else if (item.ResumeToken == null && _job.SourceServerVersion.StartsWith("3"))
                        {
                            options = new ChangeStreamOptions { BatchSize = 100, FullDocument = ChangeStreamFullDocumentOption.UpdateLookup };
                        }
                        else if (item.ChangeStreamStartedOn.HasValue && !_job.SourceServerVersion.StartsWith("3"))
                        {
                            var bsonTimestamp = MongoHelper.ConvertToBsonTimestamp((DateTime)item.ChangeStreamStartedOn);
                            options = new ChangeStreamOptions { BatchSize = 100, FullDocument = ChangeStreamFullDocumentOption.UpdateLookup, StartAtOperationTime = bsonTimestamp };
                            item.ResetChangeStream = false; //reset the start time after setting resume token
                        }
                    }
                    else
                    {
                        if (item.SyncBackResumeToken != null)
                        {
                            options = new ChangeStreamOptions { FullDocument = ChangeStreamFullDocumentOption.UpdateLookup, ResumeAfter = BsonDocument.Parse(item.SyncBackResumeToken) };
                        }
                        else if (item.SyncBackChangeStreamStartedOn.HasValue)
                        {
                            var bsonTimestamp = MongoHelper.ConvertToBsonTimestamp((DateTime)item.SyncBackChangeStreamStartedOn);
                            options = new ChangeStreamOptions { FullDocument = ChangeStreamFullDocumentOption.UpdateLookup, StartAtOperationTime = bsonTimestamp };
                        }
                    }

                    // Create a CancellationTokenSource with a timeout (e.g., 5 minutes)
                    var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromMinutes(_processorRunDurationInMin));
                    CancellationToken cancellationToken = cancellationTokenSource.Token;


                    //to do remove skipFirst logic
                    bool skipFirst = false;
                    _processingStartedList.TryGetValue($"{databaseName}.{collectionName}", out skipFirst);

                    Log.AddVerboseMessage($"{_syncBackPrefix}Monitoring change stream with new batch for {targetCollection.CollectionNamespace}");
                    Log.Save();

                    WatchCollection(item, options, sourceCollection, targetCollection, cancellationToken, skipFirst);

                                           
                }                    
                catch (MongoCommandException ex) when (ex.ToString().Contains("Resume of change stream was not possible"))
                {
                    // Handle other potential exceptions
                    Log.WriteLine($"{_syncBackPrefix}Oplog is full. Error processing change stream for {targetCollection.CollectionNamespace}. Details : {ex.ToString()}", LogType.Error);
                    Log.Save();
                    Thread.Sleep(1000 * 300);
                }
                catch (MongoCommandException ex) when (ex.Message.Contains("Expired resume token") || ex.Message.Contains("cursor"))
                {
                    Log.WriteLine($"Resume token expired or cursor invalid for {targetCollection.CollectionNamespace}. Using last datetime to continue.");
                    item.ResumeToken = null; // Reset the resume token
                }
                catch (Exception ex)
                {
                    // Handle other potential exceptions
                    Log.WriteLine($"{_syncBackPrefix}Error processing change stream for {targetCollection.CollectionNamespace}. Details : {ex.ToString()}", LogType.Error);
                    Log.Save();
                }
                finally
                {
                    Log.Save();
                }

            }
            catch (Exception ex)
            {
                Log.WriteLine($"{_syncBackPrefix}Error processing change stream. Details : {ex.ToString()}", LogType.Error);
                Log.Save();
            }
        }

        private void WatchCollection(MigrationUnit item, ChangeStreamOptions options, IMongoCollection<BsonDocument> sourceCollection,IMongoCollection<BsonDocument> targetCollection, CancellationToken cancellationToken, bool skipFirst)
        {
            bool isVCore = (_syncBack ? _job.TargetEndpoint : _job.SourceEndpoint)
                .Contains("mongocluster.cosmos.azure.com", StringComparison.OrdinalIgnoreCase);

            long counter = 0;

            ChnageStreamsDocuments chnageStreamsDocuments = new ChnageStreamsDocuments();

            try
            {
                
                //_docsToBeInserted.Clear();
                //_docsToBeUpdated.Clear();
                //_docsToBeDeleted.Clear();

                //using var cursor = sourceCollection.Watch(options);
                using var cursor = sourceCollection.Watch(options, cancellationToken);

                string lastProcessedToken = string.Empty;
                if (_job.SourceServerVersion.StartsWith("3"))
                {
                    // For MongoDB 3.x, ResumeAfter skips the current item, so replay it manually if needed
                    if (item.ResumeDocumentId != null && !item.ResumeDocumentId.IsBsonNull)
                    {
                        AutoReplayFirstChangeInResumeToken(item.ResumeDocumentId, item.ResumeTokenOperation, sourceCollection, targetCollection);
                    }

                    foreach (var change in cursor.ToEnumerable(cancellationToken))
                    {
                        cancellationToken.ThrowIfCancellationRequested();
                        if (ExecutionCancelled) return;

                        lastProcessedToken = string.Empty;
                        _resumeTokenCache.TryGetValue($"{targetCollection.CollectionNamespace}", out lastProcessedToken);
                        if (lastProcessedToken == change.ResumeToken.ToJson())
                        {
                            item.CSUpdatesInLastBatch = 0;
                            return; // Skip processing if the event has already been processed
                        }

                        if (counter > 1 || !skipFirst)
                        {
                            if (!ProcessCursor(change, cursor, targetCollection, item, chnageStreamsDocuments, ref counter))
                                return;
                        }
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
                            _resumeTokenCache.TryGetValue($"{targetCollection.CollectionNamespace}", out lastProcessedToken);
                            //Console.WriteLine(counter.ToString() + " : " + lastProcessedToken + " --?  " + change.ResumeToken.ToJson());
                            if (lastProcessedToken == change.ResumeToken.ToJson())
                            {
                                item.CSUpdatesInLastBatch = 0;
                                return; // Skip processing if the event has already been processed
                            }
                           

                            //if (counter > 1 || !skipFirst || isVCore)
                            //{
                                if (!ProcessCursor(change, cursor, targetCollection, item, chnageStreamsDocuments, ref counter))
                                    return;
                            //}
                        }

                        if (counter > _config.ChangeStreamMaxDocsInBatch || ExecutionCancelled)
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
                    _jobList?.Save();
                }
                catch (Exception ex)
                {
                    Log.WriteLine($"{_syncBackPrefix}Error processing changes in batch for {targetCollection.CollectionNamespace}. Details : {ex.ToString()}", LogType.Error);
                    Log.Save();
                }
            }
        }
        

        // This method retrieves the event associated with the ResumeToken
        private void AutoReplayFirstChangeInResumeToken(BsonValue? documentId, ChangeStreamOperationType opType, IMongoCollection<BsonDocument> sourceCollection, IMongoCollection<BsonDocument> targetCollection)
        {
            if (documentId==null || documentId.IsBsonNull)
            {
                Log.WriteLine("Auto replay for {targetCollection.CollectionNamespace} resume token is null, skipping processing.");
                Log.Save();
                return;
            }

            Log.WriteLine($"Auto replay for {targetCollection.CollectionNamespace} resume token is type: {opType} ");

            var filter = Builders<BsonDocument>.Filter.Eq("_id", documentId); // Assuming _id is your resume token
            var result = sourceCollection.Find(filter).FirstOrDefault(); // Retrieve the document for the resume token

            if (result != null)
            {
                try
                {
                    switch (opType)
                    {
                        case ChangeStreamOperationType.Insert:
                            targetCollection.InsertOne(result);
                            break;
                        case ChangeStreamOperationType.Update:
                        case ChangeStreamOperationType.Replace:
                            if (result == null || result.IsBsonNull)
                            {
                                Log.WriteLine($"No document found. Deleting document with _id {documentId} for {opType}.");
                                var deleteTTLFilter = Builders<BsonDocument>.Filter.Eq("_id", documentId);
                                try
                                {
                                    targetCollection.DeleteOne(deleteTTLFilter);
                                }
                                catch
                                { }
                            }
                            else
                            {
                                targetCollection.ReplaceOne(filter, result, new ReplaceOptions { IsUpsert = true });
                            }
                            break;
                        case ChangeStreamOperationType.Delete:
                            var deleteFilter = Builders<BsonDocument>.Filter.Eq("_id", documentId);
                            targetCollection.DeleteOne(deleteFilter);
                            break;
                        default:
                            Log.WriteLine($"Unhandled operation type: {opType}");
                            break;
                    }
                }
                catch (MongoException mex) when (opType == ChangeStreamOperationType.Insert && mex.Message.Contains("DuplicateKey"))
                {
                    // Ignore duplicate key errors for inserts, typically caused by reprocessing of the same change stream
                }
                catch (Exception ex)
                {
                    Log.WriteLine($"Error processing operation {opType} on {targetCollection.CollectionNamespace} with _id {documentId}. Details : {ex.ToString()}", LogType.Error);
                    Log.Save();
                }
            }
            else
            {
                Log.WriteLine("Document for the ResumeToken doesn't exist (may be deleted).");
                Log.Save();
            }
            Log.Save();
        }


        private bool ProcessCursor(ChangeStreamDocument<BsonDocument> change, IChangeStreamCursor<ChangeStreamDocument<BsonDocument>> cursor, IMongoCollection<BsonDocument> targetCollection, MigrationUnit item, ChnageStreamsDocuments chnageStreamsDocuments, ref long counter)
        {

            try
            {
                counter++;
                if (!_job.SourceServerVersion.StartsWith("3") && change.ClusterTime!=null)
                {
                    // Access the ClusterTime (timestamp) from the ChangeStreamDocument
                    var timestamp = change.ClusterTime; // Convert BsonTimestamp to DateTime

                    // Output change details to the console
                    Log.AddVerboseMessage($"{_syncBackPrefix}{change.OperationType} operation detected in {targetCollection.CollectionNamespace} for _id: {change.DocumentKey["_id"]} having TS (UTC): {MongoHelper.BsonTimestampToUtcDateTime(timestamp)}.  Sequence in Batch # {counter}");
                    ProcessChange(change, targetCollection, chnageStreamsDocuments,_job.IsSimulatedRun);


                    if (!_syncBack)
                        item.CursorUtcTimestamp = MongoHelper.BsonTimestampToUtcDateTime(timestamp);
                    else
                        item.SyncBackCursorUtcTimestamp = MongoHelper.BsonTimestampToUtcDateTime(timestamp); //for reverse sync
                }
                else if (!_job.SourceServerVersion.StartsWith("3") && change.WallTime != null) //for vcore
                {
                    // use Walltime
                    var timestamp = change.WallTime;

                    // Output change details to the monito
                    Log.AddVerboseMessage($"{_syncBackPrefix}{change.OperationType} operation detected in {targetCollection.CollectionNamespace} for _id: {change.DocumentKey["_id"]} having TS (UTC): {timestamp.Value}. Update # {counter}");
                    ProcessChange(change, targetCollection, chnageStreamsDocuments,_job.IsSimulatedRun);
                    if (!_syncBack)
                        item.CursorUtcTimestamp = timestamp.Value;
                    else
                        item.SyncBackCursorUtcTimestamp = timestamp.Value;
                }
                else
                {
                    // Output change details to the monito
                    ProcessChange(change, targetCollection, chnageStreamsDocuments, _job.IsSimulatedRun);
                }

                //only add if not added before
                if (!_processingStartedList.ContainsKey($"{targetCollection.CollectionNamespace}"))
                    _processingStartedList.Add($"{targetCollection.CollectionNamespace}", true);
                else
                    _processingStartedList[$"{targetCollection.CollectionNamespace}"] = true; // Update to true after first run

                //item.ResumeToken = cursor.Current.FirstOrDefault().ResumeToken.ToJson();
                item.ResumeToken = change.ResumeToken.ToJson();

                //only add if not added before
                if (!_resumeTokenCache.ContainsKey($"{targetCollection.CollectionNamespace}"))
                    _resumeTokenCache.Add($"{targetCollection.CollectionNamespace}", item.ResumeToken);
                else
                    _resumeTokenCache[$"{targetCollection.CollectionNamespace}"] = item.ResumeToken; 
                

                // Break if batch size is reached
                if (counter > _config.ChangeStreamMaxDocsInBatch)
                    return false;

                // Break if execution is canceled
                if (ExecutionCancelled)
                    return false;

                return true;
            }
            catch (Exception ex)
            {
                Log.WriteLine($"{_syncBackPrefix}Error processing cursor. Details : {ex.ToString()}", LogType.Error);
                Log.Save();
                return false;
            }
        }


        private void ProcessChange(ChangeStreamDocument<BsonDocument> change, IMongoCollection<BsonDocument> targetCollection, ChnageStreamsDocuments chnageStreamsDocuments, bool isWriteSimulated)
        {
            if (isWriteSimulated)
                return;


            BsonValue idValue= BsonNull.Value;

            try
            {
                if (!change.DocumentKey.TryGetValue("_id", out idValue))
                {
                    Log.WriteLine($"{_syncBackPrefix}Error processing operation {change.OperationType} on {targetCollection.CollectionNamespace}. Change stream event missing _id in DocumentKey.", LogType.Error);
                    Log.Save();
                    return;
                }

                switch (change.OperationType)
                {
                    case ChangeStreamOperationType.Insert:
                        //targetCollection.InsertOne(change.FullDocument);
                        chnageStreamsDocuments.DocsToBeInserted.Add(change);
                        break;
                    case ChangeStreamOperationType.Update:
                    case ChangeStreamOperationType.Replace:
                        var filter = Builders<BsonDocument>.Filter.Eq("_id", idValue);
                        if (change.FullDocument == null || change.FullDocument.IsBsonNull)
                        {
                            Log.WriteLine($"{_syncBackPrefix}No document found on source. Deleting document with _id {idValue} for {change.OperationType}.");
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
                            //targetCollection.ReplaceOne(filter, change.FullDocument, new ReplaceOptions { IsUpsert = true });
                            chnageStreamsDocuments.DocsToBeUpdated.Add(change);
                        }
                        break;
                    case ChangeStreamOperationType.Delete:
                        //var deleteFilter = Builders<BsonDocument>.Filter.Eq("_id", idValue);
                        //targetCollection.DeleteOne(deleteFilter);
                        chnageStreamsDocuments.DocsToBeDeleted.Add(change);
                        break;
                    default:
                        Log.WriteLine($"{_syncBackPrefix}Unhandled operation type: {change.OperationType}");
                        break;
                }
            }           
            catch (Exception ex)
            {
                Log.WriteLine($"{_syncBackPrefix}Error processing operation {change.OperationType} on {targetCollection.CollectionNamespace} with _id {idValue}. Details : {ex.ToString()}", LogType.Error);
                Log.Save();
            }
        }


        private async Task BulkProcessChangesAsync(
            IMongoCollection<BsonDocument> collection,
            List<ChangeStreamDocument<BsonDocument>> insertEvents,
            List<ChangeStreamDocument<BsonDocument>> updateEvents,
            List<ChangeStreamDocument<BsonDocument>> deleteEvents,
            int batchSize = 50)
        {
            try
            {
                // Insert operations
                foreach (var batch in insertEvents.Chunk(batchSize))
                {
                    var insertModels = batch
                        .Where(e => e.FullDocument != null)
                        .Select(e => new InsertOneModel<BsonDocument>(e.FullDocument))
                        .ToList();

                    if (insertModels.Any())
                        await collection.BulkWriteAsync(insertModels, new BulkWriteOptions { IsOrdered = false });
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
                Log.WriteLine($"{_syncBackPrefix}Error processing operation  {collection.CollectionNamespace}. Details : {ex.ToString()}", LogType.Error);
                Log.Save();
            }
        }
    }
}


