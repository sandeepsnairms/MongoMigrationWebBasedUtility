using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Linq;
using System.Reflection.Metadata;
using System.Threading;
using System.Threading.Tasks;
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

        private  Dictionary<string, bool> _processingStartedList = new Dictionary<string, bool>();
        private Dictionary<string, MigrationUnit> _chnageStreamsToProcess = new Dictionary<string, MigrationUnit>();

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
            if (!_chnageStreamsToProcess.ContainsKey(key))
            {
                _chnageStreamsToProcess.Add(key, item);
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


                _chnageStreamsToProcess.Clear();
                foreach (var migrationUnit in _job.MigrationUnits)
                {
                    if (migrationUnit.SourceStatus == CollectionStatus.OK && migrationUnit.DumpComplete==true && migrationUnit.RestoreComplete==true)
                    {
                        _chnageStreamsToProcess.Add($"{migrationUnit.DatabaseName}.{migrationUnit.CollectionName}", migrationUnit);
                    }
                }


                if (_chnageStreamsToProcess.Count == 0)
                {
                    Log.WriteLine($"{_syncBackPrefix}No change streams to process.");
                    Log.Save();
                    _isCSProcessing = false;
                    return;
                }

                var keys = _chnageStreamsToProcess.Keys.ToList();

                Log.WriteLine($"{_syncBackPrefix}Starting change stream processing for {keys.Count} collection(s). Each round-robin batch will process {Math.Min(_concurrentProcessors,keys.Count)} collections for {_processorRunDurationInMin} minute(s).");
                Log.Save();


                while (!token.IsCancellationRequested && !ExecutionCancelled)
                {
                    var tasks = new List<Task>();

                    for (int i = 0; i < Math.Min(_concurrentProcessors, keys.Count); i++)
                    {
                        int currentCount=keys.Count;
                        var key = keys[index];
                        var unit = _chnageStreamsToProcess[key];
                      
                        // Run synchronous method in background
                        tasks.Add(Task.Run(() => ProcessCollectionChangeStream(unit,true), token));

                        keys = _chnageStreamsToProcess.Keys.ToList();//get latest keys after each iteration,new items could have been added.
                        if( currentCount!= keys.Count)
                        {
                            Log.WriteLine($"{_syncBackPrefix}Change stream processing updated to {keys.Count} collection(s). Each round-robin batch will process {Math.Min(_concurrentProcessors, keys.Count)} collections for {_processorRunDurationInMin} minute(s).");
                        }
                        index = (index + 1) % keys.Count;
                    }

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

                /*
                if(_job.CSStartsAfterAllUploads==true && !_job.CSPostProcessingStarted)
                {
                    if (!Helper.IsOfflineJobCompleted(_job) || (Helper.IsOfflineJobCompleted(_job) && _job.SyncBackEnabled && !_job.ProcessingSyncBack))
                    {
                        //chnageStreamsToProcess.Add($"{databaseName}.{collectionName}", item);
                        Log.WriteLine($"{_syncBackPrefix}Change stream for {databaseName}.{collectionName} added to queue.");
                        Log.Save();
                        return;

                    }
                }
                else if (_job.CSStartsAfterAllUploads == true && _job.CSPostProcessingStarted && !IsCSProcessingRun)
                {
                    return;
                }*/


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

                        if (item.CursorUtcTimestamp > DateTime.MinValue && !_job.SourceServerVersion.StartsWith("3"))
                        {
                            var bsonTimestamp = MongoHelper.ConvertToBsonTimestamp(item.CursorUtcTimestamp.ToLocalTime());
                            options = new ChangeStreamOptions { BatchSize = 100, FullDocument = ChangeStreamFullDocumentOption.UpdateLookup, StartAtOperationTime = bsonTimestamp };
                        }
                        else if (item.ResumeToken != null)
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

            try
            {
                using var cursor = sourceCollection.Watch(options);
                int counter = 0;

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

                        counter++;

                        if (counter > 1 || !skipFirst)
                        {
                            if (!ProcessCursor(change, cursor, targetCollection, item, ref counter))
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

                            counter++;

                            if (counter > 1 || !skipFirst || isVCore)
                            {
                                if (!ProcessCursor(change, cursor, targetCollection, item, ref counter))
                                    return;
                            }
                        }

                        if (counter > _config.ChangeStreamMaxDocsInBatch || ExecutionCancelled)
                            return;
                    }
                }

                Log.Save();
            }
            catch (OperationCanceledException)
            {
                //Log.AddVerboseMessage($"{_syncBackPrefix}Change stream batch for {targetCollection.CollectionNamespace} terminated. A new batch will be started.");
            }
            catch (Exception ex)
            {
                   throw;
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


        private bool ProcessCursor(ChangeStreamDocument<BsonDocument> change, IChangeStreamCursor<ChangeStreamDocument<BsonDocument>> cursor, IMongoCollection<BsonDocument> targetCollection, MigrationUnit item, ref int counter)
        {

            try
            {
                if (!_job.SourceServerVersion.StartsWith("3") && change.ClusterTime!=null)
                {

                    // Access the ClusterTime (timestamp) from the ChangeStreamDocument
                    var timestamp = change.ClusterTime; // Convert BsonTimestamp to DateTime

                    // Output change details to the console
                    Log.AddVerboseMessage($"{_syncBackPrefix}{change.OperationType} operation detected in {targetCollection.CollectionNamespace} for _id: {change.DocumentKey["_id"]} having TS (UTC): {MongoHelper.BsonTimestampToUtcDateTime(timestamp)}");
                    ProcessChange(change, targetCollection,_job.IsSimulatedRun);
                   
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
                    Log.AddVerboseMessage($"{_syncBackPrefix}{change.OperationType} operation detected in {targetCollection.CollectionNamespace} for _id: {change.DocumentKey["_id"]} having TS (UTC): {timestamp.Value}");
                    ProcessChange(change, targetCollection, _job.IsSimulatedRun);
                    if (!_syncBack)
                        item.CursorUtcTimestamp = timestamp.Value;
                    else
                        item.SyncBackCursorUtcTimestamp = timestamp.Value;
                }
                else
                {
                    // Output change details to the monito
                    Log.AddVerboseMessage($"{_syncBackPrefix}{change.OperationType} operation detected in {targetCollection.CollectionNamespace} for _id: {change.DocumentKey["_id"]}");
                    ProcessChange(change, targetCollection, _job.IsSimulatedRun);
                }

                //only add if not added before
                if (!_processingStartedList.ContainsKey($"{targetCollection.CollectionNamespace}"))
                    _processingStartedList.Add($"{targetCollection.CollectionNamespace}", true);
                else
                    _processingStartedList[$"{targetCollection.CollectionNamespace}"] = true; // Update to true after first run

                item.ResumeToken = cursor.Current.FirstOrDefault().ResumeToken.ToJson();
                _jobList?.Save(); // persists state

                counter++;

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


        private void ProcessChange(ChangeStreamDocument<BsonDocument> change, IMongoCollection<BsonDocument> targetCollection, bool isWriteSimulated)
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
                        targetCollection.InsertOne(change.FullDocument);
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
                            targetCollection.ReplaceOne(filter, change.FullDocument, new ReplaceOptions { IsUpsert = true });
                        }
                        break;
                    case ChangeStreamOperationType.Delete:
                        var deleteFilter = Builders<BsonDocument>.Filter.Eq("_id", idValue);
                        targetCollection.DeleteOne(deleteFilter);
                        break;
                    default:
                        Log.WriteLine($"{_syncBackPrefix}Unhandled operation type: {change.OperationType}");
                        break;
                }
            }
            catch (MongoException mex) when (change.OperationType == ChangeStreamOperationType.Insert && mex.Message.Contains("DuplicateKey"))
            {
                // Ignore duplicate key errors for inserts, typically caused by reprocessing of the same change stream
            }
            catch (Exception ex)
            {
                Log.WriteLine($"{_syncBackPrefix}Error processing operation {change.OperationType} on {targetCollection.CollectionNamespace} with _id {idValue}. Details : {ex.ToString()}", LogType.Error);
                Log.Save();
            }
        }
    }
}


