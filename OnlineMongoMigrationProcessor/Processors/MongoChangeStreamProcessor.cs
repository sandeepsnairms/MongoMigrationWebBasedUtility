﻿using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Linq;
using System.Reflection.Metadata;
using System.Threading;
using System.Threading.Tasks;

#pragma warning disable CS8602 // Dereference of a possibly null reference.

namespace OnlineMongoMigrationProcessor
{
    internal class MongoChangeStreamProcessor
    {
        private MongoClient _sourceClient;
        private MongoClient _targetClient;
        private JobList? _jobs;
        private MigrationSettings? _config;
        private bool _syncBack = false;
        private string _syncBackPrefix = string.Empty;
            

        private  Dictionary<string, bool> _processingStartedList = new Dictionary<string, bool>();
        //private bool skipFirst = false;

        public bool ExecutionCancelled { get; set; }
        

        public MongoChangeStreamProcessor(MongoClient sourceClient, MongoClient targetClient, JobList jobs, MigrationSettings config, bool syncBack = false)
        {
            _sourceClient = sourceClient;
            _targetClient = targetClient;
            _jobs = jobs;
            _config = config;
            _syncBack = syncBack;
            if (_syncBack)
                _syncBackPrefix = "Sync Back: ";
        }

        public void ProcessCollectionChangeStream(MigrationJob job, MigrationUnit item)
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
                

                Log.WriteLine($"{_syncBackPrefix}Replaying change stream for {databaseName}.{collectionName}");
               

                while (!ExecutionCancelled && item.DumpComplete)
                {
                    try
                    {
                        ChangeStreamOptions options = new ChangeStreamOptions { };

                        if (!_syncBack)
                        {

                            if (item.CursorUtcTimestamp > DateTime.MinValue && !job.SourceServerVersion.StartsWith("3"))
                            {
                                var bsonTimestamp = MongoHelper.ConvertToBsonTimestamp(item.CursorUtcTimestamp.ToLocalTime());
                                options = new ChangeStreamOptions { BatchSize = 100, FullDocument = ChangeStreamFullDocumentOption.UpdateLookup, StartAtOperationTime = bsonTimestamp };
                            }
                            else if (item.ResumeToken != null)
                            {
                                options = new ChangeStreamOptions { BatchSize = 100, FullDocument = ChangeStreamFullDocumentOption.UpdateLookup, ResumeAfter = BsonDocument.Parse(item.ResumeToken) };
                            }
                            else if (item.ResumeToken == null && job.SourceServerVersion.StartsWith("3"))
                            {
                                options = new ChangeStreamOptions { BatchSize = 100, FullDocument = ChangeStreamFullDocumentOption.UpdateLookup };
                            }
                            else if (item.ChangeStreamStartedOn.HasValue && !job.SourceServerVersion.StartsWith("3"))
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
                        var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromMinutes(5));
                        CancellationToken cancellationToken = cancellationTokenSource.Token;

                        bool skipFirst = false;
                        _processingStartedList.TryGetValue($"{databaseName}.{collectionName}", out skipFirst);

                        WatchCollection(job, item, options, sourceCollection, targetCollection, cancellationToken, skipFirst);                        

                        Log.AddVerboseMessage($"{_syncBackPrefix}Monitoring change stream with new batch for {targetCollection.CollectionNamespace}");

                        // Pause briefly before next iteration
                        Thread.Sleep(100);
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
            }
            catch (Exception ex)
            {
                Log.WriteLine($"{_syncBackPrefix}Error processing change stream. Details : {ex.ToString()}", LogType.Error);
                Log.Save();
            }
        }



        private void WatchCollection(MigrationJob job,MigrationUnit item, ChangeStreamOptions options, IMongoCollection<BsonDocument> sourceCollection, IMongoCollection<BsonDocument> targetCollection, CancellationToken cancellationToken, bool skipFirst)
        {
            string host = string.Empty;
            if (_syncBack)
            {
                host = job.TargetEndpoint;
            }
            else
            {
                host = job.SourceEndpoint;
            }
            bool isVCore = host.Contains("mongocluster.cosmos.azure.com");

            try
            {
                // Open a change stream
                using (var cursor = sourceCollection.Watch(options))
                {
                    int counter = 0;

                    // Continuously monitor the change stream
                    //mongo 3.6 has different implementation for change stream
                    if (job.SourceServerVersion.StartsWith("3"))
                    {
                        if (item.ResumeDocumentId != null && !item.ResumeDocumentId.IsBsonNull)
                        {
                            //manually replay  the first change as the ResumeAfter skips the current item
                            AutoReplayFirstChangeInResumeToken(item.ResumeDocumentId, item.ResumeTokenOperation, sourceCollection, targetCollection);
                        }

                        //proceed with the rest of the changes
                        foreach (ChangeStreamDocument<BsonDocument> change in cursor.ToEnumerable())
                        {
                            counter++;
                            if(counter>1 || !skipFirst) // skip first change if skipFirst is true
                            {
                                if (ProcessCursor(job, change, cursor, targetCollection, item, ref counter) == false)
                                {
                                    break;
                                }
                            }                            
                            cancellationToken.ThrowIfCancellationRequested();
                        }
                    }
                    else
                    {
                        while (cursor.MoveNext(cancellationToken))
                        {
                            foreach (var change in cursor.Current)
                            {
                                counter++;
                                if (counter > 1 || !skipFirst || isVCore) // skip first change if skipFirst is true
                                {
                                    if (ProcessCursor(job, change, cursor, targetCollection, item, ref counter) == false)
                                    {
                                        break;
                                    }
                                }
                                cancellationToken.ThrowIfCancellationRequested();
                            }

                            cancellationToken.ThrowIfCancellationRequested();

                            // Break the outer loop if conditions are met
                            if (counter > _config.ChangeStreamBatchSize || ExecutionCancelled)
                                break;
                        }
                    }
                    Log.Save();
                }
            }
            catch (OperationCanceledException)
            {
                // Handled cancellation gracefully               
            }
            catch
            {
                throw; // Rethrow the exception to be handled by the caller
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


        private bool ProcessCursor(MigrationJob job, ChangeStreamDocument<BsonDocument> change, IChangeStreamCursor<ChangeStreamDocument<BsonDocument>> cursor, IMongoCollection<BsonDocument> targetCollection, MigrationUnit item, ref int counter)
        {

            try
            {
                if (!job.SourceServerVersion.StartsWith("3") && change.ClusterTime!=null)
                {

                    // Access the ClusterTime (timestamp) from the ChangeStreamDocument
                    var timestamp = change.ClusterTime; // Convert BsonTimestamp to DateTime

                    // Output change details to the console
                    Log.AddVerboseMessage($"{_syncBackPrefix}{change.OperationType} operation detected in {targetCollection.CollectionNamespace} for _id: {change.DocumentKey["_id"]} having TS (UTC): {MongoHelper.BsonTimestampToUtcDateTime(timestamp)}");
                    ProcessChange(change, targetCollection,job.IsSimulatedRun);
                   
                    if (!_syncBack)
                        item.CursorUtcTimestamp = MongoHelper.BsonTimestampToUtcDateTime(timestamp);
                    else
                        item.SyncBackCursorUtcTimestamp = MongoHelper.BsonTimestampToUtcDateTime(timestamp); //for reverse sync
                }
                else if (!job.SourceServerVersion.StartsWith("3") && change.WallTime != null) //for vcore
                {
                    // use Walltime
                    var timestamp = change.WallTime;

                    // Output change details to the console
                    Log.AddVerboseMessage($"{_syncBackPrefix}{change.OperationType} operation detected in {targetCollection.CollectionNamespace} for _id: {change.DocumentKey["_id"]} having TS (UTC): {timestamp.Value}");
                    ProcessChange(change, targetCollection, job.IsSimulatedRun);
                    if (!_syncBack)
                        item.CursorUtcTimestamp = timestamp.Value;
                    else
                        item.SyncBackCursorUtcTimestamp = timestamp.Value;
                }
                else
                {
                    // Output change details to the console
                    Log.AddVerboseMessage($"{_syncBackPrefix}{change.OperationType} operation detected in {targetCollection.CollectionNamespace} for _id: {change.DocumentKey["_id"]}");
                    ProcessChange(change, targetCollection, job.IsSimulatedRun);
                }

                //only add if not added before
                if (!_processingStartedList.ContainsKey($"{targetCollection.CollectionNamespace}"))
                    _processingStartedList.Add($"{targetCollection.CollectionNamespace}", true);
                else
                    _processingStartedList[$"{targetCollection.CollectionNamespace}"] = true; // Update to true after first run

                item.ResumeToken = cursor.Current.FirstOrDefault().ResumeToken.ToJson();
                _jobs?.Save(); // persists state

                counter++;

                // Break if batch size is reached
                if (counter > _config.ChangeStreamBatchSize)
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


