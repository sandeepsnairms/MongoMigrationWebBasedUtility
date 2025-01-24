using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OnlineMongoMigrationProcessor
{
    internal class MongoChangeStreamProcessor
    {
        public  bool ExecutionCancelled;
        MongoClient sourceClient;
        MongoClient targetClient;
        private Joblist? Jobs;
        MigrationSettings? Config;

        public MongoChangeStreamProcessor(MongoClient sourceClient, MongoClient targetClient, Joblist Jobs, MigrationSettings Config)
        {
            this.sourceClient = sourceClient;
            this.targetClient = targetClient;
            this.Jobs = Jobs;
            this.Config = Config;
        }

        public  void ProcessCollectionChangeStream(MigrationUnit item)
        {
            try
            {
                string databaseName = item.DatabaseName;
                string collectionName = item.CollectionName;

                var sourceDb = sourceClient.GetDatabase(databaseName);
                var sourceCollection = sourceDb.GetCollection<BsonDocument>(collectionName);

                var targetDb = targetClient.GetDatabase(databaseName);
                var targetCollection = targetDb.GetCollection<BsonDocument>(collectionName);

                Log.WriteLine($"Replaying change stream for {databaseName}.{collectionName}");

                while (!ExecutionCancelled)
                {

                    try
                    {
                        ChangeStreamOptions options = new ChangeStreamOptions { };

                        if (item.cursorUtcTimestamp > DateTime.MinValue)
                        {
                            var bsonTimStamp = MongoHelper.ConvertToBsonTimestamp(item.cursorUtcTimestamp.ToLocalTime());
                            options = new ChangeStreamOptions { FullDocument = ChangeStreamFullDocumentOption.UpdateLookup, StartAtOperationTime = bsonTimStamp };

                        }
                        else if (item.resumeToken != null)
                        {
                            options = new ChangeStreamOptions { FullDocument = ChangeStreamFullDocumentOption.UpdateLookup, ResumeAfter = MongoDB.Bson.BsonDocument.Parse(item.resumeToken) };
                        }
                        else if (item.ChangeStreamStartedOn.HasValue)
                        {
                            var bsonTimStamp = MongoHelper.ConvertToBsonTimestamp((DateTime)item.ChangeStreamStartedOn);
                            options = new ChangeStreamOptions { FullDocument = ChangeStreamFullDocumentOption.UpdateLookup, StartAtOperationTime = bsonTimStamp };

                        }


                        // Create a CancellationTokenSource with a timeout (e.g.,   5 minutes)
                        var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromMinutes(5));
                        CancellationToken cancellationToken = cancellationTokenSource.Token;

                        // Open a Change Stream
                        using (var cursor = sourceCollection.Watch(options))
                        {
                            int counter = 0;

                            // Continuously monitor the change stream
                            while (cursor.MoveNext(cancellationToken))
                            {
                                foreach (var change in cursor.Current)
                                {
                                    // Access the ClusterTime (timestamp) from the ChangeStreamDocument
                                    var timestamp = change.ClusterTime; // Convert BsonTimestamp to DateTime

                                    // Output change details to the console
                                    Log.AddVerboseMessage($"{change.OperationType} operation detected in {targetCollection.CollectionNamespace} for _id: {change.DocumentKey["_id"]} having TS (UTC): {MongoHelper.BsonTimestampToUtcDateTime(timestamp)}");
                                    ProcessChange(change, targetCollection);

                                    item.resumeToken = cursor.Current.FirstOrDefault().ResumeToken.ToJson();
                                    item.cursorUtcTimestamp = MongoHelper.BsonTimestampToUtcDateTime(timestamp);
                                    Jobs?.Save(); // persists state

                                    counter++;

                                    // Break if batch size is reached
                                    if (counter > Config.ChangeStreamBatchSize)
                                        break;

                                    // Break if execution is canceled
                                    if (ExecutionCancelled)
                                        break;
                                }

                                // Break the outer loop if conditions are met
                                if (counter > Config.ChangeStreamBatchSize || ExecutionCancelled)
                                    break;
                            }
                            Log.Save();
                        }

                        // Pause briefly before next iteration (optional)
                        System.Threading.Thread.Sleep(100);
                    }
                    catch (OperationCanceledException ex)
                    {
                        // Handle cancellation gracefully
                        Log.AddVerboseMessage($"CS Batch duration expired while monitoring {targetCollection.CollectionNamespace}. CS will resume automatically");
                    }
                    catch (MongoCommandException ex) when (ex.Message.Contains("Resume of change stream was not possible"))
                    {
                        // Handle other potential exceptions
                        Log.WriteLine($"Oplog is full. Error processing change stream for {targetCollection.CollectionNamespace}. Details : {ex.Message}", LogType.Error);
                        Log.Save();
                        System.Threading.Thread.Sleep(1000 * 300);
                    }
                    catch (Exception ex)
                    {
                        // Handle other potential exceptions
                        Log.WriteLine($"Error processing change stream for {targetCollection.CollectionNamespace}. Details : {ex.Message}", LogType.Error);
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
                Log.WriteLine($"Error processing change stream. Details : {ex.Message}", LogType.Error);
                Log.Save();
            }
        }



        private void ProcessChange(ChangeStreamDocument<BsonDocument> change, IMongoCollection<BsonDocument> targetCollection)
        {
            try
            {
                switch (change.OperationType)
                {
                    case ChangeStreamOperationType.Insert:
                        targetCollection.InsertOne(change.FullDocument);
                        break;
                    case ChangeStreamOperationType.Update:
                    case ChangeStreamOperationType.Replace:
                        var filter = Builders<BsonDocument>.Filter.Eq("_id", change.DocumentKey["_id"]);
                        targetCollection.ReplaceOne(filter, change.FullDocument, new ReplaceOptions { IsUpsert = true });
                        break;

                    case ChangeStreamOperationType.Delete:
                        var deleteFilter = Builders<BsonDocument>.Filter.Eq("_id", change.DocumentKey["_id"]);
                        targetCollection.DeleteOne(deleteFilter);
                        break;

                    default:
                        Log.WriteLine($"Unhandled operation type: {change.OperationType}");
                        break;
                }
            }
            catch (MongoException mex) when (change.OperationType == ChangeStreamOperationType.Insert && mex.Message.Contains("DuplicateKey"))
            {
                //ignore do nothing as insert  is duplicated, typically casused  by reprocessing of same change stream
            }
            catch (Exception ex)
            {
                Log.WriteLine($"Error processing operation {change.OperationType} on {targetCollection.CollectionNamespace} with _id {change.DocumentKey["_id"]}. Details : {ex.Message}", LogType.Error);
                Log.Save();
            }
        }
    }
}
