using MongoDB.Bson;
using MongoDB.Driver;
using OnlineMongoMigrationProcessor.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace OnlineMongoMigrationProcessor.Helpers
{
    /// <summary>
    /// Helper class for managing aggressive change stream functionality.
    /// Handles temporary storage of document keys during delete operations before bulk copy completion.
    /// </summary>
    public class AggressiveChangeStreamHelper
    {
        private readonly MongoClient _targetClient;
        private readonly Log _log;
        private readonly string _jobId;

        public AggressiveChangeStreamHelper(MongoClient targetClient, Log log, string jobId)
        {
            _targetClient = targetClient ?? throw new ArgumentNullException(nameof(targetClient));
            _log = log ?? throw new ArgumentNullException(nameof(log));
            _jobId = jobId ?? throw new ArgumentNullException(nameof(jobId));
        }

        /// <summary>
        /// Stores a document key in the temporary collection for later deletion after bulk copy completion.
        /// </summary>
        /// <param name="sourceDatabaseName">Source database name</param>
        /// <param name="sourceCollectionName">Source collection name</param>
        /// <param name="documentKey">Document key from change stream delete event</param>
        /// <returns>True if successfully stored, false otherwise</returns>
        public async Task<bool> StoreDocumentKeyAsync(string sourceDatabaseName, string sourceCollectionName, BsonDocument documentKey)
        {
            try
            {
                var tempDb = _targetClient.GetDatabase(_jobId);
                var tempCollectionName = $"{sourceDatabaseName}_{sourceCollectionName}";
                var tempCollection = tempDb.GetCollection<BsonDocument>(tempCollectionName);

                var tempDocument = new BsonDocument
                {
                    ["_id"] = ObjectId.GenerateNewId(), // Use generated ObjectId for temp collection
                    ["documentKey"] = documentKey,
                    ["createdAt"] = DateTime.UtcNow
                };

                await tempCollection.InsertOneAsync(tempDocument);               
                return true;
            }
            catch (Exception ex)
            {
                _log.WriteLine($"Error storing document key for aggressive change stream: {ex.Message}", LogType.Error);
                return false;
            }
        }

        /// <summary>
        /// Stores multiple document keys in the temporary collection for later deletion after bulk copy completion.
        /// This is a batch version for better performance.
        /// </summary>
        /// <param name="sourceDatabaseName">Source database name</param>
        /// <param name="sourceCollectionName">Source collection name</param>
        /// <param name="documentKeys">Collection of document keys from change stream delete events</param>
        /// <returns>Number of successfully stored documents</returns>
        public async Task<int> StoreDocumentKeysAsync(string sourceDatabaseName, string sourceCollectionName, IEnumerable<BsonDocument> documentKeys)
        {
            try
            {
                var tempDb = _targetClient.GetDatabase(_jobId);
                var tempCollectionName = $"{sourceDatabaseName}_{sourceCollectionName}";
                var tempCollection = tempDb.GetCollection<BsonDocument>(tempCollectionName);

                var tempDocuments = documentKeys.Select(documentKey => new BsonDocument
                {
                    ["_id"] = ObjectId.GenerateNewId(), // Use generated ObjectId for temp collection
                    ["documentKey"] = documentKey,
                    ["createdAt"] = DateTime.UtcNow
                }).ToList();

                if (tempDocuments.Count == 0)
                {
                    return 0;
                }

                await tempCollection.InsertManyAsync(tempDocuments, new InsertManyOptions { IsOrdered = false });
                _log.AddVerboseMessage($"Stored {tempDocuments.Count} document keys for aggressive change stream delete in temp collection {tempCollectionName}");
                return tempDocuments.Count;
            }
            catch (Exception ex)
            {
                _log.WriteLine($"Error storing document keys for aggressive change stream: {ex.Message}", LogType.Error);
                return 0;
            }
        }

        /// <summary>
        /// Removes a document key from the temporary collection when an insert/update is processed for the same key.
        /// </summary>
        /// <param name="sourceDatabaseName">Source database name</param>
        /// <param name="sourceCollectionName">Source collection name</param>
        /// <param name="documentKey">Document key to remove</param>
        /// <returns>True if successfully removed, false otherwise</returns>
        public async Task<bool> RemoveDocumentKeyAsync(string sourceDatabaseName, string sourceCollectionName, BsonDocument documentKey)
        {
            try
            {
                var tempDb = _targetClient.GetDatabase(_jobId);
                var tempCollectionName = $"{sourceDatabaseName}_{sourceCollectionName}";
                var tempCollection = tempDb.GetCollection<BsonDocument>(tempCollectionName);

                var filter = Builders<BsonDocument>.Filter.Eq("documentKey", documentKey);
                var result = await tempCollection.DeleteManyAsync(filter);

                if (result.DeletedCount > 0)
                {
                    _log.AddVerboseMessage($"Removed {result.DeletedCount} document key(s) from temp collection {tempCollectionName}: {documentKey.ToJson()}");
                }

                return true;
            }
            catch (Exception ex)
            {
                _log.WriteLine($"Error removing document key from temp collection: {ex.Message}", LogType.Error);
                return false;
            }
        }

        /// <summary>
        /// Removes multiple document keys from the temporary collection when inserts/updates are processed for the same keys.
        /// This is a batch version for better performance.
        /// </summary>
        /// <param name="sourceDatabaseName">Source database name</param>
        /// <param name="sourceCollectionName">Source collection name</param>
        /// <param name="documentKeys">Collection of document keys to remove</param>
        /// <returns>Number of successfully removed documents</returns>
        public async Task<long> RemoveDocumentKeysAsync(string sourceDatabaseName, string sourceCollectionName, IEnumerable<BsonDocument> documentKeys)
        {
            try
            {
                var tempDb = _targetClient.GetDatabase(_jobId);
                var tempCollectionName = $"{sourceDatabaseName}_{sourceCollectionName}";
                var tempCollection = tempDb.GetCollection<BsonDocument>(tempCollectionName);

                var filters = documentKeys.Select(documentKey => 
                    Builders<BsonDocument>.Filter.Eq("documentKey", documentKey)).ToList();

                if (filters.Count == 0)
                {
                    return 0;
                }

                // Use $or filter to match any of the document keys
                var orFilter = Builders<BsonDocument>.Filter.Or(filters);
                var result = await tempCollection.DeleteManyAsync(orFilter);

                if (result.DeletedCount > 0)
                {
                    _log.AddVerboseMessage($"Removed {result.DeletedCount} document key(s) from temp collection {tempCollectionName}");
                }

                return result.DeletedCount;
            }
            catch (Exception ex)
            {
                _log.WriteLine($"Error removing document keys from temp collection: {ex.Message}", LogType.Error);
                return 0;
            }
        }

        /// <summary>
        /// Processes all stored document keys for deletion after bulk copy completion.
        /// </summary>
        /// <param name="sourceDatabaseName">Source database name</param>
        /// <param name="sourceCollectionName">Source collection name</param>
        /// <returns>Number of documents deleted from target collection</returns>
        public async Task<long> DeleteStoredDocsAsync(string sourceDatabaseName, string sourceCollectionName)
        {
            try
            {
                var tempDb = _targetClient.GetDatabase(_jobId);
                var tempCollectionName = $"{sourceDatabaseName}_{sourceCollectionName}";
                var tempCollection = tempDb.GetCollection<BsonDocument>(tempCollectionName);

                // Check if temp collection exists
                var collectionNames = await tempDb.ListCollectionNamesAsync();
                var collectionList = await collectionNames.ToListAsync();
                if (!collectionList.Contains(tempCollectionName))
                {
                    return 0;
                }

                // Get all stored document keys
                var storedKeys = await tempCollection.Find(Builders<BsonDocument>.Filter.Empty).ToListAsync();
                
                if (storedKeys.Count == 0)
                {                   
                    return 0;
                }

                // Get target collection
                var targetDb = _targetClient.GetDatabase(sourceDatabaseName);
                var targetCollection = targetDb.GetCollection<BsonDocument>(sourceCollectionName);

                long deletedCount = 0;
                const int batchSize = 100;

                // Process deletes in batches
                for (int i = 0; i < storedKeys.Count; i += batchSize)
                {
                    var batch = storedKeys.Skip(i).Take(batchSize);
                    var deleteModels = new List<DeleteOneModel<BsonDocument>>();

                    foreach (var storedDoc in batch)
                    {
                        if (storedDoc.Contains("documentKey"))
                        {
                            var documentKey = storedDoc["documentKey"].AsBsonDocument;
                            var filter = MongoHelper.BuildFilterFromDocumentKey(documentKey);
                            deleteModels.Add(new DeleteOneModel<BsonDocument>(filter));
                        }
                    }

                    if (deleteModels.Count > 0)
                    {
                        try
                        {
                            var result = await targetCollection.BulkWriteAsync(deleteModels, new BulkWriteOptions { IsOrdered = false });
                            deletedCount += result.DeletedCount;
                            _log.AddVerboseMessage($"Deleted {result.DeletedCount} documents from {sourceDatabaseName}.{sourceCollectionName} (batch {i / batchSize + 1})");
                        }
                        catch (MongoBulkWriteException<BsonDocument> ex)
                        {
                            deletedCount += ex.Result?.DeletedCount ?? 0;
                            _log.WriteLine($"Bulk delete partially failed for {sourceDatabaseName}.{sourceCollectionName}: {ex.Message}", LogType.Error);
                        }
                    }
                }

                // Clean up temp collection after processing
                await tempDb.DropCollectionAsync(tempCollectionName);
                _log.WriteLine($"Processed aggressive change stream deletes for {sourceDatabaseName}.{sourceCollectionName}: {deletedCount} documents deleted, temp collection cleaned up");

                return deletedCount;
            }
            catch (Exception ex)
            {
                _log.WriteLine($"Error processing stored deletes for {sourceDatabaseName}.{sourceCollectionName}: {ex.Message}", LogType.Error);
                return 0;
            }
        }

        /// <summary>
        /// Cleans up all temporary collections for the job.
        /// </summary>
        /// <returns>True if cleanup was successful, false otherwise</returns>
        public async Task<bool> CleanupTempCollectionsAsync()
        {
            try
            {
                var tempDb = _targetClient.GetDatabase(_jobId);
                await _targetClient.DropDatabaseAsync(_jobId);
                _log.WriteLine($"Cleaned up all temporary collections for job {_jobId}");
                return true;
            }
            catch (Exception ex)
            {
                _log.WriteLine($"Error cleaning up temporary collections for job {_jobId}: {ex.Message}", LogType.Error);
                return false;
            }
        }
    }
}