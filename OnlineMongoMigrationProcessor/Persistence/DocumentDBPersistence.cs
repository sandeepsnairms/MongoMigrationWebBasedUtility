using MongoDB.Bson;
using MongoDB.Driver;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace OnlineMongoMigrationProcessor.Persistence
{
    /// <summary>
    /// MongoDB implementation of PersistenceStorage.
    /// Persists objects (MigrationJob, MigrationUnit, JobList, MigrationSettings) 
    /// to MongoDB storage using _id as the key.
    /// </summary>
    public class DocumentDBPersistence : PersistenceStorage
    {
        private static IMongoClient? _client;
        private static IMongoDatabase? _database;
        private static IMongoCollection<BsonDocument>? _collection;
        private static bool _isInitialized = false;
        private static readonly object _initLock = new object();
        
        private const string DATABASE_NAME = "mongomigrationwebappstorage";
        private const string COLLECTION_NAME = "datafiles";

        /// <summary>
        /// Initializes the MongoDB persistence layer with the provided connection string.
        /// This method is thread-safe and idempotent.
        /// </summary>
        /// <param name="connectionStringOrPath">MongoDB connection string</param>
        /// <exception cref="ArgumentException">Thrown when connection string is null or empty</exception>
        /// <exception cref="InvalidOperationException">Thrown when initialization fails</exception>
        public override void Initialize(string connectionStringOrPath)
        {
            if (_isInitialized)
                return;

            lock (_initLock)
            {
                if (_isInitialized)
                    return;

                if (string.IsNullOrWhiteSpace(connectionStringOrPath))
                    throw new ArgumentException("Connection string cannot be null or empty", nameof(connectionStringOrPath));

                try
                {
                    _client = new MongoClient(connectionStringOrPath);
                    _database = _client.GetDatabase(DATABASE_NAME);
                    _collection = _database.GetCollection<BsonDocument>(COLLECTION_NAME);
                    
                    _isInitialized = true;
                }
                catch (Exception ex)
                {
                    throw new InvalidOperationException($"Failed to initialize DocumentDBPersistence: {ex.Message}", ex);
                }
            }
        }

        /// <summary>
        /// Ensures the storage is initialized before operations
        /// </summary>
        private static void EnsureInitialized()
        {
            if (!_isInitialized || _collection == null)
                throw new InvalidOperationException("DocumentDBPersistence is not initialized. Call Initialize() first with a valid connection string.");
        }

        /// <summary>
        /// Normalizes the ID for MongoDB by replacing backslashes and forward slashes with underscores.
        /// Handles hierarchical IDs like "job1\mu1" by converting them to "job1_mu1".
        /// </summary>
        private static string NormalizeIdForMongo(string id)
        {
            return id.Replace('\\', '_').Replace('/', '_');
        }

        /// <summary>
        /// Upserts a document with the specified _id.
        /// Creates a new document if it doesn't exist, updates if it does.
        /// </summary>
        /// <param name="id">Unique identifier (_id) for the document</param>
        /// <param name="jsonContent">JSON content to store</param>
        /// <returns>True if successful, false otherwise</returns>
        public override async Task<bool> UpsertDocumentAsync(string id, string jsonContent)
        {
            EnsureInitialized();

            if (string.IsNullOrWhiteSpace(id))
                throw new ArgumentException("ID cannot be null or empty", nameof(id));

            if (string.IsNullOrWhiteSpace(jsonContent))
                throw new ArgumentException("JSON content cannot be null or empty", nameof(jsonContent));

            try
            {
                var normalizedId = NormalizeIdForMongo(id);
                var document = BsonDocument.Parse(jsonContent);
                document["_id"] = normalizedId;

                var filter = Builders<BsonDocument>.Filter.Eq("_id", normalizedId);
                await _collection!.ReplaceOneAsync(filter, document, new ReplaceOptions { IsUpsert = true });

                return true;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[DocumentDBPersistence] Error upserting document {id}: {ex.Message}");
                return false;
            }
        }

        /// <summary>
        /// Reads a document by its _id and returns it with Id property restored
        /// </summary>
        /// <param name="id">Unique identifier (_id) of the document</param>
        /// <returns>JSON content with Id property if found, null otherwise</returns>
        public override async Task<string?> ReadDocumentAsync(string id)
        {
            EnsureInitialized();

            if (string.IsNullOrWhiteSpace(id))
                throw new ArgumentException("ID cannot be null or empty", nameof(id));

            try
            {
                var normalizedId = NormalizeIdForMongo(id);
                var filter = Builders<BsonDocument>.Filter.Eq("_id", normalizedId);
                var document = await _collection!.Find(filter).FirstOrDefaultAsync();
                
                if (document == null)
                    return null;

                // Convert _id back to Id property for consistency with your models
                var idValue = document["_id"];
                document.Remove("_id");
                document["Id"] = idValue;
                
                return document.ToJson();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[DocumentDBPersistence] Error reading document {id}: {ex.Message}");
                return null;
            }
        }

        /// <summary>
        /// Checks if a document exists by its _id
        /// </summary>
        /// <param name="id">Unique identifier (_id) of the document</param>
        /// <returns>True if document exists, false otherwise</returns>
        public override async Task<bool> DocumentExistsAsync(string id)
        {
            EnsureInitialized();

            if (string.IsNullOrWhiteSpace(id))
                return false;

            try
            {
                var normalizedId = NormalizeIdForMongo(id);
                var filter = Builders<BsonDocument>.Filter.Eq("_id", normalizedId);
                var count = await _collection!.CountDocumentsAsync(filter);
                return count > 0;
            }
            catch
            {
                return false;
            }
        }

        /// <summary>
        /// Deletes a document by its _id
        /// </summary>
        /// <param name="id">Unique identifier (_id) of the document to delete</param>
        /// <returns>True if deleted, false otherwise</returns>
        public override async Task<bool> DeleteDocumentAsync(string id)
        {
            EnsureInitialized();

            if (string.IsNullOrWhiteSpace(id))
                throw new ArgumentException("ID cannot be null or empty", nameof(id));

            try
            {
                var normalizedId = NormalizeIdForMongo(id);
                var filter = Builders<BsonDocument>.Filter.Eq("_id", normalizedId);
                var result = await _collection!.DeleteOneAsync(filter);
                return result.DeletedCount > 0;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[DocumentDBPersistence] Error deleting document {id}: {ex.Message}");
                return false;
            }
        }

        /// <summary>
        /// Lists all document IDs in the collection
        /// </summary>
        /// <returns>List of document IDs</returns>
        public override async Task<List<string>> ListDocumentIdsAsync()
        {
            EnsureInitialized();

            try
            {
                var projection = Builders<BsonDocument>.Projection.Include("_id");
                var documents = await _collection!.Find(Builders<BsonDocument>.Filter.Empty)
                    .Project(projection)
                    .ToListAsync();
                
                return documents.Select(doc => doc["_id"].AsString).ToList();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[DocumentDBPersistence] Error listing document IDs: {ex.Message}");
                return new List<string>();
            }
        }

        /// <summary>
        /// Pushes a LogObject to the LogEntries array in the document.
        /// If the document doesn't exist, it creates a new one with the LogEntries array.
        /// If it exists, appends the LogObject to the existing array.
        /// </summary>
        /// <param name="id">Unique identifier (_id) of the document</param>
        /// <param name="logObject">LogObject to push to the array</param>
        /// <returns>True if successful, false otherwise</returns>
        public override async Task<bool> PushLogEntryAsync(string id, LogObject logObject)
        {
            EnsureInitialized();

            if (string.IsNullOrWhiteSpace(id))
                throw new ArgumentException("ID cannot be null or empty", nameof(id));

            if (logObject == null)
                throw new ArgumentNullException(nameof(logObject));

            try
            {
                var normalizedId = NormalizeIdForMongo(id);
                var filter = Builders<BsonDocument>.Filter.Eq("_id", normalizedId);
                
                // Convert LogObject to BsonDocument
                var json = JsonConvert.SerializeObject(logObject);
                var logBsonDocument = BsonDocument.Parse(json);
                
                // Use $push to add to LogEntries array, create document if it doesn't exist
                var update = Builders<BsonDocument>.Update
                    .Push("LogEntries", logBsonDocument)
                    .SetOnInsert("_id", normalizedId);
                
                var options = new UpdateOptions { IsUpsert = true };
                await _collection!.UpdateOneAsync(filter, update, options);

                return true;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[DocumentDBPersistence] Error pushing log entry to document {id}: {ex.Message}");
                return false;
            }
        }

        /// <summary>
        /// Tests the connection to MongoDB storage
        /// </summary>
        /// <returns>True if connection is successful, false otherwise</returns>
        public override async Task<bool> TestConnectionAsync()
        {
            if (!_isInitialized)
                return false;

            try
            {
                await _database!.ListCollectionNamesAsync();
                return true;
            }
            catch
            {
                return false;
            }
        }

        /// <summary>
        /// Checks if the storage is initialized
        /// </summary>
        public override bool IsInitialized => _isInitialized;
    }
}
