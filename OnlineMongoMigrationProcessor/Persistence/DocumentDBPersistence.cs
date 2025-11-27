using MongoDB.Bson;
using MongoDB.Driver;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using ZstdSharp.Unsafe;

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
        private static IMongoCollection<BsonDocument>? _dataCollection;
        private static IMongoCollection<BsonDocument>? _logCollection;
        private static bool _isInitialized = false;
        private static readonly object _initLock = new object();
        private static string _appId = string.Empty;
        private const string DATABASE_NAME = "mongomigrationwebappstorage";
        private const string DATA_Collection = "datafiles";
        private const string LOG_Collection = "logfiles";

        /// <summary>
        /// Initializes the MongoDB persistence layer with the provided connection string.
        /// This method is thread-safe and idempotent.
        /// </summary>
        /// <param name="connectionStringOrPath">MongoDB connection string</param>
        /// <exception cref="ArgumentException">Thrown when connection string is null or empty</exception>
        /// <exception cref="InvalidOperationException">Thrown when initialization fails</exception>
        public override void Initialize(string connectionStringOrPath,string appId)
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
                    _dataCollection = _database.GetCollection<BsonDocument>(DATA_Collection);
                    _logCollection = _database.GetCollection<BsonDocument>(LOG_Collection);
                    var collectionNames = _database.ListCollectionNames().ToListAsync().GetAwaiter().GetResult();

                    if (!collectionNames.Contains(DATA_Collection))
                    {
                        _database.CreateCollectionAsync(DATA_Collection).GetAwaiter().GetResult();
                    }

                    if (!collectionNames.Contains(LOG_Collection))
                    {
                        _database.CreateCollectionAsync(LOG_Collection).GetAwaiter().GetResult();
                    }

                    _appId = appId;
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
            if (!_isInitialized || _dataCollection == null)
                throw new InvalidOperationException("DocumentDBPersistence is not initialized. Call Initialize() first with a valid connection string.");
        }

        /// <summary>
        /// Normalizes the ID for MongoDB by replacing backslashes and forward slashes with underscores.
        /// Handles hierarchical IDs like "job1\mu1" by converting them to "job1_mu1".
        /// </summary>
        private static string NormalizeIdForMongo(string id)
        {
            return $"{_appId}.{id.Replace('\\', '_').Replace('/', '_')}";
        }

        /// <summary>
        /// Upserts a document with the specified _id.
        /// Creates a new document if it doesn't exist, updates if it does.
        /// </summary>
        /// <param name="id">Unique identifier (_id) for the document</param>
        /// <param name="jsonContent">JSON content to store</param>
        /// <returns>True if successful, false otherwise</returns>
        public override bool UpsertDocument(string id, string jsonContent)
        {
            return Upsert(id, jsonContent, false);
        }

        private bool Upsert(string id, string jsonContent, bool IsLog)
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

                if (IsLog)
                    _logCollection!.ReplaceOne(filter, document, new ReplaceOptions { IsUpsert = true });
                else
                    _dataCollection!.ReplaceOne(filter, document, new ReplaceOptions { IsUpsert = true });

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
        public override string? ReadDocument(string id)
        {
            EnsureInitialized();

            if (string.IsNullOrWhiteSpace(id))
                throw new ArgumentException("ID cannot be null or empty", nameof(id));

            try
            {
                var normalizedId = NormalizeIdForMongo(id);
                var filter = Builders<BsonDocument>.Filter.Eq("_id", normalizedId);
                var document = _dataCollection!.Find(filter).FirstOrDefault();
                
                if (document == null)
                    return null;

                
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
        public override bool DocumentExists(string id)
        {
            EnsureInitialized();

            if (string.IsNullOrWhiteSpace(id))
                return false;

            try
            {
                var normalizedId = NormalizeIdForMongo(id);
                var filter = Builders<BsonDocument>.Filter.Eq("_id", normalizedId);
                var count = _dataCollection!.CountDocuments(filter);
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
        public override bool DeleteDocument(string id)
        {
            return Delete(id);
        }
        private bool Delete(string id, bool isLog = false)
        {
            EnsureInitialized();

            if (string.IsNullOrWhiteSpace(id))
                throw new ArgumentException("ID cannot be null or empty", nameof(id));

            try
            {
                var normalizedId = NormalizeIdForMongo(id);
                var filter = Builders<BsonDocument>.Filter.Eq("_id", normalizedId);

                IMongoCollection<BsonDocument> collectionToUse = isLog ? _logCollection! : _dataCollection!;
                
                var result = collectionToUse.DeleteOne(filter);
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
        public override List<string> ListDocumentIds()
        {
            EnsureInitialized();

            try
            {
                var projection = Builders<BsonDocument>.Projection.Include("_id");
                var documents = _dataCollection!.Find(Builders<BsonDocument>.Filter.Empty)
                    .Project(projection)
                    .ToList();

                return documents.Select(doc => doc["_id"].AsString).ToList();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[DocumentDBPersistence] Error listing document IDs: {ex.Message}");
                return new List<string>();
            }
        }

        public override byte[] DownloadLogsAsJsonBytes(string Id, int topEntries = 20, int bottomEntries = 230)
        {
            EnsureInitialized();
            if (string.IsNullOrWhiteSpace(Id))
                throw new ArgumentException("Path cannot be null or empty", nameof(Id));
            try
            {
                var normalizedId = NormalizeIdForMongo(Id);
                var filter = Builders<BsonDocument>.Filter.Eq("_id", normalizedId);
                var document = _logCollection!.Find(filter).FirstOrDefault();
                if (document != null)
                {
                    var logsArray = document.GetValue("logs").AsBsonArray;
                    var selectedLogs = new BsonArray();
                    int totalLogs = logsArray.Count;

                    //if toptopEntries=0 and bottomEntries=0 return all logs
                    if (topEntries == 0 && bottomEntries == 0)
                    {
                        selectedLogs = logsArray;
                    }
                    else
                    {
                        for (int i = 0; i < Math.Min(topEntries, totalLogs); i++)
                        {
                            selectedLogs.Add(logsArray[i]);
                        }
                        for (int i = Math.Max(totalLogs - bottomEntries, topEntries); i < totalLogs; i++)
                        {
                            selectedLogs.Add(logsArray[i]);
                        }
                    }
                    var resultDocument = new BsonDocument
                    {
                        { "_id", normalizedId },
                        { "logs", selectedLogs }
                    };
                    var jsonString = resultDocument.ToJson(new MongoDB.Bson.IO.JsonWriterSettings
                    {
                        Indent = true,       // enable pretty formatting
                        IndentChars = "  "   // optional: two spaces
                    });
                    return System.Text.Encoding.UTF8.GetBytes(jsonString);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[DocumentDBPersistence] Error downloading logs for {Id}: {ex.Message}");
            }
            return Array.Empty<byte>();
        }


        public override LogBucket ReadLogs(string id, out string fileName)
        {
            fileName = id;// $"{id}_logs.json";
            if (string.IsNullOrWhiteSpace(id))
                throw new ArgumentException("ID cannot be null or empty", nameof(id));

            try
            {
                var normalizedId = NormalizeIdForMongo(id);
                var filter = Builders<BsonDocument>.Filter.Eq("_id", normalizedId);
                var document = _logCollection!.Find(filter).FirstOrDefault();

                if (document != null)
                {                   
                    var logsArray = document.GetValue("logs").AsBsonArray;
                    var logObjects = logsArray.Select(logBson => JsonConvert.DeserializeObject<LogObject>(logBson.ToJson())).Where(lo => lo != null).ToList()!;
                    var logBucket = new LogBucket
                    {
                        Logs = logObjects
                    };
                    
                    return logBucket;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[DocumentDBPersistence] Error reading logs for job {id}: {ex.Message}");
            }

            return new LogBucket(); 
        }

        public override void PushLogEntry(string jobId, LogObject logObject)
        {
            //store one document per jobId, each logObject is an entry in an array field "logs"
            //if document exist  use push to add logObject to logs array, addnew entries at the end
            EnsureInitialized();
            if (string.IsNullOrWhiteSpace(jobId))
                throw new ArgumentException("Job ID cannot be null or empty", nameof(jobId));
            try
                {
                var normalizedId = NormalizeIdForMongo(jobId);
                var filter = Builders<BsonDocument>.Filter.Eq("_id", normalizedId);
                var logEntryBson = BsonDocument.Parse(JsonConvert.SerializeObject(logObject));
                var update = Builders<BsonDocument>.Update.Push("logs", logEntryBson);
                _logCollection!.UpdateOne(filter, update, new UpdateOptions { IsUpsert = true });
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[DocumentDBPersistence] Error pushing log entry for job {jobId}: {ex.Message}");
            }

        }

        /// <summary>
        /// Tests the connection to MongoDB storage
        /// </summary>
        /// <returns>True if connection is successful, false otherwise</returns>
        public override bool TestConnection()
        {
            if (!_isInitialized)
                return false;

            try
            {
                _database!.ListCollectionNames();
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
