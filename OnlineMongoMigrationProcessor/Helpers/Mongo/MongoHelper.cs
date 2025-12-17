using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Driver;
using MongoDB.Driver.Core.Clusters;
using MongoDB.Driver.GeoJsonObjectModel.Serializers;
using Newtonsoft.Json.Linq;
using OnlineMongoMigrationProcessor.Context;
using OnlineMongoMigrationProcessor.Models;
using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;


// Nullability handled explicitly; removed pragmas.

namespace OnlineMongoMigrationProcessor.Helpers.Mongo
{
    internal static class MongoHelper
    {
        // Define the new delegate type - made public for use in ParallelWriteProcessor
        public delegate void CounterDelegate<TMigration>(
             TMigration migration,
             CounterType type,
             ChangeStreamOperationType? operationType = null,
             int count = 1);

        /// <summary>
        /// Determines if an exception is transient and should be retried
        /// </summary>
        private static bool IsTransientException(Exception ex)
        {
            // Check for timeout exceptions
            if (ex is TimeoutException)
                return true;

            // Check for MongoDB connection exceptions
            if (ex is MongoConnectionException)
                return true;

            // Check for MongoDB execution timeout
            if (ex is MongoExecutionTimeoutException)
                return true;

            // Check for network/socket exceptions in inner exceptions
            if (ex.InnerException != null)
            {
                if (ex.InnerException is System.Net.Sockets.SocketException)
                    return true;
                if (ex.InnerException is IOException)
                    return true;
            }

            // Check exception messages for common transient error patterns
            var message = ex.Message?.ToLower() ?? string.Empty;
            if (message.Contains("timeout") ||
                message.Contains("timed out") ||
                message.Contains("connection") ||
                message.Contains("network") ||
                message.Contains("socket") ||
                message.Contains("server selection") ||
                message.Contains("no connection could be made") ||
                message.Contains("actively refused"))
            {
                return true;
            }

            return false;
        }

        /// <summary>
        /// Gets a friendly name for the transient error type
        /// </summary>
        private static string GetTransientErrorType(Exception ex)
        {
            if (ex is TimeoutException)
                return "Timeout";
            if (ex is MongoConnectionException)
                return "Connection error";
            if (ex is MongoExecutionTimeoutException)
                return "Execution timeout";
            
            var message = ex.Message?.ToLower() ?? string.Empty;
            if (message.Contains("timeout") || message.Contains("timed out"))
                return "Timeout";
            if (message.Contains("connection") || message.Contains("socket"))
                return "Connection error";
            if (message.Contains("server selection"))
                return "Server selection timeout";
            
            return "Transient error";
        }

        #region Change Stream Processing Helpers

        /// <summary>
        /// Initializes aggressive change stream helper if needed
        /// </summary>
        private static AggressiveChangeStreamHelper? InitializeAggressiveHelper(
            bool isAggressive, 
            bool isAggressiveComplete, 
            MongoClient? targetClient, 
            Log log, 
            string jobId)
        {
            if (isAggressive && !isAggressiveComplete && targetClient != null)
            {
                return new AggressiveChangeStreamHelper(targetClient, log, jobId);
            }
            return null;
        }

        /// <summary>
        /// Stores document keys for aggressive change stream processing during bulk copy
        /// Returns (shouldSkipNormalProcessing, failureCount)
        /// </summary>
        private static async Task<(bool ShouldSkip, int FailureCount)> StoreDocumentKeysForAggressiveMode<TMigration>(
            bool isAggressive,
            bool isAggressiveComplete,
            AggressiveChangeStreamHelper? aggressiveHelper,
            bool isSimulatedRun,
            IEnumerable<BsonDocument> documentKeys,
            string databaseName,
            string collectionName,
            string operationType,
            TMigration mu,
            CounterDelegate<TMigration> incrementCounter,
            ChangeStreamOperationType csOperationType,
            Log log,
            string logPrefix)
        {
            if (!isAggressive || isAggressiveComplete || aggressiveHelper == null || isSimulatedRun)
            {
                return (false, 0);
            }

            if (documentKeys == null || !documentKeys.Any())
            {
                return (true, 0); // Still skip normal processing
            }

            try
            {
                int storedCount = await aggressiveHelper.StoreDocumentKeysAsync(databaseName, collectionName, documentKeys, operationType);
                incrementCounter(mu, CounterType.Processed, csOperationType, storedCount);
                return (true, 0); // Skip normal processing, no failures
            }
            catch (Exception ex)
            {
                log.WriteLine($"{logPrefix} Error storing {operationType} document keys for aggressive change stream: {ex.Message}", LogType.Error);
                return (true, 1); // Skip normal processing, 1 failure
            }
        }

        /// <summary>
        /// Executes bulk write operation with retry logic for transient errors
        /// </summary>
        private static async Task<(bool Success, long ProcessedCount)> ExecuteBulkWriteWithRetry<TModel>(
            IMongoCollection<BsonDocument> collection,
            List<TModel> models,
            string operationName,
            Log log,
            string logPrefix,
            bool isSimulatedRun,
            Func<BulkWriteResult<BsonDocument>, long> getCountFunc) where TModel : WriteModel<BsonDocument>
        {
            if (!models.Any())
            {
                return (true, 0);
            }

            int maxRetries = 10;
            int retryDelayMs = 500;
            long processedCount = 0;

            for (int attempt = 0; attempt <= maxRetries; attempt++)
            {
                try
                {
                    if (!isSimulatedRun)
                    {
                        var result = await collection.BulkWriteAsync(models, new BulkWriteOptions { IsOrdered = false });
                        processedCount = getCountFunc(result);
                    }
                    else
                    {
                        processedCount = models.Count;
                    }
                    return (true, processedCount);
                }
                catch (MongoBulkWriteException<BsonDocument>)
                {
                    // Let MongoBulkWriteException bubble up to caller for specialized handling
                    throw;
                }
                catch (MongoCommandException cmdEx) when (cmdEx.Message.Contains("Could not acquire lock") || cmdEx.Message.Contains("deadlock"))
                {
                    if (attempt < maxRetries)
                    {
                        int delay = retryDelayMs * (int)Math.Pow(2, attempt);
                        log.WriteLine($"{logPrefix}Deadlock detected for {collection.CollectionNamespace.FullName}. Retry {attempt + 1}/{maxRetries} after {delay}ms...", LogType.Warning);
                        await Task.Delay(delay);
                    }
                    else
                    {
                        log.WriteLine($"{logPrefix}Deadlock persisted after {maxRetries} retries for {collection.CollectionNamespace.FullName}. ", LogType.Error);
                        throw new InvalidOperationException(
                            $"CRITICAL: Unable to process {operationName} batch for {collection.CollectionNamespace.FullName} after {maxRetries} retry attempts due to persistent deadlock.");
                    }
                }
                catch (Exception ex) when (IsTransientException(ex))
                {
                    if (attempt < maxRetries)
                    {
                        int delay = retryDelayMs * (int)Math.Pow(2, attempt);
                        string errorType = GetTransientErrorType(ex);
                        log.WriteLine($"{logPrefix}{errorType} detected for {collection.CollectionNamespace.FullName}. Retry {attempt + 1}/{maxRetries} after {delay}ms... Error: {ex.Message}", LogType.Warning);
                        await Task.Delay(delay);
                    }
                    else
                    {
                        log.WriteLine($"{logPrefix}Transient error persisted after {maxRetries} retries for {collection.CollectionNamespace.FullName}.Batch size: {models.Count} documents.", LogType.Error);
                        throw new InvalidOperationException(
                            $"CRITICAL: Unable to process {operationName} batch for {collection.CollectionNamespace.FullName} after {maxRetries} retry attempts due to persistent errors. Error: {ex.Message}");
                    }
                }
            }

            return (false, 0);
        }

        /// <summary>
        /// Handles BulkWriteException with duplicate key errors
        /// </summary>
        private static (long ProcessedCount, int Failures) HandleBulkWriteException<TMigration>(
            MongoBulkWriteException<BsonDocument> ex,
            TMigration mu,
            CounterDelegate<TMigration> incrementCounter,
            ChangeStreamOperationType operationType,
            Log log,
            string logPrefix,
            string collectionFullName,
            Func<BulkWriteResult<BsonDocument>, long> getCountFunc)
        {
            long processedCount = getCountFunc(ex.Result);
            int failures = 0;

            var duplicateKeyErrors = ex.WriteErrors.Where(err => err.Code == 11000).ToList();
            incrementCounter(mu, CounterType.Skipped, operationType, duplicateKeyErrors.Count);

            var otherErrors = ex.WriteErrors.Where(err => err.Code != 11000).ToList();
            if (otherErrors.Any())
            {
                failures = otherErrors.Count;
                log.WriteLine($"{logPrefix} {operationType} BulkWriteException (non-duplicate errors) in {collectionFullName}: {string.Join(", ", otherErrors.Select(e => e.Message))}");
            }
            else if (duplicateKeyErrors.Count > 0)
            {
                log.ShowInMonitor($"{logPrefix} Skipped {duplicateKeyErrors.Count} duplicate key {operationType}s in {collectionFullName}");
            }

            return (processedCount, failures);
        }

        /// <summary>
        /// Executes bulk write operation for updates with retry logic, returning both modified and upsert counts
        /// </summary>
        private static async Task<(bool Success, long ModifiedCount, long UpsertCount)> ExecuteBulkWriteWithRetryForUpdates(
            IMongoCollection<BsonDocument> collection,
            List<ReplaceOneModel<BsonDocument>> models,
            Log log,
            string logPrefix,
            bool isSimulatedRun)
        {
            if (!models.Any())
            {
                return (true, 0, 0);
            }

            int maxRetries = 10;
            int retryDelayMs = 500;

            for (int attempt = 0; attempt <= maxRetries; attempt++)
            {
                try
                {
                    if (!isSimulatedRun)
                    {
                        var result = await collection.BulkWriteAsync(models, new BulkWriteOptions { IsOrdered = false });
                        return (true, result.ModifiedCount, result.Upserts.Count);
                    }
                    else
                    {
                        return (true, models.Count, 0);
                    }
                }
                catch (MongoBulkWriteException<BsonDocument>)
                {
                    // Let it bubble up to caller for specialized handling
                    throw;
                }
                catch (MongoCommandException cmdEx) when (cmdEx.Message.Contains("Could not acquire lock") || cmdEx.Message.Contains("deadlock"))
                {
                    if (attempt < maxRetries)
                    {
                        int delay = retryDelayMs * (int)Math.Pow(2, attempt);
                        log.WriteLine($"{logPrefix}Deadlock detected for {collection.CollectionNamespace.FullName}. Retry {attempt + 1}/{maxRetries} after {delay}ms...", LogType.Warning);
                        await Task.Delay(delay);
                    }
                    else
                    {
                        log.WriteLine($"{logPrefix}Deadlock persisted after {maxRetries} retries for {collection.CollectionNamespace.FullName}. Batch size: {models.Count} documents. ", LogType.Error);
                        throw new InvalidOperationException(
                            $"CRITICAL: Unable to process update batch for {collection.CollectionNamespace.FullName} after {maxRetries} retry attempts due to persistent deadlock.");
                    }
                }
                catch (Exception ex) when (IsTransientException(ex))
                {
                    if (attempt < maxRetries)
                    {
                        int delay = retryDelayMs * (int)Math.Pow(2, attempt);
                        string errorType = GetTransientErrorType(ex);
                        log.WriteLine($"{logPrefix}{errorType} detected for {collection.CollectionNamespace.FullName}. Retry {attempt + 1}/{maxRetries} after {delay}ms... Error: {ex.Message}", LogType.Warning);
                        await Task.Delay(delay);
                    }
                    else
                    {
                        log.WriteLine($"{logPrefix}Transient error persisted after {maxRetries} retries for {collection.CollectionNamespace.FullName}. Batch size: {models.Count} documents. ", LogType.Error);
                        throw new InvalidOperationException(
                            $"CRITICAL: Unable to process update batch for {collection.CollectionNamespace.FullName} after {maxRetries} retry attempts due to persistent errors.");
                    }
                }
            }

            return (false, 0, 0);
        }

        /// <summary>
        /// Handles BulkWriteException for updates with special duplicate key retry logic
        /// </summary>
        private static async Task<(long UpdateCount, long UpsertCount, int Failures)> HandleUpdateBulkWriteException<TMigration>(
            MongoBulkWriteException<BsonDocument> ex,
            IMongoCollection<BsonDocument> collection,
            List<ReplaceOneModel<BsonDocument>> updateModels,
            TMigration mu,
            CounterDelegate<TMigration> incrementCounter,
            Log log,
            string logPrefix)
        {
            long updateCount = ex.Result?.ModifiedCount ?? 0;
            long upsertCount = ex.Result?.Upserts?.Count ?? 0;
            int failures = 0;

            var duplicateKeyErrors = ex.WriteErrors
                .Where(err => err.Code == 11000)
                .ToList();

            incrementCounter(mu, CounterType.Skipped, ChangeStreamOperationType.Update, duplicateKeyErrors.Count);

            // Log non-duplicate key errors
            var otherErrors = ex.WriteErrors
                .Where(err => err.Code != 11000)
                .ToList();

            failures += otherErrors.Count;

            if (otherErrors.Any())
            {
                log.WriteLine($"{logPrefix} Update BulkWriteException (non-duplicate errors) in {collection.CollectionNamespace.FullName}: {string.Join(", ", otherErrors.Select(e => e.Message))}");
            }

            // Handle duplicate key errors by attempting individual operations
            if (duplicateKeyErrors.Any())
            {
                log.ShowInMonitor($"{logPrefix} Handling {duplicateKeyErrors.Count} duplicate key errors for updates in {collection.CollectionNamespace.FullName}");

                // Process failed operations individually
                foreach (var error in duplicateKeyErrors)
                {
                    try
                    {
                        // Try to find the corresponding update model and retry as update without upsert
                        var failedModel = updateModels[error.Index];
                        if (failedModel is ReplaceOneModel<BsonDocument> replaceModel)
                        {
                            // Try update without upsert first
                            var updateResult = await collection.ReplaceOneAsync(replaceModel.Filter, replaceModel.Replacement, new ReplaceOptions { IsUpsert = false });
                            if (updateResult.ModifiedCount > 0)
                            {
                                updateCount++;
                            }
                            else
                            {
                                // Document might not exist, but we can't upsert due to unique constraint
                                // This is expected in some scenarios - count as skipped
                                incrementCounter(mu, CounterType.Skipped, ChangeStreamOperationType.Update, 1);
                            }
                        }
                    }
                    catch (Exception retryEx)
                    {
                        failures++;
                        log.ShowInMonitor($"{logPrefix} Individual retry failed for update in {collection.CollectionNamespace.FullName}: {retryEx.Message}");
                    }
                }
            }

            return (updateCount, upsertCount, failures);
        }

        #endregion


        public static BsonValue GetIdRangeMin(BsonDocument? filter)
        {
            if (filter == null)
                return BsonMinKey.Value;

            // Direct _id clause
            if (filter.Contains("_id") && filter["_id"].IsBsonDocument)
            {
                var idDoc = filter["_id"].AsBsonDocument;
                if (idDoc.TryGetValue("$gte", out var gte))
                    return gte;
                if (idDoc.TryGetValue("$gt", out var gt))
                    return gt;
            }

            // Handle logical operators recursively
            foreach (var key in new[] { "$and", "$or" })
            {
                if (filter.Contains(key) && filter[key].IsBsonArray)
                {
                    var clauses = filter[key].AsBsonArray;

                    var mins = clauses
                        .Where(c => c.IsBsonDocument)
                        .Select(c => GetIdRangeMin(c.AsBsonDocument))
                        .Where(v => v != BsonMinKey.Value)
                        .ToList();

                    if (mins.Count > 0)
                        return mins.OrderBy(v => v, new BsonValueComparerSimple()).First();
                }
            }

            return BsonMinKey.Value;
        }

        public static BsonValue GetIdRangeMax(BsonDocument? filter)
        {
            if (filter == null)
                return BsonMaxKey.Value;

            // Direct _id clause
            if (filter.Contains("_id") && filter["_id"].IsBsonDocument)
            {
                var idDoc = filter["_id"].AsBsonDocument;
                if (idDoc.TryGetValue("$lte", out var lte))
                    return lte;
                if (idDoc.TryGetValue("$lt", out var lt))
                    return lt;
            }

            // Handle logical operators recursively
            foreach (var key in new[] { "$and", "$or" })
            {
                if (filter.Contains(key) && filter[key].IsBsonArray)
                {
                    var clauses = filter[key].AsBsonArray;

                    var maxes = clauses
                        .Where(c => c.IsBsonDocument)
                        .Select(c => GetIdRangeMax(c.AsBsonDocument))
                        .Where(v => v != BsonMaxKey.Value)
                        .ToList();

                    if (maxes.Count > 0)
                        return maxes.OrderByDescending(v => v, new BsonValueComparerSimple()).First();
                }
            }

            return BsonMaxKey.Value;
        }
        private class BsonValueComparerSimple : IComparer<BsonValue>
        {
            public int Compare(BsonValue? x, BsonValue? y)
            {
                if (x == null && y == null) return 0;
                if (x == null) return -1;
                if (y == null) return 1;
                return x.CompareTo(y);
            }
        }
        public static long GetActualDocumentCount(IMongoCollection<BsonDocument> collection, MigrationUnit mu )
        {
            FilterDefinition<BsonDocument>? userFilter = GetFilterDoc(mu.UserFilter);
            var filter = userFilter ?? Builders<BsonDocument>.Filter.Empty;
            return collection.CountDocuments(filter);
        }

		/// <summary>
		/// Helper method to list all databases from MongoDB connection
		/// </summary>
		/// <param name="connectionString">MongoDB connection string</param>
		/// <returns>List of database names, excluding system databases</returns>
		public static async Task<List<string>> ListDatabasesAsync(string connectionString)
		{
			var databases = new List<string>();
			try
			{
				var client = new MongoClient(connectionString);
				var databasesCursor = await client.ListDatabasesAsync();
				var databasesDocument = await databasesCursor.ToListAsync();

				foreach (var db in databasesDocument)
				{
					var dbName = db["name"].AsString;
					// Skip system databases
					if (!IsSystemDatabase(dbName))
					{
						databases.Add(dbName);
					}
				}
			}
			catch (Exception)
			{
				// Return empty list if connection fails
			}
			return databases;
		}

		/// <summary>
		/// Helper method to list all collections from a specific database
		/// </summary>
		/// <param name="connectionString">MongoDB connection string</param>
		/// <param name="databaseName">Database name</param>
		/// <returns>List of collection names, excluding system collections, views, and other non-collection types</returns>
		public static async Task<List<string>> ListCollectionsAsync(string connectionString, string databaseName)
		{
			var collections = new List<string>();
			try
			{
				var client = new MongoClient(connectionString);
				var database = client.GetDatabase(databaseName);
				
				// Get all collections (system collections will be filtered below)
				var collectionsCursor = await database.ListCollectionsAsync();
				var allCollections = await collectionsCursor.ToListAsync();

				foreach (var collectionInfo in allCollections)
				{
					var collectionName = collectionInfo["name"].AsString;
					
					// Skip system collections (additional check)
					if (IsSystemCollection(collectionName))
						continue;
					
					// Check if it's a collection (not a view or other type)
					var type = collectionInfo.GetValue("type", "collection").AsString;
					if (type == "collection")
					{
						collections.Add(collectionName);
					}
				}
			}
			catch (Exception)
			{
				// Return empty list if connection fails
			}
			return collections;
		}

		/// <summary>
		/// Check if database name is a system database
		/// </summary>
		private static bool IsSystemDatabase(string databaseName)
		{
			var systemDatabases = new[] { "admin", "local", "config" };
			return systemDatabases.Contains(databaseName, StringComparer.OrdinalIgnoreCase);
		}

		/// <summary>
		/// Check if collection name is a system collection
		/// </summary>
		private static bool IsSystemCollection(string collectionName)
		{
			return collectionName.StartsWith("system.", StringComparison.OrdinalIgnoreCase);
		}
		public static (long Lsn, string Rid, string Min, string Max) ExtractValuesFromResumeToken(BsonDocument bsonDoc)
        {
            if (bsonDoc == null || !bsonDoc.Contains("_data"))
                throw new ArgumentException("Invalid BSON document or missing _data field", nameof(bsonDoc));

            // Step 1: Get the Base64 string from the BSON binary field
            string base64Str = Convert.ToBase64String(bsonDoc["_data"].AsBsonBinaryData.Bytes);

            // Step 2: Base64 decode into ASCII JSON string
            byte[] bytes = Convert.FromBase64String(base64Str);
            string asciiJson = Encoding.ASCII.GetString(bytes);

            // Step 3: Parse the decoded JSON
            using JsonDocument jsonDoc = JsonDocument.Parse(asciiJson);
            var root = jsonDoc.RootElement;

            // Step 4: Extract LSN
            string? rawValue = root
                .GetProperty("Continuation")[0]
                .GetProperty("State")
                .GetProperty("value")
                .GetString();

            if (rawValue == null)
                throw new InvalidOperationException("Resume token LSN value is missing or null.");

            rawValue = rawValue.Trim('"');
            long lsn = long.Parse(rawValue);

            // Step 5: Extract Rid, Min, and Max
            string rid = root.GetProperty("Rid").GetString() ?? string.Empty;

            string min = root
                .GetProperty("Continuation")[0]
                .GetProperty("FeedRange")
                .GetProperty("value")
                .GetProperty("min")
                .GetString() ?? string.Empty;

            string max = root
                .GetProperty("Continuation")[0]
                .GetProperty("FeedRange")
                .GetProperty("value")
                .GetProperty("max")
                .GetString() ?? string.Empty;

            return (lsn, rid, min, max);
        }


        public static FilterDefinition<BsonDocument> GenerateQueryFilter(
             BsonValue? gte,
             BsonValue? lte,
             DataType dataType,
             BsonDocument userFilterDoc,
             bool skipDataTypeFilter = false)
        {

            var userFilter = new BsonDocumentFilterDefinition<BsonDocument>(userFilterDoc);

            var filterBuilder = Builders<BsonDocument>.Filter;

            FilterDefinition<BsonDocument> typeFilter;

            if (dataType == DataType.Other)
                skipDataTypeFilter = true;

            if (skipDataTypeFilter)
            {
                // Skip DataType filtering - use empty filter for type
                typeFilter = FilterDefinition<BsonDocument>.Empty;
            }
            else
            {
                // Original DataType filtering logic
                typeFilter = filterBuilder.Eq("_id", new BsonDocument("$type", DataTypeToBsonType(dataType)));
            }

            bool hasGte = gte != null && !gte.IsBsonNull && !(gte is BsonMaxKey);
            bool hasLte = lte != null && !lte.IsBsonNull && !(lte is BsonMaxKey);

            FilterDefinition<BsonDocument> idFilter;

            if (hasGte && hasLte)
            {
                if (skipDataTypeFilter)
                {
                    idFilter = filterBuilder.And(
                        BuildFilterGte("_id", gte!, dataType),
                        BuildFilterLt("_id", lte!, dataType)
                    );
                }
                else
                {
                    idFilter = filterBuilder.And(
                        typeFilter,
                        BuildFilterGte("_id", gte!, dataType),
                        BuildFilterLt("_id", lte!, dataType)
                    );
                }
            }
            else if (hasGte)
            {
                if (skipDataTypeFilter)
                {
                    idFilter = BuildFilterGte("_id", gte!, dataType);
                }
                else
                {
                    idFilter = filterBuilder.And(
                        typeFilter,
                        BuildFilterGte("_id", gte!, dataType)
                    );
                }
            }
            else if (hasLte)
            {
                if (skipDataTypeFilter)
                {
                    idFilter = BuildFilterLt("_id", lte!, dataType);
                }
                else
                {
                    idFilter = filterBuilder.And(
                        typeFilter,
                        BuildFilterLt("_id", lte!, dataType)
                    );
                }
            }
            else
            {
                idFilter = typeFilter;
            }

            if (userFilter != null && userFilter.Document.ElementCount>0)
            {
                // Combine userFilter with idFilter using AND
                //if (!MongoHelper.UsesIdFieldInFilter(userFilterDoc)) // if user filter does not use _id, we can combine at root                {
                //{
                    // Combine userFilter with idFilter using AND
                    return filterBuilder.And(userFilter, idFilter);
                //}
            }

            return idFilter;
        }


        public static long GetDocumentCount(IMongoCollection<BsonDocument> collection, BsonValue? gte, BsonValue? lte, DataType dataType, BsonDocument userFilterDoc, bool skipDataTypeFilter = false)
        {
            FilterDefinition<BsonDocument> filter = GenerateQueryFilter(gte, lte, dataType,userFilterDoc, skipDataTypeFilter);
                        
            // Execute the query and return the count with 10 minute timeout
            return collection.CountDocuments(filter, new CountOptions { MaxTime = TimeSpan.FromMinutes(10) });           
        }

        public static long GetDocumentCount(
           IMongoCollection<BsonDocument> collection,
           FilterDefinition<BsonDocument>? filter,
           BsonDocument? userFilterDoc)
        {

            // Use empty filter if null
            filter ??= Builders<BsonDocument>.Filter.Empty;

            FilterDefinition<BsonDocument> combinedFilter = filter;

            // Only combine if userFilterDoc is non-null and has elements
            if (userFilterDoc != null && userFilterDoc.ElementCount > 0)
            {
                try
                {
                    var userFilter = new BsonDocumentFilterDefinition<BsonDocument>(userFilterDoc);

                    // Safely combine filters
                    combinedFilter = Builders<BsonDocument>.Filter.And(filter, userFilter);
                }
                catch (Exception ex)
                {
                    combinedFilter = filter; // fallback to base filter
                }
            }

            return collection.CountDocuments(
                combinedFilter,
                new CountOptions { MaxTime = TimeSpan.FromMinutes(120) }
            );
            
        }

        public static async Task<(bool IsCSEnabled, string Version)> IsChangeStreamEnabledAsync(Log log,string PEMFileContents,string connectionString, MigrationUnit mu, bool createCollection=false)
        {
            string version = string.Empty;
            string collectionName = string.Empty;
            string databaseName = string.Empty;
            MongoClient? client = null;
            try
            {
                //// Connect to the MongoDB server
                client = MongoClientFactory.Create(log,connectionString,true, PEMFileContents);

                
                if (createCollection)
                {
                    databaseName = Guid.NewGuid().ToString();
                    collectionName = "test";

                    var database = client.GetDatabase(databaseName);
                    var collection = database.GetCollection<BsonDocument>(collectionName);

                    // Insert a dummy document
                    var dummyDoc = new BsonDocument
                    {
                        { "name", "dummy" },
                        { "timestamp", DateTime.UtcNow }
                    };

                    await collection.InsertOneAsync(dummyDoc);
                }
                else
                {
                    databaseName = mu.DatabaseName;
                    collectionName = mu.CollectionName;
                }


                if(Helper.IsRU(connectionString)) //for MongoDB API
                {
                    var database = client.GetDatabase(databaseName);
                    var collection = database.GetCollection<BsonDocument>(collectionName);
                    var options = new ChangeStreamOptions
                    {
                        FullDocument = ChangeStreamFullDocumentOption.UpdateLookup
                    };
                    var pipeline = new BsonDocument[]
                    {
                        new BsonDocument("$match", new BsonDocument("operationType",
                            new BsonDocument("$in", new BsonArray { "insert", "update", "replace", "delete" }))),
                        new BsonDocument("$project", new BsonDocument
                        {
                            { "operationType", 1 },  // ✅ include this
                            { "_id", 1 },
                            { "fullDocument", 1 },
                            { "ns", 1 },
                            { "documentKey", 1 }
                        })
                    };

                    using var cursor = collection.Watch<ChangeStreamDocument<BsonDocument>>(pipeline, options);
                    return (IsCSEnabled: true, Version: "");
                }

                if (connectionString.Contains("mongocluster.cosmos.azure.com")) //for vcore
                {
                    var database = client.GetDatabase(databaseName);
                    var collection = database.GetCollection<BsonDocument>(collectionName);

                    var options = new ChangeStreamOptions
                    {
                        FullDocument = ChangeStreamFullDocumentOption.UpdateLookup
                    };
                    using var cursor = await collection.WatchAsync(options);

                    return (IsCSEnabled: true, Version: "");
                }
                else
                {


                    // Check the server status to verify replica set or sharded cluster
                    var adminDatabase = client.GetDatabase("admin");
                    var masterCommand = new BsonDocument("isMaster", 1);
                    var isMasterResult = await adminDatabase.RunCommandAsync<BsonDocument>(masterCommand);

                    // Get Mongo Version
                    var verCommand = new BsonDocument("buildInfo", 1);
                    var result = await adminDatabase.RunCommandAsync<BsonDocument>(verCommand);

                    version = result["version"].AsString;

                    // Check if the server is part of a replica set or a sharded cluster
                    if (isMasterResult.Contains("setName") || isMasterResult.GetValue("msg", "").AsString == "isdbgrid")
                    {
						log.WriteLine("Change streams are enabled on source (replica set or sharded cluster).");
                        
                        return (IsCSEnabled: true, Version: version);
                    }
                    else
                    {
						log.WriteLine("Change streams are not enabled on source (standalone server).", LogType.Error);
                        
                        return (IsCSEnabled: false, Version: version);
                    }
                }
            }
            catch (MongoCommandException ex) when (ex.Message.Contains("$changeStream is not supported"))
            {
				log.WriteLine("Change streams are not enabled on vCore.", LogType.Error);
                
                return (IsCSEnabled: false, Version: "");

            }
            catch (MongoCommandException ex) when (ex.Message.Contains("Match stage must include constraints on"))
            {
                log.WriteLine("Online migration capability is not enabled on source RU account. Please contact cdbmigrationsupport@microsoft.com", LogType.Error);

                return (IsCSEnabled: false, Version: "");

            }
            catch (Exception ex)
            {
				log.WriteLine($"Error checking for change streams: {ex}", LogType.Error);
                
                //return (IsCSEnabled: false, Version: version);
                throw;
            }
            finally
            {
                if (createCollection && client != null)
                {
                    await client.DropDatabaseAsync(databaseName); //drop the dummy database created to test CS
                }
            }
        }

    public async static Task SetChangeStreamResumeTokenAsync(Log log, MongoClient client, MigrationJob job, MigrationUnit mu, int seconds, CancellationToken cts, bool fromChangeStreamStartedOn )
    {
        int retryCount = 0;
        bool isSucessful = false;
        bool resetCS = mu.ResetChangeStream;
        
        // Determine if we should use server-level or collection-level processing
        bool useServerLevel = job.ChangeStreamLevel == ChangeStreamLevel.Server;

        while (!isSucessful && retryCount < 10)
        {
                try
                {
                    BsonDocument resumeToken = new BsonDocument();
                    var database = client.GetDatabase(mu.DatabaseName);
                    var collection = database.GetCollection<BsonDocument>(mu.CollectionName);

                    // Initialize with safe defaults; will be overridden below
                    var options = new ChangeStreamOptions { BatchSize = 500, FullDocument = ChangeStreamFullDocumentOption.UpdateLookup };

                    if(fromChangeStreamStartedOn)
                    {
                        var start = (mu.ChangeStreamStartedOn ?? DateTime.UtcNow).ToUniversalTime();
                        log.WriteLine($"Resetting change stream start time token for {mu.DatabaseName}.{mu.CollectionName} to {start} (UTC)");
                        var bsonTimestamp = ConvertToBsonTimestamp(start);
                        options = new ChangeStreamOptions { BatchSize = 500, FullDocument = ChangeStreamFullDocumentOption.UpdateLookup, StartAtOperationTime = bsonTimestamp };
                    }

                    if (resetCS)
                    {
                        if (!string.IsNullOrEmpty(mu.OriginalResumeToken))
                        {
                            var startedOnUtc = mu.ChangeStreamStartedOn.HasValue ? mu.ChangeStreamStartedOn.Value.ToUniversalTime() : DateTime.UtcNow;
                            log.WriteLine($"Resetting change stream resume token for {mu.DatabaseName}.{mu.CollectionName} to {startedOnUtc} (UTC)");
                            options = new ChangeStreamOptions { BatchSize = 500, FullDocument = ChangeStreamFullDocumentOption.UpdateLookup, ResumeAfter = BsonDocument.Parse(mu.OriginalResumeToken) };
                        }
                        else
                        {
                            //try to go 15 min back in time, temporary fix for backward compatibility
                            var start = (mu.ChangeStreamStartedOn ?? DateTime.UtcNow).AddMinutes(-15).ToUniversalTime();
                            log.WriteLine($"Resetting change stream start time token for {mu.DatabaseName}.{mu.CollectionName} to {start} (UTC)");
                            var bsonTimestamp = ConvertToBsonTimestamp(start);
                            options = new ChangeStreamOptions { BatchSize = 500, FullDocument = ChangeStreamFullDocumentOption.UpdateLookup, StartAtOperationTime = bsonTimestamp };
                        }
                    }
                    else
                    {
                        // Check if resume token already exists based on processing level
                        string existingResumeToken = string.Empty;

                        if (useServerLevel)
                        {
                            // For server-level, check global resume token in MigrationJob
                            existingResumeToken = job.ResumeToken ?? string.Empty;
                            if (!string.IsNullOrEmpty(existingResumeToken))
                            {
                                log.WriteLine($"Server-level change stream resume token already set for job {job.Id}", LogType.Debug);
                                return;
                            }
                        }
                        else
                        {
                            // For collection-level, check collection-specific resume token
                            existingResumeToken = mu.ResumeToken ?? string.Empty;
                            if (!string.IsNullOrEmpty(existingResumeToken))
                            {
                                log.WriteLine($"Collection-level change stream resume token for {mu.DatabaseName}.{mu.CollectionName} already set", LogType.Debug);
                                return;
                            }
                        }

                        options = new ChangeStreamOptions
                        {
                            FullDocument = ChangeStreamFullDocumentOption.UpdateLookup
                        };
                    }

                    //new way to get resume token
                    //On MongoDB 4.0+, the WatchChangeStreamAsync method opens a change stream and waits for changes.
                    //On MongoDB 3.6, TailOplogAsync opens a tailable cursor on the oplog, filtering on namespace and timestamp to detect new operations.
                    if (!string.IsNullOrEmpty(job.SourceServerVersion) && job.SourceServerVersion.StartsWith("3"))
                    {
                        if (!await TailOplogAsync(client, mu.DatabaseName, mu.CollectionName, mu, cts) && !resetCS)
                        {
                            //if failed to tail oplog, fallback to watching change stream infinelty. Should be called async only
                            await WatchChangeStreamUntilChangeAsync(log, client, job, mu, collection, options, resetCS, -1, cts, useServerLevel);
                        }
                    }
                    else
                        await WatchChangeStreamUntilChangeAsync(log, client, job, mu, collection, options, resetCS, seconds, cts, useServerLevel);
                    //end of new way to get resume token

                    isSucessful = true;
                }
                catch (OperationCanceledException)
                {
                    // Distinguish between timeout (no activity) vs manual cancellation
                    if (resetCS && mu.ResetChangeStream)
                    {
                        mu.ResetChangeStream = false;

                        // Check if it was a timeout or manual cancellation
                        if (cts.IsCancellationRequested)
                        {
                            log.WriteLine($"Change stream reset cancelled for {mu.DatabaseName}.{mu.CollectionName} (system shutdown or manual cancellation).", LogType.Warning);
                            isSucessful = true; // Exit retry loop - cancellation is final
                        }
                        else
                        {
                            // Timeout occurred - no changes detected within the time window
                            // Use OriginalResumeToken to reset ResumeToken back to the original position
                            if (!string.IsNullOrEmpty(mu.OriginalResumeToken))
                            {
                                mu.ResumeToken = mu.OriginalResumeToken;
                                log.WriteLine($"No changes detected in {mu.DatabaseName}.{mu.CollectionName} during {seconds}s timeout window. ResumeToken reset to OriginalResumeToken.", LogType.Info);
                            }
                            else
                            {
                                log.WriteLine($"No changes detected in {mu.DatabaseName}.{mu.CollectionName} during {seconds}s timeout window. No OriginalResumeToken available - will start fresh on next change.", LogType.Info);
                            }
                            isSucessful = true; // Exit retry loop - timeout is expected and handled
                        }
                    }
                    else
                    {
                        // For non-reset cancellations, just exit the retry loop
                        isSucessful = true;
                    }
                }
                catch (Exception ex) when (ex is MongoExecutionTimeoutException || ex is TimeoutException)
                {
                    log.WriteLine($"Timeout when setting change stream resume token for {mu.DatabaseName}.{mu.CollectionName}: {ex}", LogType.Debug);
                }
                catch (Exception ex)
                {
                    retryCount++;

                    log.WriteLine($"Attempt {retryCount}. Error setting change stream resume token for {mu.DatabaseName}.{mu.CollectionName}: {ex}", LogType.Error);

                    // If all retries exhausted, clear the flag to prevent infinite retry loop
                    if (retryCount >= 10 && mu.ResetChangeStream)
                    {
                        mu.ResetChangeStream = false;
                        log.WriteLine($"Failed to reset change stream for {mu.DatabaseName}.{mu.CollectionName} after {retryCount} attempts. Flag cleared to prevent infinite retries. Manual intervention may be required.", LogType.Error);
                    }
                }
                finally
                {
                    if (useServerLevel)
                        log.WriteLine($"Exiting Server-level SetChangeStreamResumeTokenAsync for job {job.Id}", LogType.Verbose);
                    else
                        log.WriteLine($"Exiting Collection-level SetChangeStreamResumeToken for {mu.DatabaseName}.{mu.CollectionName} - ResumeToken: {(!string.IsNullOrEmpty(mu.ResumeToken) ? "SET" : "NOT SET")}, InitialDocReplayed: {mu.InitialDocumenReplayed}", LogType.Verbose);

                    MigrationJobContext.SaveMigrationUnit(mu, false);
                }
        }
        return;
    }

    private static async Task WatchChangeStreamUntilChangeAsync(Log log, MongoClient client, MigrationJob job, MigrationUnit mu, IMongoCollection<BsonDocument> collection, ChangeStreamOptions options, bool resetCS, int seconds, CancellationToken manualCts, bool useServerLevel = false)
    {
        var pipeline = new BsonDocument[] { };
        if (job.JobType == JobType.RUOptimizedCopy)
        {
            pipeline = new BsonDocument[]
                {
                new BsonDocument("$match", new BsonDocument("operationType",
                    new BsonDocument("$in", new BsonArray { "insert", "update", "replace","delete" }))
                ),
                new BsonDocument("$project", new BsonDocument
                {
                    { "_id", 1 },
                    { "fullDocument", 1 },
                    { "ns", 1 },
                    { "documentKey", 1 }
                })
                };
        }

        // Set MaxAwaitTime to control how long each MoveNextAsync waits for changes
        // This allows multiple polling attempts within the overall timeout
        if (options.MaxAwaitTime == null)
        {
            options.MaxAwaitTime = TimeSpan.FromMilliseconds(500); // Poll every 500ms
        }

        CancellationTokenSource cts;
        if (seconds > 0)
            cts = new CancellationTokenSource(TimeSpan.FromSeconds(seconds));
        else
            cts = new CancellationTokenSource();

        CancellationTokenSource linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cts.Token, manualCts);

        // Choose between server-level or collection-level change stream
        IChangeStreamCursor<ChangeStreamDocument<BsonDocument>> cursor;
        
        if (useServerLevel)
        {
            // Server-level change stream
            log.WriteLine($"Setting up server-level change stream resume token for job {job.Id}");
            cursor = await client.WatchAsync<ChangeStreamDocument<BsonDocument>>(pipeline, options, linkedCts.Token);
        }
        else
        {
            // Collection-level change stream
            log.WriteLine($"Setting up collection-level change stream resume token for {mu.DatabaseName}.{mu.CollectionName}",LogType.Debug);
            cursor = await collection.WatchAsync<ChangeStreamDocument<BsonDocument>>(pipeline, options, linkedCts.Token);
        }


        using (cursor)
        {
                try
                {

                    if (job.JobType == JobType.RUOptimizedCopy)
                    {
                        // Use linkedCts.Token instead of cts.Token to respect both timeout and manual cancellation
                        if (await cursor.MoveNextAsync(linkedCts.Token))
                        {
                            if (!resetCS && (useServerLevel ? string.IsNullOrEmpty(job.OriginalResumeToken) : string.IsNullOrEmpty(mu.OriginalResumeToken)))
                            {
                                var resumeTokenJson = cursor.GetResumeToken().ToJson();

                                mu.ResumeToken = resumeTokenJson;
                                mu.OriginalResumeToken = resumeTokenJson;

                            }
                            return;
                        }
                        return;
                    }

                    // Iterate until cancellation or first change detected
                    // Use linkedCts to respect both timeout and manual cancellation
                    while (!linkedCts.Token.IsCancellationRequested)
                    {
                        var hasNext = await cursor.MoveNextAsync(linkedCts.Token);
                        if (!hasNext)
                        {
                            break; // Stream closed or no more data
                        }

                        foreach (var change in cursor.Current)
                        {
                            //if bulk load is complete, no point in continuing to watch
                            // Use the local resetCS variable captured at method start, not mu.ResetChangeStream
                            if ((mu.RestoreComplete || job.IsSimulatedRun) && mu.DumpComplete && !resetCS)
                                return;


                            // Handle server-level vs collection-level resume token storage
                            if (useServerLevel)
                            {
                                var databaseName = change.CollectionNamespace.DatabaseNamespace.DatabaseName;
                                var collectionName = change.CollectionNamespace.CollectionName;
                                var collectionKey = $"{databaseName}.{collectionName}";

                                //checking if change is in collections to be migrated.
                                var migrationUnit = Helper.GetMigrationUnitsToMigrate(job).FirstOrDefault(mu =>
                                    string.Equals(mu.DatabaseName, databaseName, StringComparison.OrdinalIgnoreCase) &&
                                    string.Equals(mu.CollectionName, collectionName, StringComparison.OrdinalIgnoreCase));

                                if (migrationUnit != null && Helper.IsMigrationUnitValid(migrationUnit))
                                {
                                    // Use common function for server-level resume token setting
                                    SetResumeTokenProperties(job, change, resetCS, databaseName, collectionName);
                                    log.WriteLine($"Server-level resume token set for job {job.Id} with collection key {job.ResumeCollectionKey}");
                                    // Exit immediately after first change detected
                                    return;
                                }
                            }
                            else
                            {
                                // Use common function for collection-level resume token setting
                                SetResumeTokenProperties(mu, change, resetCS);

                                log.WriteLine($"Collection-level resume token set for {mu.DatabaseName}.{mu.CollectionName} - Operation: {mu.ResumeTokenOperation}, DocumentId: {mu.ResumeDocumentId}", LogType.Verbose);

                                // Exit immediately after first change detected
                                return;
                            }

                        }
                    }

                }
                catch (Exception ex) when (ex is TimeoutException)
                {
                    log.WriteLine($"Timeout while watching change stream for {mu.DatabaseName}.{mu.CollectionName}: {ex}", LogType.Debug);
                }
                catch (Exception ex) when (ex is OperationCanceledException)
                {
                    // Cancellation requested - exit quietly
                }
        }
    }

        /// Manually tails the oplog.rs capped collection for MongoDB 3.6 support.
        /// </summary>
    private static async Task<bool> TailOplogAsync(MongoClient client, string dbName, string collectionName,MigrationUnit mu, CancellationToken cancellationToken)
        {
            try
            {

                var localDb = client.GetDatabase("local");
                var oplog = localDb.GetCollection<BsonDocument>("oplog.rs");

                // The namespace string for filtering is "db.collection"
                string ns = $"{dbName}.{collectionName}";

                // Construct filter: ts > last timestamp or start from now
                var tsFilter = mu.ChangeStreamStartedOn.HasValue
                    ? ConvertToBsonTimestamp(mu.ChangeStreamStartedOn.Value)
                    : ConvertToBsonTimestamp(DateTime.UtcNow.AddSeconds(-1));

                var filterBuilder = Builders<BsonDocument>.Filter;
                var filter = filterBuilder.Gt("ts", tsFilter) & filterBuilder.Eq("ns", ns);

                // Options for tailable cursor
                var options = new FindOptions<BsonDocument>
                {
                    CursorType = CursorType.TailableAwait,
                    NoCursorTimeout = true,
                    BatchSize = 500
                };

                using (var cursor = await oplog.FindAsync(filter, options, cancellationToken))
                {
                    while (!cancellationToken.IsCancellationRequested)
                    {
                        while (await cursor.MoveNextAsync(cancellationToken))
                        {
                            foreach (var doc in cursor.Current)
                            {
                                // Parse oplog document for resume info
                                var ts = doc["ts"].AsBsonTimestamp;
                                var nsValue = doc["ns"].AsString;

                                if (!string.Equals(ns, nsValue, StringComparison.Ordinal))
                                    continue; // Filter different ns, just in case

                                // Update mu fields
                                mu.ChangeStreamStartedOn = BsonTimestampToUtcDateTime(ts);
                                // Oplog does not have a resume token; use ts as marker.

                                // If you have document key or op, you can extract here

                                return true; // Exit on first oplog mu detected
                            }
                        }

                        // If no data yet, wait a bit before retrying
                        await Task.Delay(1000, cancellationToken);
                    }
                }
                return true; // Successfully tailing oplog
            }
            catch {
            return false; // Return false if any error occurs
            }
        }

        public static async Task<(bool Exits,bool IsCollection)> CheckIsCollectionAsync(MongoClient client, string databaseName, string collectionName)
        {

            var database = client.GetDatabase(databaseName);

            // Filter by collection name
            var filter = new BsonDocument("name", collectionName);

            using var cursor = await database.ListCollectionsAsync(new ListCollectionsOptions { Filter = filter });
            var collectionInfo = await cursor.FirstOrDefaultAsync();

            if (collectionInfo == null)
            {
                return new(false, false);
            }

            // Check the "type" field returned in listCollections
            var type = collectionInfo.GetValue("type", "collection").AsString;
            return new(true, type == "collection");

        }

        public static async Task<bool> CheckRUCollectionExistsAsync(MongoClient client, string databaseName, string collectionName)
        {
            var db = client.GetDatabase(databaseName);
            var coll = db.GetCollection<RawBsonDocument>(collectionName);
            // Check if collection has at least one document or any indexes
            var hasData = await coll.Find(FilterDefinition<RawBsonDocument>.Empty)
                                    .Limit(1)
                                    .AnyAsync();
            if (hasData)
                return true;

            // If no data, check if any indexes exist (other than default)
            var indexList = await coll.Indexes.ListAsync();
            var indexes = await indexList.ToListAsync();

            // Collection exists if there are any indexes (including _id)
            return indexes.Count > 0;
        }


        public static async Task<bool> CheckCollectionExistsAsync(MongoClient client, string databaseName, string collectionName)
        {               

            var db = client.GetDatabase(databaseName);
            var coll = db.GetCollection<RawBsonDocument>(collectionName);
            try
            {
                var result = await coll.Aggregate()
                    .AppendStage<RawBsonDocument>(@"{ $collStats: { count: {} } }")
                    .FirstOrDefaultAsync();

                if (result == null)
                    return false;
                else
                    return true;
            }
            catch (MongoCommandException ex) when (ex.CodeName == "NamespaceNotFound")
            {
                return false;
            }
            catch
            {

                // Check if collection has at least one index
                var indexCursor = await coll.Indexes.ListAsync();
                var indexes = await indexCursor.ToListAsync();

                bool collectionExists = indexes.Count > 0;
                return collectionExists;
            }

        }

        public static async Task<bool> CheckCollectionValidAsync(MongoClient client, string databaseName, string collectionName)
        {
            if(await CheckCollectionExistsAsync(client, databaseName, collectionName))
            {
                (bool Exits, bool IsCollection) ret;
                try
                {
                    ret = await CheckIsCollectionAsync(client, databaseName, collectionName); //fails if connnected to secondary
                }
                catch
                {
                    return true;
                }                
                if(ret.Exits)
                {
                    return ret.IsCollection;
                }
                else
                    return false;
            }
            else
            {
                return false;
            }
        }

        public static async Task<(long CollectionSizeBytes, long DocumentCount)> GetCollectionStatsAsync(MongoClient client, string databaseName, string collectionName)
        {
            var database = client.GetDatabase(databaseName);
            var collection = database.GetCollection<BsonDocument>(collectionName);

            var statsCommand = new BsonDocument { { "collStats", collectionName } };
            var stats = await database.RunCommandAsync<BsonDocument>(statsCommand);
            long totalCollectionSizeBytes = stats.Contains("storageSize") ? stats["storageSize"].ToInt64() : stats["size"].ToInt64();

            long documentCount;
            if (stats["count"].IsInt32)
            {
                documentCount = stats["count"].ToInt32();
            }
            else if (stats["count"].IsInt64)
            {
                documentCount = stats["count"].ToInt64();
            }
            else
            {
                throw new InvalidOperationException("Unexpected data type for document count.");
            }

            return new (totalCollectionSizeBytes, documentCount);
        }


        public static async Task<bool> DeleteAndCopyIndexesAsync(Log log,MigrationUnit mu, string targetConnectionString, IMongoCollection<BsonDocument> sourceCollection, bool skipIndexes)
        {
            try
            {
                // Extract database and collection details from the source collection
                var sourceDatabase = sourceCollection.Database;
                var sourceCollectionName = sourceCollection.CollectionNamespace.CollectionName;

                // Connect to the target database
                var targetClient = MongoClientFactory.Create(log,targetConnectionString);
                var targetDatabaseName = sourceDatabase.DatabaseNamespace.DatabaseName;
                var targetDatabase = targetClient.GetDatabase(targetDatabaseName);
                var targetCollectionName = sourceCollectionName;

				log.WriteLine($"Creating collection: {targetDatabaseName}.{targetCollectionName}");
                

                // Check if the target collection exists
                var collectionNamesCursor = await targetDatabase.ListCollectionNamesAsync();
                var collectionNames = await collectionNamesCursor.ToListAsync();
                bool targetCollectionExists = collectionNames.Contains(targetCollectionName);

                // Delete the target collection if it exists
                if (targetCollectionExists)
                {
                    await targetDatabase.DropCollectionAsync(targetCollectionName);
					log.WriteLine($"Deleted existing target collection: {targetDatabaseName}.{targetCollectionName}");
                    
                }

                if (skipIndexes)
                    return true;

				log.WriteLine($"Creating indexes for: {targetDatabaseName}.{targetCollectionName}");
                

                // Create the target collection
                await targetDatabase.CreateCollectionAsync(targetCollectionName);
                mu.TargetCreated = true;

                var targetCollection = targetDatabase.GetCollection<BsonDocument>(targetCollectionName);

                IndexCopier indexCopier = new IndexCopier();
                int count=await indexCopier.CopyIndexesAsync(sourceCollection, targetClient, targetDatabaseName, targetCollectionName, log);
                mu.IndexesMigrated = count;
                log.WriteLine($"{count} Indexes copied successfully to {targetDatabaseName}.{targetCollectionName}");
                
                return true;
            }
            catch (Exception ex)
            {
				log.WriteLine($"Error copying indexes: {ex}", LogType.Error);
                
                return false;
            }
        }

        private static FilterDefinition<BsonDocument> BuildFilterLt(string fieldName, BsonValue? value, DataType dataType)
        {
            var filterBuilder = Builders<BsonDocument>.Filter;

            if (value == null || value.IsBsonNull) return FilterDefinition<BsonDocument>.Empty;

            return dataType switch
            {
                DataType.ObjectId => filterBuilder.Lt(fieldName, value.AsObjectId),
                DataType.Int => filterBuilder.Lt(fieldName, value.AsInt32),
                DataType.Int64 => filterBuilder.Lt(fieldName, value.AsInt64),
                DataType.String => filterBuilder.Lt(fieldName, value.AsString),
                DataType.Decimal128 => filterBuilder.Lt(fieldName, value.AsDecimal128),
                DataType.Date => filterBuilder.Lt(fieldName, ((BsonDateTime)value).ToUniversalTime()),
                DataType.Object => filterBuilder.Lt(fieldName, value.AsBsonDocument),
                DataType.BinData => filterBuilder.Lt(fieldName, value.AsBsonDocument),
                _ => throw new ArgumentException($"Unsupported DataType: {dataType}")
            };
        }

        private static FilterDefinition<BsonDocument> BuildFilterGte(string fieldName, BsonValue? value, DataType dataType)
        {
            var filterBuilder = Builders<BsonDocument>.Filter;

            if (value == null || value.IsBsonNull) return FilterDefinition<BsonDocument>.Empty;

            return dataType switch
            {
                DataType.ObjectId => filterBuilder.Gte(fieldName, value.AsObjectId),
                DataType.Int => filterBuilder.Gte(fieldName, value.AsInt32),
                DataType.Int64 => filterBuilder.Gte(fieldName, value.AsInt64),
                DataType.String => filterBuilder.Gte(fieldName, value.AsString),
                DataType.Decimal128 => filterBuilder.Gte(fieldName, value.AsDecimal128),
                DataType.Date => filterBuilder.Gte(fieldName, ((BsonDateTime)value).ToUniversalTime()),
                DataType.Object => filterBuilder.Gte(fieldName, value.AsBsonDocument),
                DataType.BinData => filterBuilder.Gte(fieldName, value.AsBsonDocument),
                _ => throw new ArgumentException($"Unsupported DataType: {dataType}")
            };
        }

        private static string DataTypeToBsonType(DataType dataType)
        {
            return dataType switch
            {
                DataType.ObjectId => "objectId",
                DataType.Int => "int",
                DataType.Int64 => "long",
                DataType.String => "string",
                DataType.Decimal128 => "decimal",
                DataType.Date => "date",
                DataType.Object => "object",
                DataType.BinData => "binData",
                _ => throw new ArgumentException($"Unsupported DataType: {dataType}")
            };
        }

        public static bool UsesIdFieldInFilter(BsonDocument filter)
        {
            foreach (var element in filter)
            {
                var name = element.Name;

                // Direct reference to _id
                if (name == "_id")
                    return true;

                var value = element.Value;

                // Logical operators like $and, $or, $nor contain arrays of filters
                if (name.StartsWith("$") && value.IsBsonArray)
                {
                    foreach (var sub in value.AsBsonArray)
                    {
                        if (sub.IsBsonDocument && UsesIdFieldInFilter(sub.AsBsonDocument))
                            return true;
                    }
                }

                // Nested document (e.g. { "customer": { "_id": ... } })
                if (value.IsBsonDocument && UsesIdFieldInFilter(value.AsBsonDocument))
                    return true;
            }

            return false;
        }

        public static BsonDocument GetFilterDoc(string? filter)
        {
            if (string.IsNullOrWhiteSpace(filter))
                return new BsonDocument(); // return empty document

            return BsonDocument.TryParse(filter, out var filterDoc)
                ? filterDoc
                : new BsonDocument(); // return empty if parsing fails
        }


        public static string GenerateQueryString(BsonValue? gte, BsonValue? lte, DataType dataType, BsonDocument? userFilterDoc, MigrationUnit? migrationUnit = null)
        {
            // Check if we should skip DataType filter when DataTypeFor_Id is specified
            bool skipDataTypeFilter = migrationUnit?.DataTypeFor_Id.HasValue == true;

            // Build the _id sub-object
            var idConditions = new List<string>();

            // Only add $type condition if we're not skipping DataType filter
            if (!skipDataTypeFilter || dataType== DataType.Other)
            {
                idConditions.Add($"\\\"$type\\\": \\\"{DataTypeToBsonType(dataType)}\\\"");
            }

            if (!(gte == null || gte.IsBsonNull) && gte is not BsonMaxKey)
            {
                idConditions.Add($"\\\"$gte\\\": {BsonValueToString(gte, dataType)}");
            }

            if (!(lte == null || lte.IsBsonNull) && lte is not BsonMaxKey)
            {
                idConditions.Add($"\\\"$lte\\\": {BsonValueToString(lte, dataType)}");
            }

            var rootConditions = new List<string>();

            // Only add _id filter if we have conditions
            if (idConditions.Count > 0)
            {
                rootConditions.Add($"\\\"_id\\\": {{ {string.Join(", ", idConditions)} }}");
            }

            

            // Add user filter at the root level if provided
            if (userFilterDoc != null && userFilterDoc.ElementCount > 0)
            {
                //if (!MongoHelper.UsesIdFieldInFilter(userFilterDoc)) // if user filter does not use _id, we can combine at root
                //{
                    // Escape quotes for command-line use
                    var userFilterJsonEscaped = userFilterDoc.ToJson().Replace("\"", "\\\"");
                    rootConditions.Add(userFilterJsonEscaped.TrimStart('{').TrimEnd('}'));
                //}
            }

            // If no conditions exist, return empty filter
            if (rootConditions.Count == 0)
            {
                return "{}";
            }

            // Combine into a valid JSON object
            var queryString = "{ " + string.Join(", ", rootConditions) + " }";
            return queryString;
        }

        public static bool CheckForUserFilterMatch(BsonDocument doc, BsonDocument filter)
        {
            foreach (var element in filter.Elements)
            {
                if (element.Name == "$and")
                {
                    foreach (var cond in element.Value.AsBsonArray)
                    {
                        if (!CheckForUserFilterMatch(doc, cond.AsBsonDocument)) return false;
                    }
                }
                else if (element.Name == "$or")
                {
                    bool any = false;
                    foreach (var cond in element.Value.AsBsonArray)
                    {
                        if (CheckForUserFilterMatch(doc, cond.AsBsonDocument))
                        {
                            any = true;
                            break;
                        }
                    }
                    if (!any) return false;
                }
                else if (element.Value.IsBsonDocument)
                {
                    var opDoc = element.Value.AsBsonDocument;
                    foreach (var op in opDoc.Elements)
                    {
                        switch (op.Name)
                        {
                            case "$eq":
                                if (!doc.Contains(element.Name) || doc[element.Name] != op.Value) return false;
                                break;
                            case "$gte":
                                if (!doc.Contains(element.Name) || doc[element.Name].CompareTo(op.Value) < 0) return false;
                                break;
                            case "$gt":
                                if (!doc.Contains(element.Name) || doc[element.Name].CompareTo(op.Value) <= 0) return false;
                                break;
                            case "$lte":
                                if (!doc.Contains(element.Name) || doc[element.Name].CompareTo(op.Value) > 0) return false;
                                break;
                            case "$lt":
                                if (!doc.Contains(element.Name) || doc[element.Name].CompareTo(op.Value) >= 0) return false;
                                break;
                            case "$in":
                                if (!doc.Contains(element.Name) || !op.Value.AsBsonArray.Contains(doc[element.Name])) return false;
                                break;
                            default:
                                throw new NotSupportedException($"Operator {op.Name} is not supported yet.");
                        }
                    }
                }
                else
                {
                    if (!doc.Contains(element.Name) || doc[element.Name] != element.Value) return false;
                }
            }

            return true;
        }


        public static string GenerateQueryString(BsonDocument? userFilterDoc)
        {
            if (userFilterDoc == null || userFilterDoc.ElementCount == 0)
            {
                return "{}"; // Empty filter
            }

            // Convert to JSON and escape quotes for shell usage
            var userFilterJsonEscaped = userFilterDoc.ToJson().Replace("\"", "\\\"");

            return userFilterJsonEscaped;
        }
                

        public static BsonDocument ConvertUserFilterToBSONDocument(string userFilter)
        {
            // Start with user filter or an empty JSON object
            return string.IsNullOrWhiteSpace(userFilter)
                ? new BsonDocument()
                : BsonSerializer.Deserialize<BsonDocument>(userFilter);
             
        }

        private static string BsonValueToString(BsonValue? value, DataType dataType)
        {
            if (value == null || value.IsBsonNull) return string.Empty;

            if (value is BsonMaxKey)
                return "{ \\\"$maxKey\\\": 1 }"; // Return a $maxKey representation

            return dataType switch
            {
                DataType.ObjectId => $"{{\\\"$oid\\\":\\\"{value.AsObjectId}\\\"}}",
                DataType.Int => value.AsInt32.ToString(),
                DataType.Int64 => value.AsInt64.ToString(),
                DataType.String => $"\\\"{value.AsString}\\\"",
                DataType.Decimal128 => $"{{\\\"$numberDecimal\\\":\\\"{value.AsDecimal128}\\\"}}",
                DataType.Date => $"{{\\\"$date\\\":\\\"{((BsonDateTime)value).ToUniversalTime():yyyy-MM-ddTHH:mm:ssZ}\\\"}}",
                DataType.Object => value.AsBsonDocument.ToString(),
                DataType.BinData => $"{{\\\"$binary\\\":{{\\\"base64\\\":\\\"{Convert.ToBase64String(value.AsBsonBinaryData.Bytes)}\\\",\\\"subType\\\":\\\"{value.AsBsonBinaryData.SubType:x2}\\\"}}}}",
                _ => throw new ArgumentException($"Unsupported DataType: {dataType}")
            };
        }

        public static DateTime BsonTimestampToUtcDateTime(BsonTimestamp bsonTimestamp)
        {
            // Extract seconds from the timestamp's value
            long secondsSinceEpoch = bsonTimestamp.Timestamp;

            // Convert seconds since Unix epoch to DateTime in UTC
            return DateTimeOffset.FromUnixTimeSeconds(secondsSinceEpoch).UtcDateTime;
        }


        public static FilterDefinition<BsonDocument> BuildFilterFromDocumentKey(BsonDocument documentKey)
        {
            var filters = new List<FilterDefinition<BsonDocument>>();

            foreach (var element in documentKey.Elements)
            {
                filters.Add(Builders<BsonDocument>.Filter.Eq(element.Name, element.Value));
            }

            return filters.Count == 1
                ? filters[0]
                : Builders<BsonDocument>.Filter.And(filters);
        }



        /// <summary>
        /// Common function to set resume token properties on either MigrationJob or MigrationUnit
        /// </summary>
        /// <param name="target">The target object (MigrationJob or MigrationUnit) to set properties on</param>
        /// <param name="change">The change stream document containing the resume token information</param>
        /// <param name="resetCS">Whether this is a change stream reset operation</param>
        /// <param name="databaseName">Database name for server-level operations</param>
        /// <param name="collectionName">Collection name for server-level operations</param>
        private static void SetResumeTokenProperties(object target, ChangeStreamDocument<BsonDocument> change, bool resetCS, string? databaseName = null, string? collectionName = null)
        {
            string resumeTokenJson = change.ResumeToken.ToJson();
            string documentKeyJson = change.DocumentKey.ToJson();
            var operationType = change.OperationType;

            // Determine timestamp
            DateTime timestamp;
            if (change.ClusterTime != null)
            {
                timestamp = BsonTimestampToUtcDateTime(change.ClusterTime);
            }
            else if (change.WallTime.HasValue)
            {
                timestamp = change.WallTime.Value.ToUniversalTime();
            }
            else
            {
                timestamp = DateTime.UtcNow;
            }

            // Set properties based on target type
            if (target is MigrationJob job)
            {
                // Server-level resume token setting
                job.ResumeToken = resumeTokenJson;
                
                if (!resetCS && string.IsNullOrEmpty(job.OriginalResumeToken))
                    job.OriginalResumeToken = resumeTokenJson;

                job.CursorUtcTimestamp = timestamp;
                job.ResumeTokenOperation = operationType;
                job.ResumeDocumentId = documentKeyJson;

                
                // Store collection key for server-level auto replay
                if (change.CollectionNamespace != null && !string.IsNullOrEmpty(databaseName) && !string.IsNullOrEmpty(collectionName))
                {
                    job.ResumeCollectionKey = $"{databaseName}.{collectionName}";
                }

                MigrationJobContext.SaveMigrationJob(job);
            }
            else if (target is MigrationUnit mu)
            {
                // Collection-level resume token setting
                mu.ResumeToken = resumeTokenJson;
                
                if (!resetCS && string.IsNullOrEmpty(mu.OriginalResumeToken))
                    mu.OriginalResumeToken = resumeTokenJson;

                mu.CursorUtcTimestamp = timestamp;
                mu.ResumeTokenOperation = operationType;
                mu.ResumeDocumentId = documentKeyJson;

                // Clear the reset flag after successfully resetting the change stream
                // This prevents duplicate "Resetting change stream resume token" log messages
                if (resetCS)
                {
                    mu.ResetChangeStream = false;
                }
                MigrationJobContext.SaveMigrationUnit(mu,true);
            }
        }

        public static BsonTimestamp ConvertToBsonTimestamp(DateTime dateTime)
        {
            // Convert DateTime to Unix timestamp (seconds since Jan 1, 1970)
            long secondsSinceEpoch = new DateTimeOffset(dateTime).ToUnixTimeSeconds();

            // BsonTimestamp requires seconds and increment (logical clock)
            // Here we're using a default increment of 0. You can adjust this if needed.
            return new BsonTimestamp((int)secondsSinceEpoch, 0);
        }

        public static async Task<int> ProcessInsertsAsync<TMigration>(
            TMigration mu,
            IMongoCollection<BsonDocument> collection,
            List<ChangeStreamDocument<BsonDocument>> events,
            CounterDelegate<TMigration> incrementCounter,
            Log log,
            string logPrefix,
            int batchSize = 50,
            bool isAggressive = false,
            bool isAggressiveComplete = true,
            string jobId = "",
            MongoClient? targetClient = null,
            bool isSimulatedRun = false)
        {
            int failures = 0;
            string databaseName = collection.Database.DatabaseNamespace.DatabaseName;
            string collectionName = collection.CollectionNamespace.CollectionName;

            // Initialize aggressive change stream helper if needed
            var aggressiveHelper = InitializeAggressiveHelper(isAggressive, isAggressiveComplete, targetClient, log, jobId);

            // Insert operations
            foreach (var batch in events.Chunk(batchSize))
            {
                // Deduplicate inserts by _id to avoid duplicate key errors within the same batch
                var deduplicatedInserts = batch
                    .Where(e => e.FullDocument != null && e.FullDocument.Contains("_id"))
                    .GroupBy(e => e.DocumentKey.ToJson())
                    .Select(g => g.First())
                    .ToList();

                // Handle aggressive change stream scenario - store document keys instead of inserting
                var (shouldSkipInsert, insertFailures) = await StoreDocumentKeysForAggressiveMode(
                    isAggressive, isAggressiveComplete, aggressiveHelper, isSimulatedRun,
                    deduplicatedInserts.Where(e => e.DocumentKey != null).Select(e => e.DocumentKey).ToList(),
                    databaseName, collectionName, "insert",
                    mu, incrementCounter, ChangeStreamOperationType.Insert,
                    log, logPrefix);
                
                failures += insertFailures;
                
                if (shouldSkipInsert)
                {
                    continue; // Skip normal insert processing
                }

                var insertModels = deduplicatedInserts
                    .Select(e =>
                    {
                        var doc = e.FullDocument;
                        var id = doc["_id"];

                        if (id.IsObjectId)
                            doc["_id"] = id.AsObjectId;

                        return new InsertOneModel<BsonDocument>(doc);
                    })
                    .ToList();

                try
                {
                    var (success, insertCount) = await ExecuteBulkWriteWithRetry(
                        collection, insertModels, "insert", log, logPrefix, isSimulatedRun, 
                        result => result.InsertedCount);

                    if (success)
                    {
                        incrementCounter(mu, CounterType.Processed, ChangeStreamOperationType.Insert, (int)insertCount);
                    }
                }
                catch (MongoBulkWriteException<BsonDocument> ex)
                {
                    var (insertCount, batchFailures) = HandleBulkWriteException(
                        ex, mu, incrementCounter, ChangeStreamOperationType.Insert, 
                        log, logPrefix, collection.CollectionNamespace.FullName, 
                        result => result?.InsertedCount ?? 0);

                    incrementCounter(mu, CounterType.Processed, ChangeStreamOperationType.Insert, (int)insertCount);
                    failures += batchFailures;
                }
            }
            return failures; // Return the count of failures encountered during processing
        }

        

        public static async Task<int> ProcessUpdatesAsync<TMigration>(
            TMigration mu,
            IMongoCollection<BsonDocument> collection,
            List<ChangeStreamDocument<BsonDocument>> events,
            CounterDelegate<TMigration> incrementCounter,
            Log log,
            string logPrefix,
            int batchSize = 50,
            bool isAggressive = false,
            bool isAggressiveComplete = true,
            string jobId = "",
            MongoClient? targetClient = null,
            bool isSimulatedRun = false)
        {
            int failures = 0;
            string databaseName = collection.Database.DatabaseNamespace.DatabaseName;
            string collectionName = collection.CollectionNamespace.CollectionName;

            // Initialize aggressive change stream helper if needed
            var aggressiveHelper = InitializeAggressiveHelper(isAggressive, isAggressiveComplete, targetClient, log, jobId);

            // Update operations
            foreach (var batch in events.Chunk(batchSize))
            {
                // Group by _id to handle multiple updates to the same document in the batch
                var groupedUpdates = batch
                    .Where(e => e.FullDocument != null && e.FullDocument.Contains("_id"))
                    .GroupBy(e => e.DocumentKey.ToJson()) //use document key instead of _id
                    .Select(g => g.OrderByDescending(e => e.ClusterTime ?? new BsonTimestamp(0, 0)).First()) // Take the latest update for each document
                    .ToList();

                // Handle aggressive change stream scenario - store document keys instead of updating
                var (shouldSkipUpdate, updateFailures) = await StoreDocumentKeysForAggressiveMode(
                    isAggressive, isAggressiveComplete, aggressiveHelper, isSimulatedRun,
                    groupedUpdates.Where(e => e.DocumentKey != null).Select(e => e.DocumentKey).ToList(),
                    databaseName, collectionName, "update",
                    mu, incrementCounter, ChangeStreamOperationType.Update,
                    log, logPrefix);
                
                failures += updateFailures;
                
                if (shouldSkipUpdate)
                {
                    continue; // Skip normal update processing
                }

                // Normal update processing (when not in aggressive mode or when bulk copy is complete)

                var updateModels = groupedUpdates
                    .Select(e =>
                    {
                        var doc = e.FullDocument;
                        var id = doc["_id"];

                        if (id.IsObjectId)
                            id = id.AsObjectId;

                        var filter = BuildFilterFromDocumentKey(e.DocumentKey);
                        //var filter = Builders<BsonDocument>.Filter.Eq("_id", id);

                        // Use ReplaceOneModel instead of UpdateOneModel to avoid conflicts with unique indexes
                        return new ReplaceOneModel<BsonDocument>(filter, doc) { IsUpsert = true };
                    })
                    .ToList();

                long updateCount = 0;
                long upsertCount = 0;
                
                try
                {
                    var (success, modifiedCount, upserts) = await ExecuteBulkWriteWithRetryForUpdates(
                        collection, updateModels, log, logPrefix, isSimulatedRun);

                    if (success)
                    {
                        updateCount = modifiedCount;
                        upsertCount = upserts;
                    }
                }
                catch (MongoBulkWriteException<BsonDocument> ex)
                {
                    var (modifiedCount, upserts, batchFailures) = await HandleUpdateBulkWriteException(
                        ex, collection, updateModels, mu, incrementCounter, log, logPrefix);
                    
                    updateCount = modifiedCount;
                    upsertCount = upserts;
                    failures += batchFailures;
                }
                
                // Upserts are inserts, not updates - count them separately
                incrementCounter(mu, CounterType.Processed, ChangeStreamOperationType.Update, (int)updateCount);
                incrementCounter(mu, CounterType.Processed, ChangeStreamOperationType.Insert, (int)upsertCount);
            }
            return failures; // Return the count of failures encountered during processing
        }

        public static bool IsCosmosRUEndpoint<T>(IMongoCollection<T> collection)
        {
            if (collection == null) return false;

            // Access the client settings via the database's client
            var settings = collection.Database.Client.Settings;

            // Check all servers (endpoints) in the settings
            return settings.Servers
                .Any(s => s.Host.Contains("mongo.cosmos.azure.com"));
        }


        public static async Task<int> ProcessDeletesAsync<TMigration>(
            TMigration mu,
            IMongoCollection<BsonDocument> collection,
            List<ChangeStreamDocument<BsonDocument>> events,
            CounterDelegate<TMigration> incrementCounter,
            Log log,
            string logPrefix,
            int batchSize = 50,
            bool isAggressive = false,
            bool isAggressiveComplete = true,
            string jobId = "",
            MongoClient? targetClient = null,
            bool isSimulatedRun = false)
        {
            int failures = 0;
            string databaseName = collection.Database.DatabaseNamespace.DatabaseName;
            string collectionName = collection.CollectionNamespace.CollectionName;

            // Initialize aggressive change stream helper if needed
            var aggressiveHelper = InitializeAggressiveHelper(isAggressive, isAggressiveComplete, targetClient, log, jobId);

            // Delete operations
            foreach (var batch in events.Chunk(batchSize))
            {
                // Deduplicate deletes by _id within the same batch
                var deduplicatedDeletes = batch
                    .GroupBy(e => e.DocumentKey != null ? e.DocumentKey.ToJson() : string.Empty) // safe null handling
                    .Where(g => !string.IsNullOrEmpty(g.Key))
                    .Select(g => g.First())
                    .ToList();


                //log all ids inbatch as CSV
                //var idsInBatch = deduplicatedDeletes
                //    .Select(e => e.DocumentKey.ToJson())
                //    .ToList();
                //log.WriteLine($"{logPrefix} Processing {deduplicatedDeletes.Count} deletes (deduplicated from {batch.Length}) with _ids: {string.Join(", ", idsInBatch)} in {collection.CollectionNamespace.FullName}");

                // Handle aggressive change stream scenario - store document keys instead of deleting
                var (shouldSkipDelete, deleteFailures) = await StoreDocumentKeysForAggressiveMode(
                    isAggressive, isAggressiveComplete, aggressiveHelper, isSimulatedRun,
                    deduplicatedDeletes.Where(e => e.DocumentKey != null).Select(e => e.DocumentKey).ToList(),
                    databaseName, collectionName, "delete",
                    mu, incrementCounter, ChangeStreamOperationType.Delete,
                    log, logPrefix);
                
                failures += deleteFailures;
                
                if (shouldSkipDelete)
                {
                    continue; // Skip normal delete processing, move to next batch
                }
                // Normal delete processing
                var deleteModels = deduplicatedDeletes
                    .Select(e =>
                    {
                        try
                        {
                            if (!e.DocumentKey.Contains("_id"))
                            {
                                log.WriteLine($"{logPrefix} Delete event missing _id in {collection.CollectionNamespace.FullName}: {e.DocumentKey.ToJson()}");
                                return null;
                            }

                            var id = e.DocumentKey.GetValue("_id", null);
                            if (id == null)
                            {
                                log.WriteLine($"{logPrefix} _id is null in DocumentKey in {collection.CollectionNamespace.FullName}: {e.DocumentKey.ToJson()}");
                                return null;
                            }

                            if (id.IsObjectId)
                                id = id.AsObjectId;

                            var filter = BuildFilterFromDocumentKey(e.DocumentKey);
                            return new DeleteOneModel<BsonDocument>(filter);
                        }
                        catch (Exception dex)
                        {
                            log.WriteLine($"{logPrefix} Error building delete model in {collection.CollectionNamespace.FullName}: {e.DocumentKey.ToJson()}, Error: {dex.Message}");
                            return null;
                        }
                    })
                    .Where(model => model != null)
                    .Cast<DeleteOneModel<BsonDocument>>()
                    .ToList();

                if (deleteModels.Any())
                {
                    try
                    {
                        var (success, deletedCount) = await ExecuteBulkWriteWithRetry(
                            collection, deleteModels, "delete", log, logPrefix, isSimulatedRun,
                            result => result.DeletedCount);

                        if (success)
                        {
                            incrementCounter(mu, CounterType.Processed, ChangeStreamOperationType.Delete, (int)deletedCount);
                        }
                    }
                    catch (MongoBulkWriteException<BsonDocument> ex)
                    {
                        // Count successful deletes even when some fail
                        long deletedCount = ex.Result?.DeletedCount ?? 0;

                        // Log errors that are not "document not found" (which is expected for deletes)
                        var significantErrors = ex.WriteErrors
                            .Where(err => !err.Message.Contains("not found") && err.Code != 11000)
                            .ToList();

                        if (significantErrors.Any())
                        {
                            failures += significantErrors.Count;
                            log.WriteLine($"{logPrefix} Bulk delete error in {collection.CollectionNamespace.FullName}: {string.Join(", ", significantErrors.Select(e => e.Message))}");
                        }

                        incrementCounter(mu, CounterType.Processed, ChangeStreamOperationType.Delete, (int)deletedCount);
                    }
                }
            }
            return failures; // Return the count of failures encountered during processing
        }

    }
}