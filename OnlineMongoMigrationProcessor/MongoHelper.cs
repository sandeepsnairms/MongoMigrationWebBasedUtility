using MongoDB.Bson;
using MongoDB.Driver;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json.Nodes;
using System.Threading.Tasks;

namespace OnlineMongoMigrationProcessor
{
    internal static class MongoHelper
    {
        public static long GetActualDocumentCount(IMongoCollection<BsonDocument> collection, MigrationUnit item)
        {
            return collection.CountDocuments(Builders<BsonDocument>.Filter.Empty);
        }

        public static FilterDefinition<BsonDocument> GenerateQueryFilter(BsonValue? gte, BsonValue? lte, DataType dataType)
        {
            var filterBuilder = Builders<BsonDocument>.Filter;

            // Initialize an empty filter
            FilterDefinition<BsonDocument> filter = FilterDefinition<BsonDocument>.Empty;


            // Create the $type filter
            var typeFilter = filterBuilder.Eq("_id", new BsonDocument("$type", DataTypeToBsonType(dataType)));

            // Add conditions based on gte and lt values
            if (!(gte == null || gte.IsBsonNull) && !(lte == null || lte.IsBsonNull) && (gte is not BsonMaxKey && lte is not BsonMaxKey))
            {
                filter = filterBuilder.And(
                    typeFilter,
                    BuildFilterGTE("_id", gte, dataType),
                    BuildFilterLT("_id", lte, dataType)
                );
            }
            else if (!(gte == null || gte.IsBsonNull) && gte is not BsonMaxKey)
            {
                filter = filterBuilder.And(typeFilter, BuildFilterGTE("_id", gte, dataType));
            }
            else if (!(lte == null || lte.IsBsonNull) && lte is not BsonMaxKey)
            {
                filter = filterBuilder.And(typeFilter, BuildFilterLT("_id", lte, dataType));
            }
            else
            {
                filter = typeFilter;
            }
            return filter;
        }

        public static long GetDocCount(IMongoCollection<BsonDocument> _collection, BsonValue? gte, BsonValue? lte, DataType dataType)
        {
            FilterDefinition<BsonDocument> filter = GenerateQueryFilter(gte, lte, dataType);

            // Execute the query and return the count
            var count = _collection.CountDocuments(filter);
            return count;
        }

        public static long GetDocCount(IMongoCollection<BsonDocument> _collection, FilterDefinition<BsonDocument> filter)
        {
            // Execute the query and return the count
            var count = _collection.CountDocuments(filter);
            return count;
        }

        public static async Task<bool> IsChangeStreamEnabledAsync(string connectionString)
        {
            try
            {
                // Connect to the MongoDB server
                var client = new MongoClient(connectionString);

                // Check the server status to verify replica set or sharded cluster
                var adminDatabase = client.GetDatabase("admin");
                var command = new BsonDocument("isMaster", 1);
                var isMasterResult = await adminDatabase.RunCommandAsync<BsonDocument>(command);

                // Check if the server is part of a replica set or a sharded cluster
                if (isMasterResult.Contains("setName") || isMasterResult.GetValue("msg", "").AsString == "isdbgrid")
                {
                    Log.WriteLine("Change streams are enabled on source (replica set or sharded cluster).");
                    Log.Save();
                    return true;
                }
                else
                {
                    Log.WriteLine("Change streams are not enabled on source (standalone server).", LogType.Error);
                    Log.Save();
                    return false;
                }
            }
            catch (Exception ex)
            {
                Log.WriteLine($"Error checking for change streams: {ex.Message}",LogType.Error);
                Log.Save();
                return false;
            }
        }

        public static async  Task<bool> DeleteAndCopyIndexesAsync(string targetConnectionString, IMongoCollection<BsonDocument> sourceCollection)
        {
            

            try
            {
                // Extract database and collection details from the source collection
                var sourceDatabase = sourceCollection.Database;
                var sourceCollectionName = sourceCollection.CollectionNamespace.CollectionName;

                // Connect to the target database
                var targetClient = new MongoClient(targetConnectionString);
                var targetDatabaseName = sourceDatabase.DatabaseNamespace.DatabaseName;
                var targetDatabase = targetClient.GetDatabase(targetDatabaseName);
                var targetCollectionName = sourceCollectionName;


                Log.WriteLine($"Creating collection with indexes: {targetDatabaseName}.{targetCollectionName}");
                Log.Save();

                // Check if the target collection exists
                var collectionNamesCursor = await targetDatabase.ListCollectionNamesAsync();
                var collectionNames = await collectionNamesCursor.ToListAsync();
                bool targetCollectionExists = collectionNames.Contains(targetCollectionName);

                // Delete the target collection if it exists
                if (targetCollectionExists)
                {
                    await targetDatabase.DropCollectionAsync(targetCollectionName);
                    Log.WriteLine($"Deleted target collection: {targetDatabaseName}.{targetCollectionName}");
                    Log.Save();
                }

                // Get the indexes from the source collection
                var indexes = await sourceCollection.Indexes.ListAsync();
                var indexDocuments = await indexes.ToListAsync();

                // Create the target collection
                await targetDatabase.CreateCollectionAsync(targetCollectionName);
                var targetCollection = targetDatabase.GetCollection<BsonDocument>(targetCollectionName);

                // Copy the indexes to the target collection
                foreach (var indexDocument in indexDocuments)
                {
                    // Exclude the default "_id_" index as it is automatically created
                    if (indexDocument.GetValue("name", "") == "_id_")
                        continue;

                    // Extract the keys and options for the index
                    var keys = indexDocument["key"].AsBsonDocument;

                    CreateIndexOptions options=null;
                    try
                    {
                        options = new CreateIndexOptions
                        {
                            Name = indexDocument.GetValue("name", default(BsonValue)).AsString,
                            Unique = indexDocument.GetValue("unique", false).ToBoolean()
                        };

                        // Create the index on the target collection
                        var indexModel = new CreateIndexModel<BsonDocument>(keys, options);
                        await targetCollection.Indexes.CreateOneAsync(indexModel);
                    }
                    catch (Exception ex)
                    {
                        Log.WriteLine($"Error copying index {options.Name}  for {targetDatabaseName}.{targetCollectionName}. Details: {ex.Message}", LogType.Error);
                        Log.Save();
                    }                    
                }

                Log.WriteLine($"{indexDocuments.Count} Indexes copied successfully to {targetDatabaseName}.{targetCollectionName}");
                Log.Save();
                return true;
            }
            catch (Exception ex)
            {
                Log.WriteLine($"Error copying indexes: {ex.Message}",LogType.Error);
                Log.Save();
                return false;
            }
        }


        private static FilterDefinition<BsonDocument> BuildFilterLT(string fieldName, BsonValue? value, DataType dataType)
        {
            var filterBuilder = Builders<BsonDocument>.Filter;

            if (value == null || value.IsBsonNull) return FilterDefinition<BsonDocument>.Empty;

            switch (dataType)
            {
                case DataType.ObjectId:
                    return filterBuilder.Lt(fieldName, value.AsObjectId);
                case DataType.Int:
                    return filterBuilder.Lt(fieldName, value.AsInt32); // Handle Int32
                case DataType.Int64:
                    return filterBuilder.Lt(fieldName, value.AsInt64); // Handle Int64
                case DataType.String:
                    return filterBuilder.Lt(fieldName, value.AsString);
                case DataType.Decimal128:
                    return filterBuilder.Lt(fieldName, value.AsDecimal128); // Handle Decimal128
                case DataType.Date:
                    return filterBuilder.Lt(fieldName, ((BsonDateTime)value).ToUniversalTime()); // Handle Date
                case DataType.UUID:
                    return filterBuilder.Lt(fieldName, value.AsGuid); // Handle UUID as Guid
                case DataType.Object:
                    return filterBuilder.Lt(fieldName, value.AsBsonDocument); // Assuming object is a BsonDocument
                default:
                    throw new ArgumentException($"Unsupported DataType: {dataType}");
            }
        }

        private static FilterDefinition<BsonDocument> BuildFilterGTE(string fieldName, BsonValue? value, DataType dataType)
        {
            var filterBuilder = Builders<BsonDocument>.Filter;

            if (value == null || value.IsBsonNull) return FilterDefinition<BsonDocument>.Empty;

            switch (dataType)
            {
                case DataType.ObjectId:
                    return filterBuilder.Gte(fieldName, value.AsObjectId);
                case DataType.Int:
                    return filterBuilder.Gte(fieldName, value.AsInt32); // Handle Int32
                case DataType.Int64:
                    return filterBuilder.Gte(fieldName, value.AsInt64); // Handle Int64
                case DataType.String:
                    return filterBuilder.Gte(fieldName, value.AsString);
                case DataType.Decimal128:
                    return filterBuilder.Gte(fieldName, value.AsDecimal128); // Handle Decimal128
                case DataType.Date:
                    return filterBuilder.Gte(fieldName, ((BsonDateTime)value).ToUniversalTime()); // Handle Date
                case DataType.UUID:
                    return filterBuilder.Gte(fieldName, value.AsGuid); // Handle UUID as Guid
                case DataType.Object:
                    return filterBuilder.Gte(fieldName, value.AsBsonDocument); // Assuming object is a BsonDocument
                default:
                    throw new ArgumentException($"Unsupported DataType: {dataType}");
            }
        }

        private static string DataTypeToBsonType(DataType dataType)
        {
            // Add $type condition based on the specified data type
            string bsonType = dataType switch
            {
                DataType.ObjectId => "objectId",  // String-based BSON type name
                DataType.Int => "int",            // Int32 type
                DataType.Int64 => "long",         // Int64 type
                DataType.String => "string",      // String type
                DataType.Decimal128 => "decimal", // Decimal128 type
                DataType.Date => "date",          // Date type
                DataType.UUID => "binData",       // UUID uses binary data type
                DataType.Object => "object",      // Embedded document
                _ => throw new ArgumentException($"Unsupported DataType: {dataType}")
            };
            return bsonType;
        }

        public static string GenerateQueryString(BsonValue? gte, BsonValue? lte, DataType dataType)
        {
            // Initialize the query string
            string queryString = "{ \\\"_id\\\": { ";

            // Track the conditions added to ensure correct formatting
            var conditions = new List<string>();                       
           
            conditions.Add($"\\\"$type\\\": \\\"{DataTypeToBsonType(dataType)}\\\"");

            // Add $gte condition if present
            if (!(gte == null || gte.IsBsonNull) && gte is not BsonMaxKey)
            {
                conditions.Add($"\\\"$gte\\\": {BsonValueToString(gte, dataType)}");
            }

            // Add $lte condition if present
            if (!(lte == null || lte.IsBsonNull) && lte is not BsonMaxKey)
            {
                conditions.Add($"\\\"$lt\\\": {BsonValueToString(lte, dataType)}");
            }

            // Combine the conditions with a comma
            queryString += string.Join(", ", conditions);

            // Close the query string
            queryString += " } }";

            return queryString;
        }

        private static string BsonValueToString(BsonValue? value, DataType dataType)
        {
            if (value == null || value.IsBsonNull) return string.Empty;

            if (value is BsonMaxKey)
                return "{ \\\"$maxKey\\\": 1 }"; // Return a $maxKey representation

            switch (dataType)
            {
                case DataType.ObjectId:
                    return $"{{\\\"$oid\\\":\\\"{value.AsObjectId}\\\"}}";
                case DataType.Int:
                    return value.AsInt32.ToString();  // Use Int32 for Int
                case DataType.Int64:
                    return value.AsInt64.ToString();  // Use Int64 for 64-bit integers
                case DataType.String:
                    return $"\\\"{value.AsString}\\\""; // String needs to be wrapped in double quotes
                case DataType.Decimal128:
                    return $"{{\\\"$numberDecimal\\\":\\\"{value.AsDecimal128}\\\"}}"; // Decimal128
                case DataType.Date:
                    return $"{{\\\"$date\\\":\\\"{((BsonDateTime)value).ToUniversalTime():yyyy-MM-ddTHH:mm:ssZ}\\\"}}"; // Date
                case DataType.UUID:
                    return $"{{\\\"$binary\\\":\\\"{value.AsGuid.ToString()}\\\", \\\"$type\\\":\\\"04\\\"}}"; // UUID
                case DataType.Object:
                    return value.AsBsonDocument.ToString();  // Raw BSON representation for Object
                default:
                    throw new ArgumentException($"Unsupported DataType: {dataType}");
            }
        }

        public static DateTime BsonTimestampToUtcDateTime(BsonTimestamp bsonTimestamp)
        {
            // Extract seconds from the timestamp's value
            long secondsSinceEpoch = bsonTimestamp.Timestamp;

            // Convert seconds since Unix epoch to DateTime in UTC
            return DateTimeOffset.FromUnixTimeSeconds(secondsSinceEpoch).UtcDateTime;
        }

        public static BsonTimestamp ConvertToBsonTimestamp(DateTime dateTime)
        {
            // Convert DateTime to Unix timestamp (seconds since Jan 1, 1970)
            long secondsSinceEpoch = new DateTimeOffset(dateTime).ToUnixTimeSeconds();

            // BsonTimestamp requires seconds and increment (logical clock)
            // Here we're using a default increment of 0. You can adjust this if needed.
            return new BsonTimestamp((int)secondsSinceEpoch, 0);
        }

    }
}
