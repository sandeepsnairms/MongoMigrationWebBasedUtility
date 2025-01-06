using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Collections.Concurrent;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.ComponentModel.DataAnnotations;
using MongoDB.Bson.Serialization.Serializers;

namespace OnlineMongoMigrationProcessor
{
    public class SamplePartitioner
    {
#pragma warning disable CS8603
#pragma warning disable CS8604

        private readonly IMongoCollection<BsonDocument> _collection;

        public SamplePartitioner(IMongoCollection<BsonDocument> collection)
        {
            _collection = collection;
        }

        /// <summary>
        /// Creates partitions based on sampled data from the collection.
        /// </summary>
        /// <param name="idField">The field used as the partition key.</param>
        /// <param name="partitionCount">The number of desired partitions.</param>
        /// <returns>A list of partition boundaries.</returns>
        public List<(BsonValue Min, BsonValue Max)> CreatePartitions(string idField, int partitionCount, DataType dataType, long minDocsPerChunk)
        {
            if (partitionCount <= 1)
                throw new ArgumentException("Partition count must be greater than 1.");

            // Calculate sampleCount as 10 times the partitionCount
            int sampleCount = partitionCount * 10;

            // Cap sampleCount at 2000
            sampleCount = Math.Min(sampleCount, 2000);

            //reduce partitionCount if less samples
            partitionCount = Math.Min(sampleCount, partitionCount);

            

            // Step 1: Build the filter pipeline based on the data type
            long docCountByType= GetDocumentCountByDataType(_collection,idField,dataType);
            if (docCountByType == 0)
            {
                Log.WriteLine($"0 Documents where {idField} is {dataType}");
                Log.Save();
                return null;
            }
            else if (docCountByType < minDocsPerChunk)
            {
                Log.WriteLine($"Document Count where {idField} is {dataType}:{docCountByType} is less than min partiton size.");
                sampleCount = 1;
                partitionCount = 1;
            }

            Log.WriteLine($"SampleCount: {sampleCount}, Partition Count: {partitionCount} where {idField} is {dataType}");
            Log.Save();


            BsonDocument matchCondition = MatchConditionBuilder(dataType, idField);

            // Step 2: Sample the data
            var pipeline = new[]
            {
                new BsonDocument("$match", matchCondition),  // Add the match condition for the data type
                new BsonDocument("$sample", new BsonDocument("size", sampleCount)),
                new BsonDocument("$project", new BsonDocument(idField, 1)) // Keep only the partition key
            };

            var sampledData = _collection.Aggregate<BsonDocument>(pipeline).ToList();
            var partitionValues = sampledData
                .Select(doc => doc.GetValue(idField, BsonNull.Value))
                .Where(value => value != BsonNull.Value)
                .Distinct()
                .OrderBy(value => value)
                .ToList();

           

            // Step 2: Calculate partition boundaries

            var boundaries = new List<(BsonValue Min, BsonValue Max)>();
            int step = Math.Max(1,partitionValues.Count / partitionCount);

            for (int i = 0; i < partitionCount; i++)
            {
                var min = partitionValues[i * step];
                var max = i == partitionCount - 1 ? BsonMaxKey.Value : partitionValues[(i + 1) * step];
                boundaries.Add((min, max));
            }

            Log.WriteLine($"Total Chunks: {boundaries.Count} where {idField} is {dataType}");
            Log.Save();

            return boundaries;
            
        }

        public static long GetDocumentCountByDataType(IMongoCollection<BsonDocument> collection, string idField, DataType dataType)
        {
            var filterBuilder = Builders<BsonDocument>.Filter;

            BsonDocument matchCondition = MatchConditionBuilder(dataType,idField);

            // Get the count of documents matching the filter
            var count = collection.CountDocuments(matchCondition);
            return count;
        }

        private static BsonDocument MatchConditionBuilder(DataType dataType, string idField)
        {
            BsonDocument matchCondition;
            switch (dataType)
            {
                case DataType.ObjectId:
                    matchCondition = new BsonDocument(idField, new BsonDocument("$type", 7)); // 7 is BSON type for ObjectId
                    break;
                case DataType.Int:
                    matchCondition = new BsonDocument(idField, new BsonDocument("$type", 16)); // 16 is BSON type for Int32
                    break;
                case DataType.Int64:
                    matchCondition = new BsonDocument(idField, new BsonDocument("$type", 18)); // 18 is BSON type for Int64
                    break;
                case DataType.String:
                    matchCondition = new BsonDocument(idField, new BsonDocument("$type", 2)); // 2 is BSON type for String
                    break;
                case DataType.Decimal128:
                    matchCondition = new BsonDocument(idField, new BsonDocument("$type", 19)); // 19 is BSON type for Decimal128
                    break;
                case DataType.Date:
                    matchCondition = new BsonDocument(idField, new BsonDocument("$type", 9)); // 9 is BSON type for Date
                    break;
                case DataType.UUID:
                    matchCondition = new BsonDocument(idField, new BsonDocument("$type", 4)); // 4 is BSON type for Binary (UUID)
                    break;
                case DataType.Object:
                    matchCondition = new BsonDocument(idField, new BsonDocument("$type", 3)); // 3 is BSON type for embedded document (Object)
                    break;
                default:
                    throw new ArgumentException($"Unsupported DataType: {dataType}");
            }
            return matchCondition;
        }

        public static (BsonValue gte, BsonValue lt) GetChunkBounds(MigrationChunk chunk)
        {
            BsonValue gte = null;
            BsonValue lt = null;

            // Initialize `gte` and `lt` based on special cases
            if (chunk.Gte.Equals("BsonMaxKey"))
                gte = BsonMaxKey.Value;
            else if (string.IsNullOrEmpty(chunk.Gte))
                gte = BsonNull.Value;

            if (chunk.Lt.Equals("BsonMaxKey"))
                lt = BsonMaxKey.Value;
            else if (string.IsNullOrEmpty(chunk.Lt))
                lt = BsonNull.Value;

            // Handle by DataType
            switch (chunk.DataType)
            {
                case DataType.ObjectId:
                    gte ??= new BsonObjectId(ObjectId.Parse(chunk.Gte));
                    lt ??= new BsonObjectId(ObjectId.Parse(chunk.Lt));
                    break;

                case DataType.Int:
                    gte ??= new BsonInt32(int.Parse(chunk.Gte));
                    lt ??= new BsonInt32(int.Parse(chunk.Lt));
                    break;

                case DataType.Int64:
                    gte ??= new BsonInt64(long.Parse(chunk.Gte));
                    lt ??= new BsonInt64(long.Parse(chunk.Lt));
                    break;

                case DataType.String:
                    gte ??= new BsonString(chunk.Gte);
                    lt ??= new BsonString(chunk.Lt);
                    break;

                case DataType.Object:
                    gte ??= BsonDocument.Parse(chunk.Gte);
                    lt ??= BsonDocument.Parse(chunk.Lt);
                    break;

                case DataType.Decimal128:
                    gte ??= new BsonDecimal128(Decimal128.Parse(chunk.Gte));
                    lt ??= new BsonDecimal128(Decimal128.Parse(chunk.Lt));
                    break;

                case DataType.Date:
                    gte ??= new BsonDateTime(DateTime.Parse(chunk.Gte));
                    lt ??= new BsonDateTime(DateTime.Parse(chunk.Lt));
                    break;

                case DataType.UUID:
                    gte ??= new BsonBinaryData(Guid.Parse(chunk.Gte).ToByteArray(), BsonBinarySubType.UuidStandard);
                    lt ??= new BsonBinaryData(Guid.Parse(chunk.Lt).ToByteArray(), BsonBinarySubType.UuidStandard);
                    break;

                default:
                    throw new ArgumentException($"Unsupported data type: {chunk.DataType}");
            }

            return (gte, lt);
        }
    }


}
