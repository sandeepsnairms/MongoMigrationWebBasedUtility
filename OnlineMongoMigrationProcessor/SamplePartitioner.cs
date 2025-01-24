﻿using MongoDB.Bson;
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
using System.Xml.Linq;
using MongoDB.Bson.IO;

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
        public ChunkBoundaries CreatePartitions(string idField, int chunkCount, DataType dataType, long minDocsPerChunk, out long docCountByType)
        {
            int segmentCount = 1;
            int minDocsPerSegment = 10000;
            long docsInChunk=0;
            int maxSamples = 2000;
            int sampleCount=0;
            int maxSegments = 20;

            Log.AddVerboseMessage($"Count documents before sampling data for {dataType}");
            Log.Save();

            docCountByType = GetDocumentCountByDataType(_collection, idField, dataType);

            if (docCountByType == 0)
            {
                Log.WriteLine($"0 Documents where {idField} is {dataType}");
                Log.Save();
                return null;
            }
            else if (docCountByType < minDocsPerChunk)
            {
                Log.WriteLine($"Document Count where {idField} is {dataType}:{docCountByType} is less than min partiton size.");
                Log.Save();
                sampleCount = 1;
                chunkCount = 1;
            }

            if (chunkCount > maxSamples)
                throw new ArgumentException("Chunk count too large. Retry with larger Chunk Size.");


            //ensuring minimum docs per chunk
            docsInChunk = docCountByType / chunkCount;
            if (docsInChunk < minDocsPerChunk)
            {
                chunkCount = Math.Max(1,(int)Math.Ceiling((double)docCountByType / minDocsPerChunk));
                docsInChunk = docCountByType / chunkCount;
            }            

            // Calculate the number of segments based on the number of documents in the chunk
            if (docsInChunk > minDocsPerSegment)
            {
                segmentCount = (int)Math.Ceiling((double)docsInChunk / minDocsPerSegment);
            }

            // dont allow more than 10 segments
            segmentCount = Math.Min(segmentCount, maxSegments);

            // Calculate sampleCount as segmentCount times the chunkCount
            sampleCount = chunkCount * segmentCount; //used to generate segments in case of non Dump/Restore sceanrio

            // dont allow more samples than maxSamples
            sampleCount = Math.Min(sampleCount, maxSamples);

            // Adjust the number of segments per chunk based on the new sampleCount
            segmentCount = Math.Max(1, sampleCount / chunkCount);
            

            if (chunkCount < 1)
                throw new ArgumentException("Chunk count must be greater than 0.");

            Log.WriteLine($"SampleCount: {sampleCount}, Chunk Count: {chunkCount} where {idField} is {dataType}");
            Log.Save();

            // Step 1: Build the filter pipeline based on the data type

            BsonDocument matchCondition = DataTypeConditionBuilder(dataType, idField);

            // Step 2: Sample the data

            Log.AddVerboseMessage($"Sampling data for {dataType} with {sampleCount} samples");
            Log.Save();

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


            // Step 3: Calculate partition boundaries

            ChunkBoundaries chunkBoundaries= new ChunkBoundaries();

            Boundary segmentBoundary = null;
            Boundary chunkBoundary = null;

            for (int i = 0; i < partitionValues.Count; i++)
            {
                var min = partitionValues[i];
                var max = i == partitionValues.Count - 1 ? BsonMaxKey.Value : partitionValues[i + 1];

                if (i % segmentCount == 0) // Parent boundary
                {
                    chunkBoundary = new Boundary
                    {
                        StartId = min,
                        EndId = max,
                        SegmentBoundaries = new List<Boundary>() // Initialize SegmentBoundaries here
                    };

                    chunkBoundaries.Boundaries ??= new List<Boundary>(); // Use null-coalescing assignment
                    chunkBoundaries.Boundaries.Add(chunkBoundary);
                }
                else // Child boundary
                {
                    if (chunkBoundary == null)
                    {
                        throw new Exception("Parent boundary not found");
                    }

                    segmentBoundary = new Boundary
                    {
                        StartId = min,
                        EndId = max
                    };

                    chunkBoundary.SegmentBoundaries.Add(segmentBoundary);
                    chunkBoundary.EndId = max; // Update the EndId of the parent boundary to match the last segment.
                }


            }



            Log.WriteLine($"Total Chunks: {chunkBoundaries.Boundaries.Count} where {idField} is {dataType}");
            Log.Save();

            return chunkBoundaries;
            
        }


        public static long GetDocumentCountByDataType(IMongoCollection<BsonDocument> collection, string idField, DataType dataType)
        {
            var filterBuilder = Builders<BsonDocument>.Filter;

            BsonDocument matchCondition = DataTypeConditionBuilder(dataType,idField);

            // Get the count of documents matching the filter
            var count = collection.CountDocuments(matchCondition);
            return count;
        }

        public static BsonDocument DataTypeConditionBuilder(DataType dataType, string idField)
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

        public static (BsonValue gte, BsonValue lt) GetChunkBounds(string gteStrring,string ltString, DataType dataType)
        {
            BsonValue gte = null;
            BsonValue lt = null;

            // Initialize `gte` and `lt` based on special cases
            if (gteStrring.Equals("BsonMaxKey"))
                gte = BsonMaxKey.Value;
            else if (string.IsNullOrEmpty(gteStrring))
                gte = BsonNull.Value;

            if (ltString.Equals("BsonMaxKey"))
                lt = BsonMaxKey.Value;
            else if (string.IsNullOrEmpty(ltString))
                lt = BsonNull.Value;

            // Handle by DataType
            switch (dataType)
            {
                case DataType.ObjectId:
                    gte ??= new BsonObjectId(ObjectId.Parse(gteStrring));
                    lt ??= new BsonObjectId(ObjectId.Parse(ltString));
                    break;

                case DataType.Int:
                    gte ??= new BsonInt32(int.Parse(gteStrring));
                    lt ??= new BsonInt32(int.Parse(ltString));
                    break;

                case DataType.Int64:
                    gte ??= new BsonInt64(long.Parse(gteStrring));
                    lt ??= new BsonInt64(long.Parse(ltString));
                    break;

                case DataType.String:
                    gte ??= new BsonString(gteStrring);
                    lt ??= new BsonString(ltString);
                    break;

                case DataType.Object:
                    gte ??= BsonDocument.Parse(gteStrring);
                    lt ??= BsonDocument.Parse(ltString);
                    break;

                case DataType.Decimal128:
                    gte ??= new BsonDecimal128(Decimal128.Parse(gteStrring));
                    lt ??= new BsonDecimal128(Decimal128.Parse(ltString));
                    break;

                case DataType.Date:
                    gte ??= new BsonDateTime(DateTime.Parse(gteStrring));
                    lt ??= new BsonDateTime(DateTime.Parse(ltString));
                    break;

                case DataType.UUID:
                    gte ??= new BsonBinaryData(Guid.Parse(gteStrring).ToByteArray(), BsonBinarySubType.UuidStandard);
                    lt ??= new BsonBinaryData(Guid.Parse(ltString).ToByteArray(), BsonBinarySubType.UuidStandard);
                    break;

                default:
                    throw new ArgumentException($"Unsupported data type: {dataType}");
            }

            return (gte, lt);
        }
    }


}
