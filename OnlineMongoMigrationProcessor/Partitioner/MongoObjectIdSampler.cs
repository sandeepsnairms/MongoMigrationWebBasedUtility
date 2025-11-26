using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Numerics;
using System.Text;
using System.Threading.Tasks;

namespace OnlineMongoMigrationProcessor.Partitioner
{
    public class ObjectIdRange
    {
        public ObjectId MinId { get; set; }
        public ObjectId MaxId { get; set; }
        public DateTime MinTimestamp => MinId.CreationTime;
        public DateTime MaxTimestamp => MaxId.CreationTime;
    }

    public class MongoObjectIdSampler
    {
        private readonly IMongoCollection<BsonDocument> _collection;
        private readonly int _timeoutSeconds;

        public MongoObjectIdSampler(IMongoCollection<BsonDocument> collection, int timeoutSeconds = 60000)
        {
            _collection = collection ?? throw new ArgumentNullException(nameof(collection));
            _timeoutSeconds = timeoutSeconds;
        }

        /// <summary>
        /// Retrieves the smallest and largest ObjectId in the collection with extended timeout.
        /// </summary>
        private async Task<ObjectIdRange> GetObjectIdRangeAsync(BsonDocument? filter)
        {
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(_timeoutSeconds));

            // Smallest ObjectId
            var minDoc = await _collection
                .Find(filter)
                .Sort(Builders<BsonDocument>.Sort.Ascending("_id"))
                .Limit(1)
                .FirstOrDefaultAsync(cts.Token);

            // Largest ObjectId
            var maxDoc = await _collection
                .Find(FilterDefinition<BsonDocument>.Empty)
                .Sort(Builders<BsonDocument>.Sort.Descending("_id"))
                .Limit(1)
                .FirstOrDefaultAsync(cts.Token);

            if (minDoc == null || maxDoc == null)
                throw new InvalidOperationException("Collection is empty or inaccessible.");

            return new ObjectIdRange
            {
                MinId = minDoc["_id"].AsObjectId,
                MaxId = maxDoc["_id"].AsObjectId
            };
        }


        /// <summary>
        /// Generates time-based equidistant ObjectIds, then validates and adjusts ranges to ensure 
        /// each range has 1K-1M records. Returns empty list if total records < 1K.
        /// </summary>
        public async Task<List<BsonValue>> GenerateEquidistantObjectIdsAsync(int count, BsonDocument filter, MigrationSettings settings)
        {
            const int MIN_RECORDS_PER_RANGE = 1000;
            const int MAX_RECORDS_PER_RANGE = 1000000;
            

            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(_timeoutSeconds));

            // Get total count first
            var totalCount = await _collection.CountAsync(filter, cancellationToken: cts.Token);

            // Return empty list if total records < 1K
            if (totalCount < MIN_RECORDS_PER_RANGE)
            {
                return new List<BsonValue>();
            }

            // Use pagination to directly sample ObjectIds at regular intervals
            if (settings.ObjectIdPartitioner == PartitionerType.UsePagination)
            {
                long pageSize = totalCount / count;
                return await GeneratePaginationBasedBoundaries(filter, totalCount, pageSize, cts.Token);
            }

            // Generate initial time-based equidistant boundaries
            var timeBased = await GenerateTimeBasedBoundaries(count, filter, cts.Token);

            if (timeBased.Count < 2)
            {
                return timeBased;
            }

            if (settings.ObjectIdPartitioner == PartitionerType.UseAdjustedTimeBoundaries)
            {
                // Validate and adjust ranges based on actual record counts
                var adjusted = await ValidateAndAdjustRanges(timeBased, filter, totalCount, MIN_RECORDS_PER_RANGE, MAX_RECORDS_PER_RANGE, cts.Token);
                return adjusted;
            }
            else
            {
                return timeBased;
            }
        }

        /// <summary>
        /// Generates time-based equidistant ObjectIds (original algorithm).
        /// </summary>
        private async Task<List<BsonValue>> GenerateTimeBasedBoundaries(int count, BsonDocument filter, CancellationToken cancellationToken)
        {
            var result = new List<BsonValue>();
            if (count < 2)
            {
                return result;
            }

            var range = await GetObjectIdRangeAsync(filter);

            var minBytes = range.MinId.ToByteArray();
            var maxBytes = range.MaxId.ToByteArray();

            var minInt = new BigInteger(minBytes, isUnsigned: true, isBigEndian: true);
            var maxInt = new BigInteger(maxBytes, isUnsigned: true, isBigEndian: true);
            var step = (maxInt - minInt) / (count - 1);

            for (int i = 0; i < count; i++)
            {
                var nextInt = minInt + (step * i);
                var nextBytes = nextInt.ToByteArray(isUnsigned: true, isBigEndian: true);

                // Ensure exactly 12 bytes
                if (nextBytes.Length < 12)
                {
                    var padded = new byte[12];
                    Array.Copy(nextBytes, 0, padded, 12 - nextBytes.Length, nextBytes.Length);
                    nextBytes = padded;
                }
                else if (nextBytes.Length > 12)
                {
                    nextBytes = nextBytes[^12..];
                }

                result.Add(new ObjectId(nextBytes));
            }

            return result;
        }

        /// <summary>
        /// Generates boundaries by paginating through the collection and sampling ObjectIds 
        /// at regular record intervals (e.g., every 100K records).
        /// Uses progressive $gt filters to avoid loading all documents into memory.
        /// </summary>
        private async Task<List<BsonValue>> GeneratePaginationBasedBoundaries(
            BsonDocument filter,
            long totalCount,
            long recordsPerRange,
            CancellationToken cancellationToken)
        {
            var result = new List<BsonValue>();
            
            // Calculate number of ranges needed
            int numRanges = (int)Math.Ceiling((double)totalCount / recordsPerRange);
            
            if (numRanges < 2)
            {
                return result; // Not enough data to create ranges
            }

            BsonValue? lastId = null;

            // Sample ObjectIds at regular intervals
            for (int i = 0; i < numRanges; i++)
            {
                FilterDefinition<BsonDocument> rangeFilter;
                
                if (lastId == null)
                {
                    // First iteration: use original filter
                    rangeFilter = filter;
                }
                else
                {
                    // Subsequent iterations: add $gt filter to continue from last ID
                    rangeFilter = Builders<BsonDocument>.Filter.And(
                        filter,
                        Builders<BsonDocument>.Filter.Gt("_id", lastId)
                    );
                }
                
                // Skip to the next boundary position and get the document
                var doc = await _collection
                    .Find(rangeFilter)
                    .Sort(Builders<BsonDocument>.Sort.Ascending("_id"))
                    .Skip((int)recordsPerRange - 1) // Skip to get the boundary document
                    .Limit(1)
                    .FirstOrDefaultAsync(cancellationToken);
                
                if (doc != null && doc.Contains("_id"))
                {
                    lastId = doc["_id"];
                    result.Add(lastId);
                }
                else
                {
                    break; // No more documents
                }
            }

            return result;
        }

        /// <summary>
        /// Validates ranges and merges/splits them to ensure each range has 10K-200K records.
        /// Uses efficient counting without sorting the entire collection.
        /// </summary>
        private async Task<List<BsonValue>> ValidateAndAdjustRanges(
            List<BsonValue> boundaries,
            BsonDocument filter,
            long totalCount,
            int minRecords,
            int maxRecords,
            CancellationToken cancellationToken)
        {
            if (boundaries.Count < 2)
                return boundaries;

            var adjustedBoundaries = new List<BsonValue>();
            var rangeStats = new List<(BsonValue startId, BsonValue endId, long count)>();

            // Step 1: Count records in each initial range
            for (int i = 0; i < boundaries.Count - 1; i++)
            {
                var startId = boundaries[i];
                var endId = boundaries[i + 1];
                
                var rangeFilter = Builders<BsonDocument>.Filter.And(
                    filter,
                    Builders<BsonDocument>.Filter.Gte("_id", startId),
                    Builders<BsonDocument>.Filter.Lt("_id", endId)
                );
                
                var count = await _collection.CountAsync(rangeFilter, cancellationToken: cancellationToken);
                rangeStats.Add((startId, endId, count));
            }

            // Step 2: Merge small ranges and split large ranges
            int idx = 0;
            while (idx < rangeStats.Count)
            {
                var currentRange = rangeStats[idx];
                
                // If range is too small, try to merge with next ranges
                if (currentRange.count < minRecords)
                {
                    long mergedCount = currentRange.count;
                    int mergeEndIdx = idx;
                    
                    // Keep merging until we have enough records or run out of ranges
                    while (mergeEndIdx < rangeStats.Count - 1 && mergedCount < minRecords)
                    {
                        mergeEndIdx++;
                        mergedCount += rangeStats[mergeEndIdx].count;
                    }
                    
                    // Add merged range
                    if (adjustedBoundaries.Count == 0 || !adjustedBoundaries[adjustedBoundaries.Count - 1].Equals(currentRange.startId))
                    {
                        adjustedBoundaries.Add(currentRange.startId);
                    }
                    
                    // Move to next unmerged range
                    idx = mergeEndIdx + 1;
                }
                // If range is too large, split it
                else if (currentRange.count > maxRecords)
                {
                    var splitBoundaries = await SplitLargeRange(
                        currentRange.startId,
                        currentRange.endId,
                        currentRange.count,
                        filter,
                        maxRecords,
                        cancellationToken);
                    
                    adjustedBoundaries.AddRange(splitBoundaries);
                    idx++;
                }
                // Range is just right
                else
                {
                    if (adjustedBoundaries.Count == 0 || !adjustedBoundaries[adjustedBoundaries.Count - 1].Equals(currentRange.startId))
                    {
                        adjustedBoundaries.Add(currentRange.startId);
                    }
                    idx++;
                }
            }

            return adjustedBoundaries;
        }

        /// <summary>
        /// Splits a large range into smaller ranges by generating intermediate time-based boundaries.
        /// </summary>
        private async Task<List<BsonValue>> SplitLargeRange(
            BsonValue startId,
            BsonValue endId,
            long recordCount,
            BsonDocument filter,
            int maxRecords,
            CancellationToken cancellationToken)
        {
            var result = new List<BsonValue> { startId };
            
            // Calculate how many splits we need
            int splits = (int)Math.Ceiling((double)recordCount / maxRecords);
            
            if (splits <= 1)
            {
                return result;
            }

            // Generate intermediate boundaries using time-based calculation
            var startOid = startId.AsObjectId;
            var endOid = endId.AsObjectId;
            
            var minBytes = startOid.ToByteArray();
            var maxBytes = endOid.ToByteArray();
            
            var minInt = new BigInteger(minBytes, isUnsigned: true, isBigEndian: true);
            var maxInt = new BigInteger(maxBytes, isUnsigned: true, isBigEndian: true);
            var step = (maxInt - minInt) / splits;

            for (int i = 1; i < splits; i++)
            {
                var nextInt = minInt + (step * i);
                var nextBytes = nextInt.ToByteArray(isUnsigned: true, isBigEndian: true);

                // Ensure exactly 12 bytes
                if (nextBytes.Length < 12)
                {
                    var padded = new byte[12];
                    Array.Copy(nextBytes, 0, padded, 12 - nextBytes.Length, nextBytes.Length);
                    nextBytes = padded;
                }
                else if (nextBytes.Length > 12)
                {
                    nextBytes = nextBytes[^12..];
                }

                result.Add(new ObjectId(nextBytes));
            }

            return result;
        }

    }
}
