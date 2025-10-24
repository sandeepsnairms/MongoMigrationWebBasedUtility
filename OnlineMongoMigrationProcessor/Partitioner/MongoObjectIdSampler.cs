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
        public async Task<ObjectIdRange> GetObjectIdRangeAsync(BsonDocument filter)
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
        /// Generates N equidistant ObjectIds between the min and max ObjectIds in the collection
        /// and returns them as BsonValue.
        /// </summary>
        public async Task<List<BsonValue>> GenerateEquidistantObjectIdsAsync(int count, BsonDocument filter)
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

                result.Add(new ObjectId(nextBytes)); // ObjectId is a BsonValue
            }

            return result;
        }

    }
}
