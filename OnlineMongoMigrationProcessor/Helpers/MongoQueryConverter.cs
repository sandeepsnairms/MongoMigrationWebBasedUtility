using MongoDB.Bson;
using MongoDB.Bson.IO;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Text.RegularExpressions;

namespace OnlineMongoMigrationProcessor.Helpers
{
    /// <summary>
    /// Converts relaxed MongoDB queries into strict Extended JSON for mongodump.
    /// </summary>
    public static class MongoQueryConverter
    {

        public static string ConvertMondumpFilter(string query, BsonValue? gte, BsonValue? lte, DataType dataType)
        {
            if(dataType!= DataType.Object )
            {
                return query;
            }
            else
            {
                return CreateMongoDumpFilter(gte, lte);
            }

        }


        public static string CreateMongoDumpFilter(BsonValue? gte, BsonValue? lt)
        {
            var ops = new List<string>();
            if (gte is BsonDocument gteDoc)
                ops.Add($"\"$gte\": {gteDoc.ToJson()}");
            if (lt is BsonDocument ltDoc)
                ops.Add($"\"$lt\": {ltDoc.ToJson()}");
            var criteria = $"{{ {string.Join(", ", ops)} }}";
            var filter = $"{{ \"_id\": {criteria} }}";

            // Escape all double quotes for safe use in shell command strings
            return filter.Replace("\"", "\\\"");
        }






    }
}