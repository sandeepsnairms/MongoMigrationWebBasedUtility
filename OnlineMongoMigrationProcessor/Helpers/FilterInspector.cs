using MongoDB.Bson;
using System;
using System.Text.Json.Nodes;
namespace OnlineMongoMigrationProcessor
{
    public static class FilterInspector
    {
        /// <summary>
        /// Returns true if the JSON filter uses _id and only $gte/$lt operators for it.
        /// Returns false if _id not used or invalid operators present.
        /// </summary>
        public static bool HasValidIdFilter(string filterJson)
        {
            if (string.IsNullOrWhiteSpace(filterJson))
                return false;

            try
            {
                var doc = BsonDocument.Parse(filterJson); // handles unquoted $lt/$gte
                return CheckNodeForId(doc);
            }
            catch
            {
                return false;
            }
        }

        private static bool CheckNodeForId(BsonDocument doc)
        {

            // Direct _id
            if (doc.Contains("_id"))
            {
                if (!doc["_id"].IsBsonDocument)
                    return false; // _id used as scalar

                var idDoc = doc["_id"].AsBsonDocument;
                foreach (var kv in idDoc)
                {
                    if (kv.Name.StartsWith("$") && kv.Name != "$gte" && kv.Name != "$lt")
                        return false; // invalid operator
                }
            }

            // Recurse into nested documents/arrays
            foreach (var element in doc)
            {
                if (element.Value.IsBsonDocument && !CheckNodeForId(element.Value.AsBsonDocument))
                    return false;

                if (element.Value.IsBsonArray)
                {
                    foreach (var item in element.Value.AsBsonArray)
                    {
                        if (item.IsBsonDocument && !CheckNodeForId(item.AsBsonDocument))
                            return false;
                    }
                }
            }

            // If _id not found anywhere, return true
            return true;
        }
    }

}
