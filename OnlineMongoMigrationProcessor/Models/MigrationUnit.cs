using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Driver;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using static System.Runtime.InteropServices.JavaScript.JSType;

#pragma warning disable CS8618 // Non-nullable field must contain a non-null value when exiting constructor. Consider adding the 'required' modifier or declaring as nullable.

namespace OnlineMongoMigrationProcessor
{
    public class MigrationUnit
    {
        public string DatabaseName { get; set; }
        public string CollectionName { get; set; }
        public string? ResumeToken { get; set; }
        public string? OriginalResumeToken { get; set; }

        public bool InitialDocumenReplayed { get; set; } = false;
        public ChangeStreamOperationType ResumeTokenOperation { get; set; }

        [JsonProperty("ResumeDocumentId")]
        private object? _resumeDocumentIdRaw { get; set; }

        [JsonIgnore]
        public BsonDocument? ResumeDocumentId
        {
            get
            {
                if (_resumeDocumentIdRaw == null)
                    return null;
                var rawString = _resumeDocumentIdRaw.ToString();
                if (string.IsNullOrEmpty(rawString))
                    return null;
                return getResumeDocumentId(rawString);
            }
            set => _resumeDocumentIdRaw = value;
        }

        private BsonDocument getResumeDocumentId(string raw)
        {
            JToken token = JToken.Parse(raw);


            if (token is JArray jArr)
            {
                // Convert array of {Name,Value} to doc
                return new BsonDocument(
                    jArr.Select(j =>
                        new BsonElement(j["Name"]!.ToString(), BsonValue.Create(j["Value"]!.ToString()))
                    )
                );
            }
            else if (token is JObject jObj)
            {
                // Convert single object {Name,Value} to doc
                return new BsonDocument
                {
                    { jObj["Name"]!.ToString(), BsonValue.Create(jObj["Value"]!.ToString()) }
                };
            }
            else
                return new BsonDocument();
            
        }

        public DateTime? BulkCopyStartedOn { get; set; }
        public DateTime? BulkCopyEndedOn { get; set; }
        public int IndexesMigrated { get; set; }

        public DateTime? ComparedOn { get; set; }
        public int VarianceCount { get; set; }

        public DateTime? ChangeStreamStartedOn { get; set; }
        public DateTime CursorUtcTimestamp { get; set; }
        public long CSUpdatesInLastBatch { get; set; }
        public long CSNormalizedUpdatesInLastBatch { get; set; }
        public int CSLastBatchDurationSeconds { get; set; }

        public string? UserFilter { get; set; }
        public string? SyncBackResumeToken { get; set; }
        public DateTime? SyncBackChangeStreamStartedOn { get; set; }
        public DateTime SyncBackCursorUtcTimestamp { get; set; }

        public double DumpPercent { get; set; }
        public double RestorePercent { get; set; }
        public bool DumpComplete { get; set; }
        public bool RestoreComplete { get; set; }
        public bool ResetChangeStream { get; set; }
        public long EstimatedDocCount { get; set; }
        public CollectionStatus SourceStatus { get; set; }
        public long ActualDocCount { get; set; }
        public long SourceCountDuringCopy { get; set; }
        public long DumpGap { get; set; }
        public long RestoreGap { get; set; }

        public long CSDInsertEvents { get; set; }
        public long CSDeleteEvents { get; set; }
        public long CSUpdateEvents { get; set; }
        public long CSErrors { get; set; }

        public long CSDocsInserted { get; set; }
        public long CSDocsDeleted { get; set; }
        public long CSDocsUpdated { get; set; }
        public long CSDuplicateDocsSkipped { get; set; }

        public long SyncBackInsertEvents { get; set; }
        public long SyncBackDeleteEvents { get; set; }
        public long SyncBackUpdateEvents { get; set; }
        public long SyncBackErrors { get; set; }

        public long SyncBackDocsInserted { get; set; }
        public long SyncBackDocsDeleted { get; set; }
        public long SyncBackDocsUpdated { get; set; }
        public long SyncBackDuplicateDocsSkipped { get; set; }

        public DataType? DataTypeFor_Id { get; set; } = null;

        public List<MigrationChunk> MigrationChunks { get; set; }

        public MigrationUnit(string databaseName, string collectionName, List<MigrationChunk> migrationChunks)
        {
            DatabaseName = databaseName;
            CollectionName = collectionName;
            MigrationChunks = migrationChunks;
        }
    }
}