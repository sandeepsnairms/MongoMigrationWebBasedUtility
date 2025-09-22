using Newtonsoft.Json;
using OnlineMongoMigrationProcessor.Models;
using MongoDB.Driver;
using System;
using System.Collections.Generic;

#pragma warning disable CS8618 // Non-nullable field must contain a non-null value when exiting constructor. Consider adding the 'required' modifier or declaring as nullable.

namespace OnlineMongoMigrationProcessor
{
    public class MigrationJob
    {
        public string? Id { get; set; }
        public string? Name { get; set; }
        public string? SourceEndpoint { get; set; }
        public string? TargetEndpoint { get; set; }
        [JsonIgnore]
        public string? SourceConnectionString { get; set; }
        [JsonIgnore]
        public string? TargetConnectionString { get; set; }
        public string? SourceServerVersion { get; set; }
        public string? NameSpaces { get; set; }
        public DateTime? StartedOn { get; set; }
        public bool IsCompleted { get; set; }
        public bool IsOnline { get; set; }
        public bool IsCancelled { get; set; }
        public bool IsStarted { get; set; }
        public JobType JobType { get; set; } = JobType.MongoDriver;
        
        // Legacy property for backward compatibility - will be removed in future versions
        // This will only be deserialized if present in JSON, but never serialized
        [JsonProperty("UseMongeDump", DefaultValueHandling = DefaultValueHandling.Ignore)]
        private bool? _useMongoDumpLegacy
        {
            get => null; // Never serialize this
            set
            {
                // Handle deserialization of legacy UseMongoDump property
                if (value.HasValue)
                {
                    JobType = value.Value ? JobType.DumpAndRestore : JobType.MongoDriver;
                }
            }
        }
        
        public bool IsSimulatedRun { get; set; }
        public bool SkipIndexes { get; set; }
        public bool AppendMode { get; set; }
        public bool SyncBackEnabled { get; set; }
        public bool ProcessingSyncBack { get; set; }
        public bool RunComparison { get; set; }
        public bool AggresiveChangeStream { get; set; }
        public bool CSStartsAfterAllUploads { get; set; }
        public bool CSPostProcessingStarted { get; set; }
        public ChangeStreamLevel ChangeStreamLevel { get; set; }
        
        // Global resume token properties for server-level change streams (Forward sync)
        public string? ResumeToken { get; set; }
        public string? OriginalResumeToken { get; set; }
        public bool InitialDocumenReplayed { get; set; } = false;
        public ChangeStreamOperationType ResumeTokenOperation { get; set; }
        public string? ResumeDocumentId { get; set; }
        public string? ResumeCollectionKey { get; set; } // CollectionKey (database.collection) for auto replay
        public DateTime? ChangeStreamStartedOn { get; set; }
        public DateTime CursorUtcTimestamp { get; set; }

        // Global resume token properties for server-level change streams (Sync back)
        public string? SyncBackResumeToken { get; set; }
        public string? SyncBackOriginalResumeToken { get; set; }
        public bool SyncBackInitialDocumenReplayed { get; set; } = false;
        public ChangeStreamOperationType SyncBackResumeTokenOperation { get; set; }
        public string? SyncBackResumeDocumentId { get; set; }
        public string? SyncBackResumeCollectionKey { get; set; } // CollectionKey (database.collection) for sync back auto replay
        public DateTime? SyncBackChangeStreamStartedOn { get; set; }
        public DateTime SyncBackCursorUtcTimestamp { get; set; }

        public List<MigrationUnit>? MigrationUnits { get; set; }
    }
}