using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Driver;
using Newtonsoft.Json;
using OnlineMongoMigrationProcessor.Helpers;
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.IO;

#pragma warning disable CS8618 // Non-nullable field must contain a non-null value when exiting constructor. Consider adding the 'required' modifier or declaring as nullable.

namespace OnlineMongoMigrationProcessor
{
    public class JobList
    {
        public List<MigrationJob>? MigrationJobs { get; set; }
        public int ActiveRestoreProcessId { get; set; } = 0;
        public int ActiveDumpProcessId { get; set; } = 0;
        private string _filePath = string.Empty;
        private static readonly object _fileLock = new object();
        private Log log;

        public JobList()
        {
            if (!Directory.Exists($"{Helper.GetWorkingFolder()}migrationjobs"))
            {
                Directory.CreateDirectory($"{Helper.GetWorkingFolder()}migrationjobs");
            }
            _filePath = $"{Helper.GetWorkingFolder()}migrationjobs\\list.json";
        }

        public void Load(Log log)
        {
            this.log = log;
            try
            {
                if (File.Exists(_filePath))
                {
                    string json = File.ReadAllText(_filePath);
                    var loadedObject = JsonConvert.DeserializeObject<JobList>(json);
                    if (loadedObject != null)
                    {
                        MigrationJobs = loadedObject.MigrationJobs;
                    }
                }
            }
            catch (Exception ex)
            {
                log.WriteLine($"Error loading data: {ex.ToString()}");
            }
        }

        public bool Save()
        {
            try
            {
                lock (_fileLock)
                {
                    string json = JsonConvert.SerializeObject(this);
                    string tempFile = _filePath + ".tmp";
                    string backupFile = _filePath + ".bak";

                    // Write to temp file
                    File.WriteAllText(tempFile, json);

                    // If current file exists and MigrationJobs is not empty, back it up
                    bool hasJobs = this.MigrationJobs != null && this.MigrationJobs.Count > 0;
                    if (File.Exists(_filePath) && hasJobs)
                    {
                        File.Copy(_filePath, backupFile, overwrite: true);
                    }

                    // Move temp file to actual file (atomic operation)
                    File.Move(tempFile, _filePath, overwrite: true);
                }

                return true;
            }
            catch (Exception ex)
            {
                log.WriteLine($"Error saving data: {ex}", LogType.Error);
                return false;
            }
        }

    }

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
        public bool CurrentlyActive { get; set; }
        public bool UseMongoDump { get; set; }
        public bool IsSimulatedRun { get; set; }
        public bool SkipIndexes { get; set; }
        public bool AppendMode { get; set; }
        public bool SyncBackEnabled { get; set; }
        public bool ProcessingSyncBack { get; set; }
        public bool CSStartsAfterAllUploads { get; set; }
        public bool CSPostProcessingStarted { get; set; }
        public List<MigrationUnit>? MigrationUnits { get; set; }
    }


    public enum CollectionStatus
    {
        Unknown,
        OK,
        NotFound        
    }

    public class MigrationUnit
    {
        public string DatabaseName { get; set; }
        public string CollectionName { get; set; }
        public string? ResumeToken { get; set; }
        public ChangeStreamOperationType ResumeTokenOperation { get; set; }

        [JsonProperty("ResumeDocumentId")]
        private object? _resumeDocumentIdRaw { get; set; }

        [JsonIgnore]
        public BsonValue? ResumeDocumentId
        {
            get => _resumeDocumentIdRaw != null ? BsonValue.Create(_resumeDocumentIdRaw) : null;
            set => _resumeDocumentIdRaw = value;
        }

        public DateTime? ChangeStreamStartedOn { get; set; }
        public DateTime CursorUtcTimestamp { get; set; }
        public long CSUpdatesInLastBatch { get; set; }
        public long CSNormalizedUpdatesInLastBatch { get; set; }
        public int CSLastBatchDurationSeconds { get; set; }

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
        public long DumpGap { get; set; }
        public long RestoreGap { get; set; }
        public List<MigrationChunk> MigrationChunks { get; set; }

        public MigrationUnit(string databaseName, string collectionName, List<MigrationChunk> migrationChunks)
        {
            DatabaseName = databaseName;
            CollectionName = collectionName;
            MigrationChunks = migrationChunks;
        }
    }

    public class LogObject
    {
        public LogObject(LogType type, string message)
        {
            Message = message;
            Type = type;
            Datetime = DateTime.UtcNow;
        }

        public string Message { get; set; }
        public LogType Type { get; set; }
        public DateTime Datetime { get; set; }
    }

    public class Boundary
    {
        public BsonValue? StartId { get; set; }
        public BsonValue? EndId { get; set; }
        public List<Boundary> SegmentBoundaries { get; set; }
    }

    public class ChunkBoundaries
    {
        public List<Boundary> Boundaries { get; set; }
    }

    public class MigrationSettings
    {
        public string? CACertContentsForSourceServer { get; set; }
        public string? MongoToolsDownloadUrl { get; set; }
        public bool ReadBinary { get; set; }
        public long ChunkSizeInMb { get; set; }
        public int ChangeStreamMaxDocsInBatch { get; set; }
		public int ChangeStreamBatchDuration { get; set; }
		public int ChangeStreamMaxCollsInBatch { get; set; }
		public int MongoCopyPageSize { get; set; }
        private string _filePath = string.Empty;
        private Log log;

        public MigrationSettings(Log log)
        {
            _filePath = $"{Helper.GetWorkingFolder()}migrationjobs\\config.json";
            this.log = log;
        }

        public void Load()
        {
            bool initialized = false;
            if (File.Exists(_filePath))
            {
                string json = File.ReadAllText(_filePath);
                var loadedObject = JsonConvert.DeserializeObject<MigrationSettings>(json);
                if (loadedObject != null)
                {
                    ReadBinary = loadedObject.ReadBinary;
                    MongoToolsDownloadUrl = loadedObject.MongoToolsDownloadUrl;
                    ChunkSizeInMb = loadedObject.ChunkSizeInMb;
					ChangeStreamMaxDocsInBatch = loadedObject.ChangeStreamMaxDocsInBatch == 0 ? 10000 : loadedObject.ChangeStreamMaxDocsInBatch;
					ChangeStreamBatchDuration = loadedObject.ChangeStreamBatchDuration == 0 ? 1 : loadedObject.ChangeStreamBatchDuration;
					ChangeStreamMaxCollsInBatch = loadedObject.ChangeStreamMaxCollsInBatch == 0 ? 5 : loadedObject.ChangeStreamMaxCollsInBatch;
					MongoCopyPageSize = loadedObject.MongoCopyPageSize;
                    CACertContentsForSourceServer = loadedObject.CACertContentsForSourceServer;
                    initialized = true;
                    if (ChangeStreamMaxDocsInBatch > 10000)
                        ChangeStreamMaxDocsInBatch = 10000;
                }
            }
            if (!initialized)
            {
                ReadBinary = false;
                MongoToolsDownloadUrl = "https://fastdl.mongodb.org/tools/db/mongodb-database-tools-windows-x86_64-100.10.0.zip";
                ChunkSizeInMb = 5120;
				MongoCopyPageSize = 500;
				ChangeStreamMaxDocsInBatch = 10000;                
                ChangeStreamBatchDuration = 1;
                ChangeStreamMaxCollsInBatch = 5;
                CACertContentsForSourceServer = string.Empty;
            }
        }

        public bool Save()
        {
            try
            {
                string json = JsonConvert.SerializeObject(this);
                File.WriteAllText(_filePath, json);
                return true;
            }
            catch (Exception ex)
            {
                log.WriteLine($"Error saving data: {ex.ToString()}", LogType.Error);
                return false;
            }
        }
    }

    public enum LogType
    {
        Error,
        Message
    }

    public class Segment
    {
        public string? Lt { get; set; }
        public string? Gte { get; set; }
        public bool? IsProcessed { get; set; }
        public long QueryDocCount { get; set; }
        public string Id { get; set; }
    }

    public class MigrationChunk
    {
        public string? Lt { get; set; }
        public string? Gte { get; set; }
        public bool? IsDownloaded { get; set; }
        public bool? IsUploaded { get; set; }
        public long DumpQueryDocCount { get; set; }
        public long DumpResultDocCount { get; set; }
        public long RestoredSuccessDocCount { get; set; }
        public long RestoredFailedDocCount { get; set; }
        public long DocCountInTarget { get; set; }
        public long SkippedAsDuplicateCount { get; set; }
        public DataType DataType { get; set; }
        public List<Segment> Segments { get; set; }


        public MigrationChunk(string startId, string endId, DataType dataType, bool? downloaded, bool? uploaded)
        {
            Gte = startId;
            Lt = endId;
            IsDownloaded = downloaded;
            IsUploaded = uploaded;
            DataType = dataType;
        }
    }

    public class ChnageStreamsDocuments
    {
        public List<ChangeStreamDocument<BsonDocument>> DocsToBeInserted = new List<ChangeStreamDocument<BsonDocument>>();
        public List<ChangeStreamDocument<BsonDocument>> DocsToBeUpdated = new List<ChangeStreamDocument<BsonDocument>>();
        public List<ChangeStreamDocument<BsonDocument>> DocsToBeDeleted = new List<ChangeStreamDocument<BsonDocument>>();
    }

    public enum DataType
    {
        ObjectId,
        Int,
        Int64,
        Decimal128,
        Date,
        Binary,
        String,
        Object,
        Other
    }
}

