using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Driver;
using Newtonsoft.Json;
using OnlineMongoMigrationProcessor.Helpers;
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.IO;
using System.Xml.Linq;

#pragma warning disable CS8618 // Non-nullable field must contain a non-null value when exiting constructor. Consider adding the 'required' modifier or declaring as nullable.

namespace OnlineMongoMigrationProcessor
{
    public class JobList
    {
        public List<MigrationJob>? MigrationJobs { get; set; }
        public int ActiveRestoreProcessId { get; set; } = 0;
        public int ActiveDumpProcessId { get; set; } = 0;
        private string _filePath = string.Empty;
        private string _backupFolderPath = string.Empty;
        private static readonly object _fileLock = new object();
        private static readonly object _loadLock = new object();
        private Log log;
        private string processedMin = string.Empty;

        private const int TUMBLING_INTERVAL_MINUTES = 5;

        private readonly string[] SlotNames =
        { "backup_slot0.json", "backup_slot1.json", "backup_slot2.json", "backup_slot3.json" };

        public JobList()
        {
            if (!Directory.Exists($"{Helper.GetWorkingFolder()}migrationjobs"))
            {
                Directory.CreateDirectory($"{Helper.GetWorkingFolder()}migrationjobs");
            }
            _filePath = $"{Helper.GetWorkingFolder()}migrationjobs\\list.json";
            _backupFolderPath = $"{Helper.GetWorkingFolder()}migrationjobs\\";

        }

        public void SetLog(Log log)
        {
            this.log = log;
        }

        public bool LoadJobs(out string errorMessage,bool loadBackup= false)
        {
            errorMessage = string.Empty;
            lock (_loadLock)
            {
                string path;

                path = loadBackup ? GetBestRestoreSlotFilePath() : _filePath;

                if (path == null || !File.Exists(path))
                {
                    errorMessage = "No suitable backup file found for restoration.";
                    return false;
                }

                //this.log = log;
                try
                {
                    if (File.Exists(path))
                    {
                        string json = File.ReadAllText(path);
                        var loadedObject = JsonConvert.DeserializeObject<JobList>(json);
                        if (loadedObject != null)
                        {
                            MigrationJobs = loadedObject.MigrationJobs;

                            if (loadBackup)
                            {
                                //delete all files in SlotNames
                                foreach (string name in SlotNames)
                                {
                                    if (System.IO.File.Exists(Path.Combine(_backupFolderPath, name)))
                                        System.IO.File.Delete(Path.Combine(_backupFolderPath, name));
                                }
                                Save(out errorMessage,true);
                            }
                        }
                    }
                    errorMessage= string.Empty;
                    return true;
                }
                catch (Exception ex)
                {
                    errorMessage = $"Error loading data: {ex.ToString()}";
                    return false;
                }
            }
        }


        private string? GetBestRestoreSlotFilePath()
        {
            DateTime now = DateTime.Now;
            DateTime minAllowedTime = now.AddMinutes(-1 * TUMBLING_INTERVAL_MINUTES * (SlotNames.Length-1));

            var backupFolder = _backupFolderPath; // Replace with your folder path
            var slotFiles = SlotNames
                .Select(name => Path.Combine(backupFolder, name))
                .Where(File.Exists)
                .Select(filePath => new
                {
                    Path = filePath,
                    Timestamp = File.GetLastWriteTime(filePath)
                })                
                .OrderBy(f => f.Timestamp) // Sort ascending
                .ToList();


            string ? latestFile = string.Empty;
            for (int i = slotFiles.Count - 1; i >= 0; i--)
            {
                if (slotFiles[i].Timestamp < minAllowedTime)
                {
                    latestFile = slotFiles[i].Path;
                    break;
                }
            }

            if(slotFiles.Count>0 && latestFile == string.Empty)
                latestFile = slotFiles.First().Path;

            if (latestFile != string.Empty)
                return backupFolder.Any() ? latestFile : null;
            else
                return string.Empty;

        }

        public DateTime GetBackupDate()
        {
            var path = GetBestRestoreSlotFilePath();

            var backupDataUpdatedOn= File.Exists(path) ? File.GetLastWriteTimeUtc(path) : DateTime.MinValue;
            return backupDataUpdatedOn;
        }

        public bool Save()
        {
           return Save(out string errorMessage);
        }

        public bool Save(out string errorMessage, bool forceBackup=false)
        {
            try
            {
                lock (_fileLock)
                {
                    string json = JsonConvert.SerializeObject(this);
                    string tempFile = _filePath + ".tmp";

                    // Step 1: Write JSON to temp
                    File.WriteAllText(tempFile, json);

                    //atomic rewrite
                    File.Move(tempFile, _filePath, overwrite: true);

                    DateTime now = DateTime.UtcNow;

                    bool hasJobs = this.MigrationJobs != null && this.MigrationJobs.Count > 0;

                    if (File.Exists(_filePath) && hasJobs && (processedMin != now.ToString("MM/dd/yyyy HH:mm")|| forceBackup))
                    {

                        // Rotate every 15 minutes
                        if (now.Minute % TUMBLING_INTERVAL_MINUTES == 0 || forceBackup)
                        {

                            //set processed minute               
                            processedMin = now.ToString("MM/dd/yyyy HH:mm");

                            int slotIndex = (now.Minute / TUMBLING_INTERVAL_MINUTES) % SlotNames.Length; // 0, 1, 2, or 3

                            // Step 3: Write new backup into slot
                            string latestSlot = Path.Combine(_backupFolderPath, SlotNames[slotIndex]);
                            File.Copy(_filePath, latestSlot, overwrite: true);
                        }
                    }
                    errorMessage = string.Empty;
                    return true;
                }
            }
            catch (Exception ex)
            {
                errorMessage = $"Error saving data: {ex}";
                log?.WriteLine(errorMessage, LogType.Error);
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
        public string? OriginalResumeToken { get; set; }

        public bool InitialDocumenReplayed { get; set; } = false;
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

    public class MigrationSettings : ICloneable
    {
      
        public string? CACertContentsForSourceServer { get; set; }
        public string? MongoToolsDownloadUrl { get; set; }
        public bool ReadBinary { get; set; }
        public long ChunkSizeInMb { get; set; }
        public int ChangeStreamMaxDocsInBatch { get; set; }
		public int ChangeStreamBatchDuration { get; set; }
        public int ChangeStreamBatchDurationMin { get; set; }
        public int ChangeStreamMaxCollsInBatch { get; set; }
		public int MongoCopyPageSize { get; set; }
        private string _filePath = string.Empty;

        public MigrationSettings()
        {
            _filePath = $"{Helper.GetWorkingFolder()}migrationjobs\\config.json";
        }

        public object Clone()
        {
            // Deep clone using JSON serialization
            var json = JsonConvert.SerializeObject(this);
            return JsonConvert.DeserializeObject<MigrationSettings>(json);
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
					ChangeStreamBatchDuration = loadedObject.ChangeStreamBatchDuration == 0 ? 120 : loadedObject.ChangeStreamBatchDuration;
                    ChangeStreamBatchDurationMin = loadedObject.ChangeStreamBatchDurationMin == 0 ? 30 : loadedObject.ChangeStreamBatchDurationMin;
                    ChangeStreamMaxCollsInBatch = loadedObject.ChangeStreamMaxCollsInBatch == 0 ? 5 : loadedObject.ChangeStreamMaxCollsInBatch;
					MongoCopyPageSize = loadedObject.MongoCopyPageSize;
                    CACertContentsForSourceServer = loadedObject.CACertContentsForSourceServer;
                    initialized = true;
                    if (ChangeStreamMaxDocsInBatch > 10000)
                        ChangeStreamMaxDocsInBatch = 10000;
                    if (ChangeStreamBatchDuration < 30)
                        ChangeStreamBatchDuration = 120;
                }
            }
            if (!initialized)
            {
                ReadBinary = false;
                MongoToolsDownloadUrl = "https://fastdl.mongodb.org/tools/db/mongodb-database-tools-windows-x86_64-100.10.0.zip";
                ChunkSizeInMb = 5120;
				MongoCopyPageSize = 500;
				ChangeStreamMaxDocsInBatch = 10000;
                ChangeStreamBatchDuration = 120;
                ChangeStreamBatchDurationMin = 30;
                ChangeStreamMaxCollsInBatch = 5;
                CACertContentsForSourceServer = string.Empty;
            }
        }

        public bool Save(out string errorMessage)
        {
            try
            {
                string json = JsonConvert.SerializeObject(this);
                File.WriteAllText(_filePath, json);
                errorMessage=string.Empty;
                return true;
            }
            catch (Exception ex)
            {
                errorMessage= $"Error saving data: {ex.ToString()}";    
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

