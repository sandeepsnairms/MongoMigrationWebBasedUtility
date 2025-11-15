using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using OnlineMongoMigrationProcessor.Helpers;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;

#pragma warning disable CS8618 // Non-nullable field must contain a non-null value when exiting constructor. Consider adding the 'required' modifier or declaring as nullable.

namespace OnlineMongoMigrationProcessor
{
    public class JobList
    {
        // Legacy property for backward compatibility - will be removed in future versions
        // This will only be deserialized if present in JSON, but never serialized
        [JsonProperty("MigrationJobs")]
        private List<MigrationJob>? _migrationJobsBackingField
        {
            get => null; // Never serialize this property - returns null so JSON.NET won't include it
            set => MigrationJobs = value; // Allow deserialization - set the public property
        }

        [JsonIgnore]
        public List<MigrationJob>? MigrationJobs { get; set; }

        public List<string>? MigrationJobIds { get; set; }
        
        
              

        //private string _filePath = string.Empty;
        //private string _backupFolderPath = string.Empty;
        private static readonly object _writeLock = new object();
        private static readonly object _loadLock = new object();
        private Log _log;
        //private string _processedMin = string.Empty;

        //private const int TUMBLING_INTERVAL_MINUTES = 5;

        //private readonly string[] SlotNames =
        //{ "backup_slot0.json", "backup_slot1.json", "backup_slot2.json", "backup_slot3.json" };


        public class ConnectionAccessor
        {
            private readonly Dictionary<string, string> _dict;

            public ConnectionAccessor(Dictionary<string, string> dict)
            {
                _dict = dict;
            }

            // Indexer to get/set by jobId
            public string this[string jobId]
            {
                get => _dict.TryGetValue(jobId, out var value) ? value : null;
                set => _dict[jobId] = value;
            }

            // Add this property to expose dictionary keys
            public IEnumerable<string> Keys => _dict.Keys;
        }

        public JobList()
        {
            Helper.CreateFolderIfNotExists($"{Helper.GetWorkingFolder()}migrationjobs");
        }        


        public bool Persist()
        {
            lock (_writeLock)
            {
                var filePath = $"{Helper.GetWorkingFolder()}migrationjobs\\joblist.json";

                string json = JsonConvert.SerializeObject(this, Formatting.Indented);

                return Helper.WriteAtomicFile(filePath, json);
            }
        }

        public void SetLog(Log _log)
        {
            this._log = _log;
        }


        public bool LoadJobList(out string errorMessage)
        {
            errorMessage = string.Empty;

            lock (_loadLock)
            {

                string oldFormatPath = $"{Helper.GetWorkingFolder()}migrationjobs\\list.json";
                string newFormatPath = $"{Helper.GetWorkingFolder()}migrationjobs\\joblist.json";

                try
                {
                    int max = 5;
                    if (!File.Exists(oldFormatPath) && !File.Exists(newFormatPath))
                    {
                        errorMessage = "No suitable file found.";
                        return false;
                    }
                    else
                    {
                        for (int i = 0; i < max; i++) 
                        {
                            try
                            {
                                //convert old format to new format
                                if (File.Exists(oldFormatPath))
                                {
                                    if (ConvertToNewFormat(oldFormatPath, out errorMessage))
                                    {
                                        File.Move(oldFormatPath, oldFormatPath + ".bak");
                                    }
                                }

                                //load new format
                                if (!File.Exists(newFormatPath))
                                {
                                    errorMessage = "No suitable file in new format found.";
                                    return false;
                                }

                                using var fs = new FileStream(
                                    newFormatPath,
                                    FileMode.Open,
                                    FileAccess.Read,
                                    FileShare.ReadWrite | FileShare.Delete);
                                using var sr = new StreamReader(fs);
                                string json = sr.ReadToEnd();
                                var loadedObject = JsonConvert.DeserializeObject<JobList>(json);
                                if (loadedObject != null)
                                {
                                    MigrationJobs = loadedObject.MigrationJobs;
                                    MigrationJobIds = loadedObject.MigrationJobIds;
                                }

                                if (MigrationJobs != null)
                                {
                                    errorMessage = string.Empty;
                                    return true;
                                }
                            }
                            catch (JsonException)
                            {
                                // If deserialization fails, wait and retry
                                Thread.Sleep(100); // Wait for 100 milliseconds before retrying
                            }
                        }
                        errorMessage = $"Error loading migration jobs.";
                        return false;                        
                    }
                }                   
                catch (Exception ex)
                {
                    errorMessage = $"Error loading migration jobs: {ex}";
                    return false;
                }
            }

        }

        private bool ConvertToNewFormat(string old_filePath, out string errorMessage)
        {
            try
            { 
                string json = File.ReadAllText(old_filePath);
                var loadedObject = JsonConvert.DeserializeObject<JobList>(json);
                if (loadedObject != null)
                {
                   
                    MigrationJobIds = new List<string>();
                    foreach (var mj in loadedObject.MigrationJobs)
                    {
                        //mj.SaveToDisk();
                        mj.MigrationUnitBasics = new List<MigrationUnitBasic>();
                        foreach (var mu in mj.MigrationUnits)
                        {
                            mu.Id = Helper.GenerateMigrationUnitId(mu.DatabaseName, mu.CollectionName);
                            mu.JobId = mj.Id;
                            mu.ParentJob = mj;
                            var mub =mu.GetBasic();
                           
                            mj.MigrationUnitBasics.Add(mub);
                            MigrationJobContext.SaveMigrationUnit(mu,false);
                        }
                        
                        MigrationJobIds.Add(mj.Id);
                        MigrationJobContext.SaveMigrationJob(mj);
                    }                    
                   
                }
                errorMessage= string.Empty; 
                return true;
            }
            catch (Exception ex)
            {
                errorMessage = $"Error loading old job list: {ex}";
                _log?.WriteLine(errorMessage, LogType.Error);
                return false;
            }
        }


        /*
        public bool LoadJobs(out string errorMessage, bool loadBackup = false)
        {
            //errorMessage = string.Empty;
            lock (_loadLock)
            {
                string path;

                path = loadBackup ? GetBestRestoreSlotFilePath() : _filePath;

                if (string.IsNullOrEmpty(path) || !File.Exists(path))
                {
                    errorMessage = "No suitable backup file found for restoration.";
                    return false;
                }

                int max = loadBackup ? 1 : 5; //if loading backup, try once  else 4 attempts 
                //this._log = _log;
                try
                {
                    if (File.Exists(path))
                    {
                        for (int i = 0; i < max; i++) //4 attempts to load json
                        {
                            try
                            {
                                string json = File.ReadAllText(path);
                                var loadedObject = JsonConvert.DeserializeObject<JobList>(json);
                                if (loadedObject != null)
                                {
                                    MigrationJobs = loadedObject.MigrationJobs;
                                }

                                if (loadBackup)
                                {
                                    //delete all files in SlotNames
                                    foreach (string name in SlotNames)
                                    {
                                        if (System.IO.File.Exists(Path.Combine(_backupFolderPath, name)))
                                            System.IO.File.Delete(Path.Combine(_backupFolderPath, name));
                                    }
                                    Save(out errorMessage, true);
                                }

                                if (MigrationJobs != null)
                                {
                                    errorMessage = string.Empty;
                                    return true;
                                }
                            }
                            catch (JsonException)
                            {
                                // If deserialization fails, wait and retry
                                Thread.Sleep(100); // Wait for 100 milliseconds before retrying
                            }

                        }

                        if (MigrationJobs == null && !loadBackup)
                        {
                            string directory = Path.GetDirectoryName(path) ?? string.Empty;
                            string fileNameWithoutExtension = Path.GetFileNameWithoutExtension(path);
                            string extension = Path.GetExtension(path);
                            string timestamp = DateTime.Now.ToString("yyyyMMdd_HHmmss");
                            string newFileName = $"{fileNameWithoutExtension}_{timestamp}{extension}";
                            string newFilePath = Path.Combine(directory, newFileName);

                            if (!File.Exists(newFilePath))
                            {
                                File.Copy(path, newFilePath);
                            }

                            errorMessage = $"Error loading migration jobs. Please restore from backup.";
                        }
                        else
                        {
                            errorMessage = string.Empty;
                        }
                        return false;
                    }
                    else
                    {
                        errorMessage = "Migration jobs file does not exist.";
                        MigrationJobs = new List<MigrationJob>();
                        return true; // Return true even if the file does not exist, as it will be created later
                    }
                }
                catch (Exception ex)
                {
                    errorMessage = $"Error loading migration jobs: {ex}";
                    return false;
                }
            }
        }*/


        //private string GetBestRestoreSlotFilePath()
        //{
        //    DateTime now = DateTime.Now;
        //    DateTime minAllowedTime = now.AddMinutes(-1 * TUMBLING_INTERVAL_MINUTES * (SlotNames.Length - 1));

        //    var backupFolder = _backupFolderPath; // Replace with your folder path
        //    var slotFiles = SlotNames
        //        .Select(name => Path.Combine(backupFolder, name))
        //        .Where(File.Exists)
        //        .Select(filePath => new
        //        {
        //            Path = filePath,
        //            Timestamp = File.GetLastWriteTime(filePath)
        //        })
        //        .OrderBy(f => f.Timestamp) // Sort ascending
        //        .ToList();


        //    string latestFile = string.Empty;
        //    for (int i = slotFiles.Count - 1; i >= 0; i--)
        //    {
        //        if (slotFiles[i].Timestamp < minAllowedTime)
        //        {
        //            latestFile = slotFiles[i].Path;
        //            break;
        //        }
        //    }

        //    if (slotFiles.Count > 0 && latestFile == string.Empty)
        //        latestFile = slotFiles.First().Path;

        //    if (latestFile != string.Empty)
        //    {
        //        return backupFolder.Any() ? latestFile : string.Empty;
        //    }
        //    else
        //    {
        //        return string.Empty;
        //    }

        //}

        //public DateTime GetBackupDate()
        //{
        //    var path = GetBestRestoreSlotFilePath();

        //    var backupDataUpdatedOn = !string.IsNullOrEmpty(path) && File.Exists(path) ? File.GetLastWriteTimeUtc(path) : DateTime.MinValue;
        //    return backupDataUpdatedOn;
        //}

        //public bool Save()
        //{
        //    return Save(out string errorMessage);
        //}

        //public bool Save(out string errorMessage, bool forceBackup = false)
        //{
        //    try
        //    {
        //        lock (_fileLock)
        //        {
        //            // Memory-safe serialization with streaming
        //            string tempFile = _filePath + ".tmp";
                    
        //            if (!TrySerializeToFileStreaming(tempFile, out string serializationError))
        //            {
        //                errorMessage = serializationError;
        //                return false;
        //            }

        //            // Step 2: Safe atomic replacement with file lock handling
        //            if (File.Exists(_filePath))
        //            {
        //                if (!TryReplaceFileWithRetry(_filePath, tempFile, out string replaceError))
        //                {
        //                    errorMessage = replaceError;
        //                    // Clean up temp file
        //                    if (File.Exists(tempFile))
        //                        File.Delete(tempFile);
        //                    return false;
        //                }
        //            }
        //            else
        //            {
        //                // No existing file, just move temp to final location with retry
        //                if (!TryMoveFileWithRetry(tempFile, _filePath, out string moveError))
        //                {
        //                    errorMessage = moveError;
        //                    // Clean up temp file
        //                    if (File.Exists(tempFile))
        //                        File.Delete(tempFile);
        //                    return false;
        //                }
        //            }

        //            DateTime now = DateTime.UtcNow;

        //            bool hasJobs = this.MigrationJobs != null && this.MigrationJobs.Count > 0;

        //            if (File.Exists(_filePath) && hasJobs && (_processedMin != now.ToString("MM/dd/yyyy HH:mm") || forceBackup))
        //            {

        //                // Rotate every 15 minutes
        //                if (now.Minute % TUMBLING_INTERVAL_MINUTES == 0 || forceBackup)
        //                {

        //                    //set processed minute               
        //                    _processedMin = now.ToString("MM/dd/yyyy HH:mm");

        //                    int slotIndex = (now.Minute / TUMBLING_INTERVAL_MINUTES) % SlotNames.Length; // 0, 1, 2, or 3

        //                    // Step 3: Write new backup into slot with retry logic
        //                    string latestSlot = Path.Combine(_backupFolderPath, SlotNames[slotIndex]);
        //                    if (!TryCopyFileWithRetry(_filePath, latestSlot, out string copyError))
        //                    {
        //                        _log?.WriteLine($"Warning: Failed to create backup slot: {copyError}", LogType.Error);
        //                        // Don't fail the entire save operation for backup failure
        //                    }
        //                }
        //            }
        //            errorMessage = string.Empty;
        //            return true;
        //        }
        //    }
        //    catch (Exception ex)
        //    {
        //        errorMessage = $"Error saving data: {ex}";
        //        _log?.WriteLine(errorMessage, LogType.Error);
        //        return false;
        //    }
        //}

        /// <summary>
        /// Memory-safe serialization using streaming to avoid OOM during large object serialization
        ///// </summary>
        //private bool TrySerializeToFileStreaming(string filePath, out string errorMessage)
        //{
        //    try
        //    {
        //        // Check available memory before serialization
        //        var gcMemory = GC.GetTotalMemory(false);
        //        var gcMemoryMB = gcMemory / 1024 / 1024;
                
        //        if (gcMemoryMB > 1536) // Warning if over 1.5GB
        //        {
        //            _log?.WriteLine($"WARNING: High memory usage before serialization: {gcMemoryMB}MB", LogType.Warning);
                    
        //            // Force GC before serialization to free up memory
        //            GC.Collect(2, GCCollectionMode.Aggressive, true, true);
        //            GC.WaitForPendingFinalizers();
                    
        //            gcMemory = GC.GetTotalMemory(false);
        //            gcMemoryMB = gcMemory / 1024 / 1024;
        //            _log?.WriteLine($"Memory after GC: {gcMemoryMB}MB", LogType.Info);
        //        }

        //        // Use streaming serialization to avoid large string allocation
        //        using (var fileStream = new FileStream(filePath, FileMode.Create, FileAccess.Write, FileShare.None, 65536))
        //        using (var streamWriter = new StreamWriter(fileStream, System.Text.Encoding.UTF8, 65536))
        //        using (var jsonWriter = new JsonTextWriter(streamWriter))
        //        {
        //            // Configure serializer for memory efficiency
        //            var serializer = new JsonSerializer
        //            {
        //                Formatting = Formatting.None, // Minimize output size
        //                NullValueHandling = NullValueHandling.Ignore,
        //                DefaultValueHandling = DefaultValueHandling.Ignore
        //            };

        //            try
        //            {
        //                // Stream serialize directly to file without creating large in-memory string
        //                serializer.Serialize(jsonWriter, this);
        //                jsonWriter.Flush();
        //                streamWriter.Flush();
        //                fileStream.Flush();
                        
        //                errorMessage = string.Empty;
        //                return true;
        //            }
        //            catch (OutOfMemoryException ex)
        //            {
        //                errorMessage = $"Out of memory during serialization: {ex.Message}. Consider reducing data size or increasing available memory.";
        //                _log?.WriteLine(errorMessage, LogType.Error);
                        
        //                // Emergency fallback: try minimal serialization
        //                return TryEmergencySave(filePath, out errorMessage);
        //            }
        //        }
        //    }
        //    catch (Exception ex)
        //    {
        //        errorMessage = $"Error during streaming serialization: {ex.Message}";
        //        _log?.WriteLine(errorMessage, LogType.Error);
        //        return false;
        //    }
        //}

        ///// <summary>
        ///// Emergency save method when regular serialization fails due to OOM
        ///// Saves only essential job metadata without detailed migration mu data
        ///// </summary>
        //private bool TryEmergencySave(string filePath, out string errorMessage)
        //{
        //    try
        //    {
        //        _log?.WriteLine("Attempting emergency save with minimal data...", LogType.Warning);
                
        //        // Create a minimal version of JobList with only essential data
        //        var minimalJobList = new JobList();
        //        minimalJobList.ActiveDumpProcessIds = this.ActiveDumpProcessIds ?? new List<int>();
        //        minimalJobList.ActiveRestoreProcessIds = this.ActiveRestoreProcessIds ?? new List<int>();
                
        //        // Create minimal jobs with only essential metadata
        //        if (this.MigrationJobs != null)
        //        {
        //            minimalJobList.MigrationJobs = new List<MigrationJob>();
        //            foreach (var job in this.MigrationJobs)
        //            {
        //                var minimalJob = new MigrationJob
        //                {
        //                    Id = job.Id,
        //                    Name = job.Name,
        //                    SourceEndpoint = job.SourceEndpoint,
        //                    TargetEndpoint = job.TargetEndpoint,
        //                    StartedOn = job.StartedOn,
        //                    IsCompleted = job.IsCompleted,
        //                    CDCMode = job.CDCMode,
        //                    IsCancelled = job.IsCancelled,
        //                    IsStarted = job.IsStarted,
        //                    JobType = job.JobType,
        //                    // Create minimal migration units without detailed chunk data
        //                    MigrationUnits = job.MigrationUnits?.Select(mu => new MigrationUnit(
        //                        mu.DatabaseName, 
        //                        mu.CollectionName, 
        //                        new List<MigrationChunk>()) // Empty chunks to save memory
        //                    {
        //                        DumpComplete = mu.DumpComplete,
        //                        RestoreComplete = mu.RestoreComplete,
        //                        DumpPercent = mu.DumpPercent,
        //                        RestorePercent = mu.RestorePercent,
        //                        SourceStatus = mu.SourceStatus,
        //                        EstimatedDocCount = mu.EstimatedDocCount,
        //                        ActualDocCount = mu.ActualDocCount
        //                    }).ToList()
        //                };
        //                minimalJobList.MigrationJobs.Add(minimalJob);
        //            }
        //        }

        //        // Use simple serialization for minimal data
        //        using (var fileStream = new FileStream(filePath, FileMode.Create, FileAccess.Write, FileShare.None))
        //        using (var streamWriter = new StreamWriter(fileStream))
        //        using (var jsonWriter = new JsonTextWriter(streamWriter))
        //        {
        //            var serializer = new JsonSerializer
        //            {
        //                Formatting = Formatting.None,
        //                NullValueHandling = NullValueHandling.Ignore,
        //                DefaultValueHandling = DefaultValueHandling.Ignore
        //            };

        //            serializer.Serialize(jsonWriter, minimalJobList);
        //            jsonWriter.Flush();
        //            streamWriter.Flush();
        //        }

        //        _log?.WriteLine("Emergency save completed successfully with minimal data", LogType.Info);
        //        errorMessage = "Emergency save completed - some detailed data was omitted to prevent memory exhaustion";
        //        return true;
        //    }
        //    catch (Exception ex)
        //    {
        //        errorMessage = $"Emergency save also failed: {ex.Message}";
        //        _log?.WriteLine(errorMessage, LogType.Error);
        //        return false;
        //    }
        //}

        //private bool TryReplaceFileWithRetry(string targetPath, string sourcePath, out string errorMessage)
        //{
        //    const int maxRetries = 5;
        //    const int delayMs = 100;

        //    for (int attempt = 1; attempt <= maxRetries; attempt++)
        //    {
        //        try
        //        {
        //            string backupFile = targetPath + ".backup";

        //            // Remove any existing backup
        //            if (File.Exists(backupFile))
        //                File.Delete(backupFile);

        //            // Move original to backup
        //            File.Move(targetPath, backupFile);

        //            try
        //            {
        //                // Move temp to final location
        //                File.Move(sourcePath, targetPath);

        //                // Clean up backup
        //                File.Delete(backupFile);

        //                errorMessage = string.Empty;
        //                return true;
        //            }
        //            catch
        //            {
        //                // If move fails, restore from backup
        //                if (File.Exists(backupFile) && !File.Exists(targetPath))
        //                {
        //                    File.Move(backupFile, targetPath);
        //                }
        //                throw;
        //            }
        //        }
        //        catch (IOException ex) when (IsFileLockException(ex) && attempt < maxRetries)
        //        {
        //            _log?.WriteLine($"File lock detected on attempt {attempt}, retrying in {delayMs}ms...");
        //            Thread.Sleep(delayMs * attempt); // Progressive delay
        //        }
        //        catch (UnauthorizedAccessException) when (attempt < maxRetries)
        //        {
        //            _log?.WriteLine($"Access denied on attempt {attempt}, retrying in {delayMs}ms...");
        //            Thread.Sleep(delayMs * attempt); // Progressive delay
        //        }
        //    }

        //    errorMessage = $"Failed to replace file after {maxRetries} attempts due to file lock or access issues.";
        //    return false;
        //}

        //private bool TryMoveFileWithRetry(string sourcePath, string targetPath, out string errorMessage)
        //{
        //    const int maxRetries = 5;
        //    const int delayMs = 100;

        //    for (int attempt = 1; attempt <= maxRetries; attempt++)
        //    {
        //        try
        //        {
        //            File.Move(sourcePath, targetPath);
        //            errorMessage = string.Empty;
        //            return true;
        //        }
        //        catch (IOException ex) when (IsFileLockException(ex) && attempt < maxRetries)
        //        {
        //            _log?.WriteLine($"File lock detected on attempt {attempt}, retrying in {delayMs}ms...");
        //            Thread.Sleep(delayMs * attempt); // Progressive delay
        //        }
        //        catch (UnauthorizedAccessException) when (attempt < maxRetries)
        //        {
        //            _log?.WriteLine($"Access denied on attempt {attempt}, retrying in {delayMs}ms...");
        //            Thread.Sleep(delayMs * attempt); // Progressive delay
        //        }
        //    }

        //    errorMessage = $"Failed to move file after {maxRetries} attempts due to file lock or access issues.";
        //    return false;
        //}

        //private bool TryCopyFileWithRetry(string sourcePath, string targetPath, out string errorMessage)
        //{
        //    const int maxRetries = 3;
        //    const int delayMs = 100;

        //    for (int attempt = 1; attempt <= maxRetries; attempt++)
        //    {
        //        try
        //        {
        //            File.Copy(sourcePath, targetPath, overwrite: true);
        //            errorMessage = string.Empty;
        //            return true;
        //        }
        //        catch (IOException ex) when (IsFileLockException(ex) && attempt < maxRetries)
        //        {
        //            _log?.WriteLine($"Backup copy file lock detected on attempt {attempt}, retrying in {delayMs}ms...");
        //            Thread.Sleep(delayMs * attempt); // Progressive delay
        //        }
        //        catch (UnauthorizedAccessException) when (attempt < maxRetries)
        //        {
        //            _log?.WriteLine($"Backup copy access denied on attempt {attempt}, retrying in {delayMs}ms...");
        //            Thread.Sleep(delayMs * attempt); // Progressive delay
        //        }
        //    }

        //    errorMessage = $"Failed to copy backup file after {maxRetries} attempts due to file lock or access issues.";
        //    return false;
        //}

        //private static bool IsFileLockException(IOException ex)
        //{
        //    // Common HRESULTs for file locking issues
        //    const int ERROR_SHARING_VIOLATION = -2147024864; // 0x80070020
        //    const int ERROR_LOCK_VIOLATION = -2147024865;    // 0x8007001F

        //    return ex.HResult == ERROR_SHARING_VIOLATION || ex.HResult == ERROR_LOCK_VIOLATION ||
        //           ex.Message.Contains("being used by another process", StringComparison.OrdinalIgnoreCase);
        //}
    }
}