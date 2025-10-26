using Newtonsoft.Json;
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
        public List<MigrationJob>? MigrationJobs { get; set; }
        
        // Thread-safe process ID tracking for parallel execution
        public List<int> ActiveDumpProcessIds { get; set; } = new List<int>();
        public List<int> ActiveRestoreProcessIds { get; set; } = new List<int>();
        
        private string _filePath = string.Empty;
        private string _backupFolderPath = string.Empty;
        private static readonly object _fileLock = new object();
        private static readonly object _loadLock = new object();
        private Log _log;
        private string _processedMin = string.Empty;

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

        public void SetLog(Log _log)
        {
            this._log = _log;
        }

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
        }


        private string GetBestRestoreSlotFilePath()
        {
            DateTime now = DateTime.Now;
            DateTime minAllowedTime = now.AddMinutes(-1 * TUMBLING_INTERVAL_MINUTES * (SlotNames.Length - 1));

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


            string latestFile = string.Empty;
            for (int i = slotFiles.Count - 1; i >= 0; i--)
            {
                if (slotFiles[i].Timestamp < minAllowedTime)
                {
                    latestFile = slotFiles[i].Path;
                    break;
                }
            }

            if (slotFiles.Count > 0 && latestFile == string.Empty)
                latestFile = slotFiles.First().Path;

            if (latestFile != string.Empty)
            {
                return backupFolder.Any() ? latestFile : string.Empty;
            }
            else
            {
                return string.Empty;
            }

        }

        public DateTime GetBackupDate()
        {
            var path = GetBestRestoreSlotFilePath();

            var backupDataUpdatedOn = !string.IsNullOrEmpty(path) && File.Exists(path) ? File.GetLastWriteTimeUtc(path) : DateTime.MinValue;
            return backupDataUpdatedOn;
        }

        public bool Save()
        {
            return Save(out string errorMessage);
        }

        public bool Save(out string errorMessage, bool forceBackup = false)
        {
            try
            {
                lock (_fileLock)
                {
                    string json = JsonConvert.SerializeObject(this);
                    string tempFile = _filePath + ".tmp";

                    // Step 1: Write JSON to temp
                    File.WriteAllText(tempFile, json);

                    // Step 2: Safe atomic replacement with file lock handling
                    if (File.Exists(_filePath))
                    {
                        if (!TryReplaceFileWithRetry(_filePath, tempFile, out string replaceError))
                        {
                            errorMessage = replaceError;
                            // Clean up temp file
                            if (File.Exists(tempFile))
                                File.Delete(tempFile);
                            return false;
                        }
                    }
                    else
                    {
                        // No existing file, just move temp to final location with retry
                        if (!TryMoveFileWithRetry(tempFile, _filePath, out string moveError))
                        {
                            errorMessage = moveError;
                            // Clean up temp file
                            if (File.Exists(tempFile))
                                File.Delete(tempFile);
                            return false;
                        }
                    }

                    DateTime now = DateTime.UtcNow;

                    bool hasJobs = this.MigrationJobs != null && this.MigrationJobs.Count > 0;

                    if (File.Exists(_filePath) && hasJobs && (_processedMin != now.ToString("MM/dd/yyyy HH:mm") || forceBackup))
                    {

                        // Rotate every 15 minutes
                        if (now.Minute % TUMBLING_INTERVAL_MINUTES == 0 || forceBackup)
                        {

                            //set processed minute               
                            _processedMin = now.ToString("MM/dd/yyyy HH:mm");

                            int slotIndex = (now.Minute / TUMBLING_INTERVAL_MINUTES) % SlotNames.Length; // 0, 1, 2, or 3

                            // Step 3: Write new backup into slot with retry logic
                            string latestSlot = Path.Combine(_backupFolderPath, SlotNames[slotIndex]);
                            if (!TryCopyFileWithRetry(_filePath, latestSlot, out string copyError))
                            {
                                _log?.WriteLine($"Warning: Failed to create backup slot: {copyError}", LogType.Error);
                                // Don't fail the entire save operation for backup failure
                            }
                        }
                    }
                    errorMessage = string.Empty;
                    return true;
                }
            }
            catch (Exception ex)
            {
                errorMessage = $"Error saving data: {ex}";
                _log?.WriteLine(errorMessage, LogType.Error);
                return false;
            }
        }

        private bool TryReplaceFileWithRetry(string targetPath, string sourcePath, out string errorMessage)
        {
            const int maxRetries = 5;
            const int delayMs = 100;

            for (int attempt = 1; attempt <= maxRetries; attempt++)
            {
                try
                {
                    string backupFile = targetPath + ".backup";

                    // Remove any existing backup
                    if (File.Exists(backupFile))
                        File.Delete(backupFile);

                    // Move original to backup
                    File.Move(targetPath, backupFile);

                    try
                    {
                        // Move temp to final location
                        File.Move(sourcePath, targetPath);

                        // Clean up backup
                        File.Delete(backupFile);

                        errorMessage = string.Empty;
                        return true;
                    }
                    catch
                    {
                        // If move fails, restore from backup
                        if (File.Exists(backupFile) && !File.Exists(targetPath))
                        {
                            File.Move(backupFile, targetPath);
                        }
                        throw;
                    }
                }
                catch (IOException ex) when (IsFileLockException(ex) && attempt < maxRetries)
                {
                    _log?.WriteLine($"File lock detected on attempt {attempt}, retrying in {delayMs}ms...");
                    Thread.Sleep(delayMs * attempt); // Progressive delay
                }
                catch (UnauthorizedAccessException) when (attempt < maxRetries)
                {
                    _log?.WriteLine($"Access denied on attempt {attempt}, retrying in {delayMs}ms...");
                    Thread.Sleep(delayMs * attempt); // Progressive delay
                }
            }

            errorMessage = $"Failed to replace file after {maxRetries} attempts due to file lock or access issues.";
            return false;
        }

        private bool TryMoveFileWithRetry(string sourcePath, string targetPath, out string errorMessage)
        {
            const int maxRetries = 5;
            const int delayMs = 100;

            for (int attempt = 1; attempt <= maxRetries; attempt++)
            {
                try
                {
                    File.Move(sourcePath, targetPath);
                    errorMessage = string.Empty;
                    return true;
                }
                catch (IOException ex) when (IsFileLockException(ex) && attempt < maxRetries)
                {
                    _log?.WriteLine($"File lock detected on attempt {attempt}, retrying in {delayMs}ms...");
                    Thread.Sleep(delayMs * attempt); // Progressive delay
                }
                catch (UnauthorizedAccessException) when (attempt < maxRetries)
                {
                    _log?.WriteLine($"Access denied on attempt {attempt}, retrying in {delayMs}ms...");
                    Thread.Sleep(delayMs * attempt); // Progressive delay
                }
            }

            errorMessage = $"Failed to move file after {maxRetries} attempts due to file lock or access issues.";
            return false;
        }

        private bool TryCopyFileWithRetry(string sourcePath, string targetPath, out string errorMessage)
        {
            const int maxRetries = 3;
            const int delayMs = 100;

            for (int attempt = 1; attempt <= maxRetries; attempt++)
            {
                try
                {
                    File.Copy(sourcePath, targetPath, overwrite: true);
                    errorMessage = string.Empty;
                    return true;
                }
                catch (IOException ex) when (IsFileLockException(ex) && attempt < maxRetries)
                {
                    _log?.WriteLine($"Backup copy file lock detected on attempt {attempt}, retrying in {delayMs}ms...");
                    Thread.Sleep(delayMs * attempt); // Progressive delay
                }
                catch (UnauthorizedAccessException) when (attempt < maxRetries)
                {
                    _log?.WriteLine($"Backup copy access denied on attempt {attempt}, retrying in {delayMs}ms...");
                    Thread.Sleep(delayMs * attempt); // Progressive delay
                }
            }

            errorMessage = $"Failed to copy backup file after {maxRetries} attempts due to file lock or access issues.";
            return false;
        }

        private static bool IsFileLockException(IOException ex)
        {
            // Common HRESULTs for file locking issues
            const int ERROR_SHARING_VIOLATION = -2147024864; // 0x80070020
            const int ERROR_LOCK_VIOLATION = -2147024865;    // 0x8007001F

            return ex.HResult == ERROR_SHARING_VIOLATION || ex.HResult == ERROR_LOCK_VIOLATION ||
                   ex.Message.Contains("being used by another process", StringComparison.OrdinalIgnoreCase);
        }
    }
}