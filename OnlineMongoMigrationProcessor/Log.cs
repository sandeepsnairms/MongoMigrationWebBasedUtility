//using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace OnlineMongoMigrationProcessor
{
#pragma warning disable CS8602

    public class LogBucket
    {
        public List<LogObject>? Logs { get; set; }
        private List<LogObject>? _verboseMessages;
        private readonly object _lock = new object();

        public void AddVerboseMessage(string message, LogType logType = LogType.Message)
        {
            lock (_lock)
            {
                _verboseMessages ??= new List<LogObject>();

                if (_verboseMessages.Count == 5)
                {
                    _verboseMessages.RemoveAt(0); // Remove the oldest item
                }
                _verboseMessages.Add(new LogObject(logType, message)); // Add the new item
            }
        }

        public List<LogObject> GetVerboseMessages()
        {
            try
            {
                _verboseMessages ??= new List<LogObject>();
                var reversedList = new List<LogObject>(_verboseMessages); // Create a copy to avoid modifying the original list
                reversedList.Reverse(); // Reverse the copy

                // If the reversed list has fewer than 5 elements, add empty message LogObjects
                while (reversedList.Count < 5)
                {
                    reversedList.Add(new LogObject(LogType.Message, ""));
                }
                return reversedList;
            }
            catch
            {
                var blankList = new List<LogObject>();
                for (int i = 0; i < 5; i++)
                {
                    blankList.Add(new LogObject(LogType.Message, ""));
                }
                return blankList;
            }
        }
    }

    public static class Log
    {
        private static LogBucket? _logBucket;
        private static string _currentId = string.Empty;

        public static string Init(string id)
        {
            string logBackupFile = string.Empty;
            _currentId = id;
            Directory.CreateDirectory($"{Helper.GetWorkingFolder()}migrationlogs");

            _logBucket = GetLogBucket(_currentId, out logBackupFile,true);

            return logBackupFile;
        }

        public static void AddVerboseMessage(string message, LogType logType = LogType.Message)
        {
            _logBucket?.AddVerboseMessage(message, logType);
        }

        public static void WriteLine(string message, LogType logType = LogType.Message)
        {
            try
            {
                _logBucket ??= new LogBucket();
                _logBucket.Logs ??= new List<LogObject>();

                _logBucket.Logs.Add(new LogObject(logType, message));

                Save();
            }
            catch { }
        }

        public static void Dispose()
        {
            _currentId = string.Empty;
            _logBucket = null;
        }

        //public static void Save()
        //{
        //    try
        //    {
        //        string json = JsonConvert.SerializeObject(_logBucket);
        //        var path = $"{Helper.GetWorkingFolder()}migrationlogs\\{_currentId}.txt";
        //        File.WriteAllText(path, json);
        //    }
        //    catch { }
        //}
        private static readonly JsonSerializerOptions _jsonOptions = new()
        {
            PropertyNamingPolicy = null,
            DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
            WriteIndented = false,
            Converters = { new JsonStringEnumConverter() }
        };

        private static readonly object _syncLock = new(); // Thread safety

        public static void Save()
        {
            var folder = Path.Combine(Helper.GetWorkingFolder(), "migrationlogs");
            var filePath = Path.Combine(folder, $"{_currentId}.txt");
            var tempPath = filePath + ".tmp";

            try
            {
                Directory.CreateDirectory(folder);

                lock (_syncLock)
                {
                    using var fileStream = new FileStream(tempPath, FileMode.Create, FileAccess.Write, FileShare.None, 32768, FileOptions.WriteThrough);

                    JsonSerializer.Serialize(fileStream, _logBucket, _jsonOptions);

                    fileStream.Flush(); // Ensure all buffered bytes are written
                }

                // Replace old file atomically
                File.Move(tempPath, filePath, true);
            }
            catch (Exception ex)
            {
                try { if (File.Exists(tempPath)) File.Delete(tempPath); } catch { }
            }
        }
        private static string CreateFileCopyWithTimestamp(string sourceFilePath)
        {
            if (string.IsNullOrEmpty(sourceFilePath))
            {
                throw new ArgumentException("Source file path cannot be null or empty.", nameof(sourceFilePath));
            }

            if (!File.Exists(sourceFilePath))
            {
                throw new FileNotFoundException("Source file not found.", sourceFilePath);
            }

            string directory = Path.GetDirectoryName(sourceFilePath) ?? string.Empty;
            string fileNameWithoutExtension = Path.GetFileNameWithoutExtension(sourceFilePath);
            string extension = Path.GetExtension(sourceFilePath);
            string timestamp = DateTime.Now.ToString("yyyyMMdd_HHmmss");
            string newFileName = $"{fileNameWithoutExtension}_{timestamp}";
            string newFilePath = Path.Combine(directory, $"{newFileName}{extension}");

            if (!File.Exists(newFilePath))
            {
                File.Copy(sourceFilePath, newFilePath);
            }

           return newFileName;
        }

        public static LogBucket GetLogBucket(string id, out string fileName, bool force=false)
        {
            try
            {
                fileName = id;

                if (id == _currentId && _logBucket != null)
                {
                    return _logBucket;
                }

                var path = $"{Helper.GetWorkingFolder()}migrationlogs\\{id}.txt";
                if (File.Exists(path))
                {
                    string json = File.ReadAllText(path);
                    try
                    {
                        //var loadedObject = JsonConvert.DeserializeObject<LogBucket>(json);
                        LogBucket loadedObject = JsonSerializer.Deserialize<LogBucket>(json);
                        return loadedObject ?? new LogBucket();
                    }
                    catch
                    {
                        fileName = CreateFileCopyWithTimestamp(path);
                        if (force)
                        {                           
                            System.IO.File.Delete(path);

                            var logBucket = new LogBucket();
                            logBucket.Logs ??= new List<LogObject>();
                            logBucket.Logs.Add(new LogObject(LogType.Error, $"Unable to load the log file as it appears to be corrupt. A new log file will be created, and the original has been backed up as {fileName}."));
                            Save();
                            fileName = id;
                            return logBucket;
                        }
                        else
                        {
                            var logBucket = new LogBucket();
                            logBucket.Logs ??= new List<LogObject>();
                            return logBucket;
                        } 
                    }
                }
                else
                {
                    return new LogBucket();
                }
            }
            catch(Exception ex)
            {
                throw new Exception("Log Init failed");
            }
        }
    }
}

