using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;

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

        public static void Init(string id)
        {
            _currentId = id;
            Directory.CreateDirectory($"{Helper.GetWorkingFolder()}migrationlogs");

            _logBucket = GetLogBucket(_currentId);
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
            }
            catch { }
        }

        public static void Dispose()
        {
            _currentId = string.Empty;
            _logBucket = null;
        }

        public static void Save()
        {
            try
            {
                string json = JsonConvert.SerializeObject(_logBucket);
                var path = $"{Helper.GetWorkingFolder()}migrationlogs\\{_currentId}.txt";
                File.WriteAllText(path, json);
            }
            catch { }
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
            string newFileName = $"{fileNameWithoutExtension}_{timestamp}{extension}";
            string newFilePath = Path.Combine(directory, newFileName);

            File.Copy(sourceFilePath, newFilePath);

           return newFilePath;
        }

        public static LogBucket GetLogBucket(string id)
        {
            try
            {
                if (id == _currentId && _logBucket != null)
                    return _logBucket;

                var path = $"{Helper.GetWorkingFolder()}migrationlogs\\{id}.txt";
                if (File.Exists(path))
                {
                    string json = File.ReadAllText(path);
                    try
                    {
                        var loadedObject = JsonConvert.DeserializeObject<LogBucket>(json);
                        return loadedObject ?? new LogBucket();
                    }
                    catch
                    {
                        string newFilePath=CreateFileCopyWithTimestamp(path);
                        System.IO.File.Delete(path);
                        var logBucket= new LogBucket();
                        logBucket.Logs ??= new List<LogObject>();
                        logBucket.Logs.Add(new LogObject(LogType.Error, $"Error loading existing log. Log file is backed up at {newFilePath}"));
                        return logBucket;
                    }
                }
                else
                {
                    return new LogBucket();
                }
            }
            catch
            {
                throw new Exception("Log Init failed");
            }
        }
    }
}

