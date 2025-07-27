//using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.IO;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading;

namespace OnlineMongoMigrationProcessor
{
#pragma warning disable CS8602      

    public class LogBucket
    {
        public List<LogObject>? Logs { get; set; } = new List<LogObject>();
    }

    public class Log
    {
        private LogBucket _logBucket;
        private List<LogObject>? _verboseMessages = new List<LogObject>();
        private string _currentId = string.Empty;

        //private static readonly object _syncLock = new();
        private static readonly object _verboseLock = new object();
        private static readonly object _readLock = new object();
        private static readonly object _writeLock = new object();
        private static readonly object _initLock = new object();

        public  bool IsInitialized { get; set; } = false;

        public void AddVerboseMessage(string message, LogType LogType = LogType.Message)
        {
            lock (_verboseLock)
            {
                if (_verboseMessages == null)
                {
                    return;
                }

                if (_verboseMessages.Count == 5)
                {
                    _verboseMessages.RemoveAt(0); // Remove the oldest item
                }
                _verboseMessages.Add(new LogObject(LogType, message)); // Add the new item
            }
        }


        
        public List<LogObject> GetVerboseMessages()
        {

            try
            {
                if (_verboseMessages == null || _verboseMessages.Count == 0)
                {
                    return new List<LogObject>();
                }

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

        private static readonly JsonSerializerOptions _jsonOptions = new()
        {
            PropertyNamingPolicy = null,
            DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
            WriteIndented = false,
            Converters = { new JsonStringEnumConverter() }
        };


        public string Init(string id)
        {
            lock (_initLock)
            {
                string logBackupFile = string.Empty;
                _currentId = id;

                Directory.CreateDirectory(Path.Combine(Helper.GetWorkingFolder(), "migrationlogs"));

                _logBucket = ReadLogFile(_currentId, out logBackupFile);
                _verboseMessages.Clear();

                IsInitialized=true;
                return logBackupFile;
            }
        }


        public void WriteLine(string message, LogType LogType = LogType.Message)
        {
            try
            {

                lock (_writeLock)
                {

                    // _logBucket ??= new LogBucket();
                    //_logBucket.Logs ??= new List<LogObject>();

                    if (_logBucket == null)
                    {
                        string logBackupFile = string.Empty;
                        _logBucket = ReadLogFile(_currentId, out logBackupFile);
                        Console.WriteLine($"LogBucket was null, re-initialized from file during WriteLine .");
                    }

                    var logObj = new LogObject(LogType, message);

                    // Add new log
                    _logBucket.Logs.Add(logObj);

                    // If more than 300 logs, remove the 21st item (index 20), keep it small
                    if (_logBucket.Logs.Count > 300 && _logBucket.Logs.Count > 20)
                    {
                        _logBucket.Logs.RemoveAt(20);
                    }

                    //persits to file
                    AppendBinaryLog(logObj);
                 }
                
            }
            catch
            {
                // Optionally log or ignore
            }
        }


        public void Dispose()
        {
            _currentId = string.Empty;
            _verboseMessages.Clear();
        }

        private void WriteBinaryLog(string id, List<LogObject> logs)
        {
            if (logs == null || logs.Count == 0)
                return;


            var folder = Path.Combine(Helper.GetWorkingFolder(), "migrationlogs");
            var binPath = Path.Combine(folder, $"{id}.bin");

            try
            {
                Directory.CreateDirectory(folder);

                using var fs = new FileStream(binPath, FileMode.Append, FileAccess.Write, FileShare.Read, 4096, FileOptions.WriteThrough);
                using var bw = new BinaryWriter(fs);

                foreach (var log in logs)
                {
                    var message = log.Message ?? string.Empty;
                    var messageBytes = Encoding.UTF8.GetBytes(message);

                    // Write record with length prefix so it can be parsed safely later
                    bw.Write(messageBytes.Length);
                    bw.Write(messageBytes);
                    bw.Write((byte)log.Type);
                    bw.Write(log.Datetime.ToBinary());
                }

                bw.Flush();         // Flush binary writer
                fs.Flush(true);     // Force flush to disk hardware

            }
            catch
            {
                throw;
            }
        }

        private void AppendBinaryLog(LogObject log)
        {
            try
            {
                var folder = Path.Combine(Helper.GetWorkingFolder(), "migrationlogs");
                var binPath = Path.Combine(folder, $"{_currentId}.bin");

                Directory.CreateDirectory(folder);

                using var fs = new FileStream(binPath, FileMode.Append, FileAccess.Write, FileShare.Read, 4096, FileOptions.WriteThrough);
                using var bw = new BinaryWriter(fs);

                var messageBytes = Encoding.UTF8.GetBytes(log.Message);
                bw.Write(messageBytes.Length);
                bw.Write(messageBytes);
                bw.Write((byte)log.Type);
                bw.Write(log.Datetime.ToBinary());
            }
            catch { }
        }
        

        private string CreateFileCopyWithTimestamp(string sourceFilePath)
        {
            if (string.IsNullOrEmpty(sourceFilePath))
                throw new ArgumentException("Source file path cannot be null or empty.", nameof(sourceFilePath));

            if (!File.Exists(sourceFilePath))
                throw new FileNotFoundException("Source file not found.", sourceFilePath);

            string directory = Path.GetDirectoryName(sourceFilePath) ?? string.Empty;
            string fileNameWithoutExtension = Path.GetFileNameWithoutExtension(sourceFilePath);
            string extension = Path.GetExtension(sourceFilePath);
            string timestamp = DateTime.Now.ToString("yyyyMMdd_HHmmss");
            string newFileName = $"{fileNameWithoutExtension}_{timestamp}{extension}";
            string newFilePath = Path.Combine(directory, newFileName);

            if (!File.Exists(newFilePath))
            {
                File.Copy(sourceFilePath, newFilePath);
            }

            return newFileName;
        }

        public LogBucket GetCurentLogBucket(string id)
        {
            if (_currentId == id && _logBucket != null)
            {
                return _logBucket;
            }

            return null;
        }

        public LogBucket ReadLogFile(string id, out string fileName)
        {
            fileName = id;

            try
            {
                lock (_readLock)
                {
                    Console.WriteLine($"Reading log file for ID: {id}");
                    var folder = Path.Combine(Helper.GetWorkingFolder(), "migrationlogs");
                    var txtPath = Path.Combine(folder, $"{id}.txt");
                    var binPath = Path.Combine(folder, $"{id}.bin");

                    // 1. Try Binary first
                    if (File.Exists(binPath))
                    {
                        return ParseLogBinFile(binPath);
                    }

                    // 2. Fallback to JSON if .bin is missing (backward compatibility)
                    if (File.Exists(txtPath))
                    {
                        string json = File.ReadAllText(txtPath);
                        try
                        {
                            //old format with LogBucket
                            LogBucket logBucket = JsonSerializer.Deserialize<LogBucket>(json, _jsonOptions);
                            if (logBucket == null || logBucket.Logs.Count == 0)
                            {
                                return new LogBucket(); // empty log
                            }
                            WriteBinaryLog(id, logBucket.Logs);
                            return ParseLogBinFile(binPath);
                        }
                        catch
                        {
                            try
                            {   //new format with List<LogObject>
                                List<LogObject>? logs = JsonSerializer.Deserialize<List<LogObject>>(json, _jsonOptions);
                                if (logs == null || logs.Count == 0)
                                {
                                    return new LogBucket(); // empty log
                                }

                                WriteBinaryLog(id, logs);
                                return ParseLogBinFile(binPath);
                            }
                            catch
                            {
                                fileName = CreateFileCopyWithTimestamp(txtPath);

                                //if (force)
                                //{
                                    File.Delete(txtPath);

                                    var logBucket = new LogBucket();
                                    logBucket.Logs ??= new List<LogObject>();
                                    logBucket.Logs.Add(new LogObject(LogType.Error, $"Unable to load the log file as JSON; original file backed up as {fileName}"));
                                    WriteBinaryLog(id, logBucket.Logs);
                                    return ParseLogBinFile(binPath);
                                //}

                                return new LogBucket(); // fallback empty
                            }
                        }
                    }

                    return new LogBucket();
                }
            }
            catch
            {
                throw new Exception("Log Init failed");
            }
        }

        public byte[] DownloadLogsAsJsonBytes(string binPath, int topEntries = 20, int bottomEntries = 230)
        {
            var logs = new List<LogObject>();
            var offsets = new List<long>();

            try
            {
                using var fs = new FileStream(binPath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
                using var br = new BinaryReader(fs);

                // Pass 1: Index all log entry positions
                while (fs.Position < fs.Length)
                {
                    long offset = fs.Position;

                    try
                    {
                        int msgLen = br.ReadInt32();
                        fs.Position += msgLen + 1 + 8; // Skip remainder: message, enum, datetime
                        offsets.Add(offset);
                    }
                    catch
                    {
                        break; // Stop on malformed log
                    }
                }

                // Select top + bottom
                var selectedOffsets = offsets
                    .Take(topEntries)
                    .Concat(offsets.Skip(Math.Max(0, offsets.Count - bottomEntries)))
                    .Distinct()
                    .OrderBy(i => i)
                    .ToList();

                // Pass 2: Read selected logs
                foreach (var offset in selectedOffsets)
                {
                    fs.Position = offset;
                    var log = TryReadLogEntry(br);
                    if (log != null)
                        logs.Add(log);
                }
            }
            catch
            {
                // Optionally log or handle error
            }

            // Serialize selected logs to JSON
            var options = new JsonSerializerOptions
            {
                WriteIndented = true,
                Converters = { new JsonStringEnumConverter() }
            };

            return JsonSerializer.SerializeToUtf8Bytes(logs, options);
        }


        private LogBucket ParseLogBinFile(string binPath,int topCount = 20, int bottomCount = 280)
        {
            var logBucket = new LogBucket { Logs = new List<LogObject>() };
            var offsets = new List<long>();

            if (!File.Exists(binPath))
                return logBucket;

            try
            {
                using (var fs = new FileStream(binPath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
                using (var br = new BinaryReader(fs))
                {
                    // First pass: collect offsets of all entries
                    while (fs.Position < fs.Length)
                    {
                        long offset = fs.Position;

                        try
                        {
                            int msgLen = br.ReadInt32();
                            fs.Position += msgLen + 1 + 8; // Skip over message, LogType, DateTime
                            offsets.Add(offset);
                        }
                        catch
                        {
                            continue;
                        }
                    }

                    // Select required offsets
                    List<long> selectedOffsets;
                    if (offsets.Count > 300)
                    {
                        // Select top N and bottom M
                        selectedOffsets = offsets
                            .Take(topCount)
                            .Concat(offsets.Skip(Math.Max(0, offsets.Count - bottomCount)))
                            .Distinct()
                            .OrderBy(o => o)
                            .ToList();
                    }
                    else
                    {
                        // Use all offsets (full log)
                        selectedOffsets = offsets;
                    }

                    // Second pass: read selected entries
                    foreach (var offset in selectedOffsets)
                    {
                        fs.Position = offset;
                        var log = TryReadLogEntry(br);
                        if (log != null)
                            logBucket.Logs!.Add(log);
                    }
                }
            }
            catch
            {
                // Optional: handle/log if needed
            }

            return logBucket;
        }


        private LogObject? TryReadLogEntry(BinaryReader br)
        {
            const int MaxReasonableLength = 1_000_000; // 1 MB max per message

            try
            {
                // Check if there are at least 4 bytes to read the length
                if (br.BaseStream.Position + 4 > br.BaseStream.Length)
                    return null;

                int len = br.ReadInt32();

                // Validate length
                if (len <= 0 || len > MaxReasonableLength)
                {
                    Console.WriteLine($"Invalid length: {len}. Aborting read.");
                    return null;
                }

                // Check if there are enough bytes to read: message, log type, and datetime
                long requiredBytes = len + 1 + 8;
                if (br.BaseStream.Position + requiredBytes > br.BaseStream.Length)
                {
                    Console.WriteLine($"Incomplete log entry: length={len}, remaining={br.BaseStream.Length - br.BaseStream.Position}");
                    return null;
                }

                // Read the message
                byte[] bytes = br.ReadBytes(len);
                if (bytes.Length != len)
                {
                    Console.WriteLine($"ReadBytes returned less than expected: got {bytes.Length}, expected {len}");
                    return null;
                }

                string msg = Encoding.UTF8.GetString(bytes);

                // Read log type (1 byte)
                byte typeByte = br.ReadByte();
                var type = (LogType)typeByte;

                // Read timestamp (8 bytes)
                long dateBinary = br.ReadInt64();
                DateTime datetime = DateTime.FromBinary(dateBinary);

                return new LogObject(type, msg) { Datetime = datetime };
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Exception during TryReadLogEntry: {ex.Message}");
                return null;
            }
        }

    }

}

