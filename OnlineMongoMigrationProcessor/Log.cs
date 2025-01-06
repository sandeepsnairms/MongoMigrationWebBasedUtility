using Newtonsoft.Json;
using SharpCompress.Common;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;


namespace OnlineMongoMigrationProcessor
{
#pragma warning disable CS8602

    public class LogBucket
    {
        public List<LogObject>? Logs;
        private List<LogObject>? VerboseMessages;

        private readonly object _lock = new object();

        public void AddVerboseMessage(string message, LogType logtype = LogType.Messge)
        {
            lock (_lock)
            {
                if (VerboseMessages == null) VerboseMessages = new List<LogObject>();

                if (VerboseMessages.Count == 5)
                {
                    VerboseMessages.RemoveAt(0); // Remove the oldest item
                }
                VerboseMessages.Add(new LogObject(logtype, message)); // Add the new item
            }
        }

        public List<LogObject> GetVerboseMessages()
        {
            try
            {
                if (VerboseMessages == null) VerboseMessages = new List<LogObject>();
                var reversedList = new List<LogObject>(VerboseMessages); // Create a copy to avoid modifying the original list
                reversedList.Reverse(); // Reverse the copy
            

                // If the reversed list has fewer than 5 elements, add empty message LogObjects
                while (reversedList.Count < 5)
                {
                    reversedList.Add(new LogObject(LogType.Messge, ""));
                }
                return reversedList;
            }
            catch {
                var blanklist = new List<LogObject>();
                for(int i=0;i< 5;i++)
                {
                    blanklist.Add(new LogObject(LogType.Messge, ""));
                }
                return blanklist;
            }
        }
    }
    public static class Log
    {
        private static LogBucket? _logBucket;
        private static string CurrentId=string.Empty ;

        public static void init(string _id)
        {
            CurrentId = _id;
            System.IO.Directory.CreateDirectory($"{Path.GetTempPath()}migrationlogs");

            _logBucket = GetLogBucket(CurrentId);
        }

        public static void AddVerboseMessage(string Message, LogType logtype = LogType.Messge)
        {
            _logBucket.AddVerboseMessage(Message, logtype);
        }

        public static void WriteLine(string Message, LogType logtype = LogType.Messge)
        {
            try
            {   
                if (_logBucket.Logs==null)
                    _logBucket.Logs=new List<LogObject>();

                _logBucket.Logs.Add(new LogObject(logtype, Message));
            }
            catch { }
        }


        public static void Dispose()
        {
            CurrentId=string.Empty;
            _logBucket = null;

        }
        public static void Save()
        {            
            try
            {
                string json = JsonConvert.SerializeObject(_logBucket);
                var path = $"{Path.GetTempPath()}migrationlogs\\{CurrentId}.txt";
                File.WriteAllText(path, json);
            }
            catch { }
        }

        //public static string DownloadLogBucket(string id)
        //{
        //    var path = $"{Path.GetTempPath()}migrationlogs\\{id}.txt";
        //    // Read the file as a stream
        //    return System.IO.File.ReadAllText(path);
        //}

        public static LogBucket GetLogBucket(string id)
        {
            try
            {
                if(id== CurrentId && _logBucket != null)
                    return _logBucket;

                var path = $"{Path.GetTempPath()}migrationlogs\\{id}.txt";
                if (File.Exists(path))
                {
                    string json = File.ReadAllText(path);
                    var loadedObject = JsonConvert.DeserializeObject<LogBucket>(json);
                    if (loadedObject != null)
                    {
                        return loadedObject;
                    }
                    else
                    {
                        return new LogBucket();
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
