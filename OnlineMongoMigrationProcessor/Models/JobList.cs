using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using OnlineMongoMigrationProcessor.Context;
using SharpCompress.Common;
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
               
        private static readonly object _writeLock = new object();
        private static readonly object _loadLock = new object();
        private Log _log;

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

        public bool Persist()
        {
            lock (_writeLock)
            {
                var filePath = $"migrationjobs\\joblist.json";
                string json = JsonConvert.SerializeObject(this, Formatting.Indented);
                return MigrationJobContext.Store.UpsertDocument(filePath, json);
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

                string newFormatPath = $"migrationjobs\\joblist.json";

                try
                {
                    int max = 5;
                    if (!MigrationJobContext.Store.DocumentExists(newFormatPath))
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
                                //load new format
                                if (!MigrationJobContext.Store.DocumentExists(newFormatPath))
                                {
                                    errorMessage = "No suitable file in new format found.";
                                    return false;
                                }
                               
                                string json = MigrationJobContext.Store.ReadDocument(newFormatPath);
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
    }
}