using Microsoft.Extensions.Configuration;
using Newtonsoft.Json;
using OnlineMongoMigrationProcessor.Persistence;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using static OnlineMongoMigrationProcessor.JobList;

namespace OnlineMongoMigrationProcessor.Context
{
    public static class MigrationJobContext
    {
        private static readonly object _writeMULock = new object();
        private static readonly object _writeJobLock = new object();
        private static readonly object _writeJobListLock = new object();


        // Thread-safe process ID tracking for parallel execution
        public static List<int> ActiveDumpProcessIds { get; set; } = new List<int>();
        public static List<int> ActiveRestoreProcessIds { get; set; } = new List<int>();


        private static readonly Dictionary<string, string> _sourceConnectionStrings = new();
        private static readonly Dictionary<string, string> _targetConnectionStrings = new();

        public static ConnectionAccessor SourceConnectionString => new(_sourceConnectionStrings);
        public static ConnectionAccessor TargetConnectionString => new(_targetConnectionStrings);

        public static MigrationJob? MigrationJob { get; set; }

        public static PersistenceStorage? Store  {get; private set; }

        public static void Initialize(IConfiguration configuration)
        {
            bool isLocal = true;
            var stateStoreCSorPath = string.Empty;
            var appId = string.Empty;
            try
            {
                bool.TryParse(configuration["StateStore:UseLocalDisk"], out isLocal);
                stateStoreCSorPath = configuration["StateStore:ConnectionStringOrPath"];             
                appId = configuration["StateStore:AppID"];
            }
            catch
            {
                //do nothing and fallback to defaults
            }

            //For local and WebApp deployments, use local disk. For others use DocumentDB
            if (Helper.IsWindows())
            {
                if (isLocal)
                {
                    Store = new DiskPersistence();
                    var localPath = string.IsNullOrEmpty(stateStoreCSorPath) ? Helper.GetWorkingFolder() : stateStoreCSorPath;
                    Store.Initialize(localPath, string.Empty);
                    return;
                }
                else
                {                  
                    if (string.IsNullOrEmpty(stateStoreCSorPath))
                    {
                        throw new InvalidOperationException("Please configure 'StateStore:ConnectionString' in appsettings.json or 'StateStoreConnectionStringOrPath'  environment variable.");
                    }
                    if (string.IsNullOrEmpty(appId))
                    {
                        throw new InvalidOperationException("Please configure 'StateStore:AppID' in appsettings.json or 'StateStoreAppID' environment variable.");
                    }

                    Store = new DocumentDBPersistence();
                    Store.Initialize(stateStoreCSorPath, appId);
                } 
            }
            else
            {

                if (string.IsNullOrEmpty(stateStoreCSorPath))
                {
                    throw new InvalidOperationException("Please configure 'StateStoreConnectionStringOrPath'  environment variable.");
                }
                if (string.IsNullOrEmpty(appId))
                {
                    throw new InvalidOperationException("Please configure 'StateStoreAppID'  environment variable.");
                }

                Store = new DocumentDBPersistence();
                Store.Initialize(stateStoreCSorPath, appId);
            }
        }

        public static bool SaveMigrationUnit(MigrationUnit mu, bool updateParent)
        {
            try
            {
                if (mu == null)
                    return false;

                if (MigrationJob != null)
                    mu.ParentJob = MigrationJob;

                if(mu.ParentJob != null && updateParent)
                    mu.UpdateParentJob();      

                lock (_writeMULock)
                {
                    mu.Persist();
                }

                if (MigrationJob != null && updateParent)
                {
                    lock (_writeJobLock)
                    {
                        MigrationJob.Persist();                        
                    }
                }
                return true;
            }
            catch
            {
                return false;
            }
        }

        public static bool SaveMigrationJob(MigrationJob job)
        {
            try
            {
                if (job != null)
                {
                    lock (_writeJobLock)
                    {
                        job.Persist();
                    }
                }
                return true;
            }
            catch
            {
                return false;
            }
        }

        public static bool SaveJobList(JobList jobList)
        {
            try
            {
                if (jobList != null)
                {
                    lock (_writeJobListLock)
                    {
                        jobList.Persist();
                    }
                }
                return true;
            }
            catch
            {
                return false;
            }
        }

        public static MigrationJob GetMigrationJob(string jobId)
        {
            try
            {
                var filePath = $"{Path.Combine("migrationjobs",jobId,"jobdefinition.json")}";

                var json=Store.ReadDocument(filePath);
                var loadedObject = JsonConvert.DeserializeObject<MigrationJob>(json);
                return loadedObject;
            }
            catch
            {
                return null;
            }
        }

        public static MigrationUnit GetMigrationUnit(string jobId, string unitId)
        {
            try
            {
                //Helper.CreateFolderIfNotExists($"{Helper.GetWorkingFolder()}migrationjobs\\{jobId}");
                var filePath = $"{Path.Combine("migrationjobs", jobId, $"{unitId}.json")}";
                string json =Store.ReadDocument(filePath);
                var loadedObject = JsonConvert.DeserializeObject<MigrationUnit>(json);
                return loadedObject;
            }
            catch
            {
                return null;
            }
        }

    }
}
