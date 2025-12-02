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

        public static string ActiveMigrationJobId { get; set; }

        private static readonly Dictionary<string, string> _sourceConnectionStrings = new();
        private static readonly Dictionary<string, string> _targetConnectionStrings = new();

        public static ConnectionAccessor SourceConnectionString => new(_sourceConnectionStrings);
        public static ConnectionAccessor TargetConnectionString => new(_targetConnectionStrings);

        public static JobList JobList {  get; private set; }
        // In-memory cache of migration jobs to ensure consistency
        private static Dictionary<string, MigrationJob> MigrationJobs { get; set; } = new Dictionary<string, MigrationJob>();
        
        // Cached instance of the currently active migration job for consistency across the application
        private static MigrationJob? _cachedCurrentlyActiveJob = null;
        
        /// <summary>
        /// Gets the currently active migration job with intelligent caching
        /// </summary>
        public static MigrationJob? CurrentlyActiveJob
        {
            get
            {
                // If we have a cached instance and it matches the active job ID, use it
                if (_cachedCurrentlyActiveJob != null && 
                    !string.IsNullOrEmpty(ActiveMigrationJobId) && 
                    _cachedCurrentlyActiveJob.Id == ActiveMigrationJobId)
                {
                    return _cachedCurrentlyActiveJob;
                }
                
                // Otherwise, fetch from GetMigrationJob and cache it
                if (!string.IsNullOrEmpty(ActiveMigrationJobId))
                {
                    _cachedCurrentlyActiveJob = GetMigrationJob(ActiveMigrationJobId);
                    return _cachedCurrentlyActiveJob;
                }
                
                return null;
            }
        }
        

        public static PersistenceStorage? Store  {get; private set; }

        public static string? AppId { get; set; }
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
                AppId = appId;
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

                    AppId=appId;
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
                AppId = appId;
                Store = new DocumentDBPersistence();
                Store.Initialize(stateStoreCSorPath, appId);
            }

            JobList= LoadJobList(out bool notFound,out string errorMessage);
            if(notFound && JobList == null)
            {
                JobList=new JobList();
                JobList.MigrationJobIds=new List<string>();                
            }
            else if(JobList == null && !string.IsNullOrEmpty(errorMessage))
            {
                throw new InvalidOperationException($"Error initializing Job List: {errorMessage}");
            }
            JobList.Persist();
        }
        public static MigrationJob? GetMigrationJob(string jobId)
        {
            if (MigrationJobs.ContainsKey(jobId))
            {
                return MigrationJobs[jobId];
            }
            else
            {
                try
                {
                    var filePath = $"{Path.Combine("migrationjobs", jobId, "jobdefinition.json")}";

                    var json = Store.ReadDocument(filePath);
                    var loadedObject = JsonConvert.DeserializeObject<MigrationJob>(json);
                    if(loadedObject == null)
                        return null;
                    MigrationJobs[jobId]= loadedObject;
                    return loadedObject;
                }
                catch
                {
                    return null;
                }
            }
        }


        //public static MigrationJob? GetCurrentlyActiveMigrationJob()
        //{
        //    // Use the cached property instead of directly calling GetMigrationJob
        //    return CurrentlyActiveJob;
        //}



        public static List<MigrationJob>  PopulateMigrationJobs(List<string> ids)
        {
            List<MigrationJob> jobs = new List<MigrationJob>();
            foreach (var id in ids)
            {
                var job=GetMigrationJob(id);
                if(job != null)
                    jobs.Add(job);
            }
            return jobs;
        }

        public static bool SaveMigrationUnit(MigrationUnit mu, bool updateParent)
        {
            try
            {
                if (mu == null)
                    return false;

                if (CurrentlyActiveJob != null)
                    mu.ParentJob = CurrentlyActiveJob;

                if(mu.ParentJob != null && updateParent)
                    mu.UpdateParentJob();      

                lock (_writeMULock)
                {
                    mu.Persist();
                }

                if (CurrentlyActiveJob != null && updateParent)
                {
                    lock (_writeJobLock)
                    {
                        CurrentlyActiveJob.Persist();                        
                    }
                }
                return true;
            }
            catch
            {
                return false;
            }
        }


        private static JobList LoadJobList(out bool notFound,out string errorMessage)
        {
            errorMessage = string.Empty;
            notFound = false;

            string newFormatPath = $"migrationjobs\\joblist.json";

            try
            {
                int max = 5;

                for (int i = 0; i < max; i++)
                {
                    try
                    {
                        if (!MigrationJobContext.Store.DocumentExists(newFormatPath))
                        {
                            notFound = true;
                            errorMessage = "No suitable file in new format found.";
                        }
                        else
                        {
                            string json = MigrationJobContext.Store.ReadDocument(newFormatPath);
                            var loadedObject = JsonConvert.DeserializeObject<JobList>(json);
                            List<string> MigrationJobIds = null;
                            if (loadedObject != null)
                            {
                                if (loadedObject.MigrationJobIds == null)
                                {
                                    errorMessage = $"Job List is corrupted, missing MigrationJobIds.";
                                    return null;
                                }
                                else
                                {
                                    JobList = loadedObject;
                                    return JobList;
                                }
                            }                            
                        }
                    }
                    catch (JsonException ex)
                    {
                        errorMessage = $"Error deserializing Job List: {ex.Message}";
                    }
                    finally
                    {
                        // Small delay before retrying
                        Task.Delay(200).Wait();
                    }
                }
                errorMessage = $"Error loading migration jobs.";
                return null;

            }
            catch (Exception ex)
            {
                errorMessage = $"Error loading migration jobs: {ex}";
                return null;
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
                        
                        // ALWAYS update the in-memory cache for this job ID
                        MigrationJobs[job.Id] = job;
                        
                        // Update the cached currently active job if this is the active job
                        if (!string.IsNullOrEmpty(ActiveMigrationJobId) && job.Id == ActiveMigrationJobId)
                        {
                            _cachedCurrentlyActiveJob = job;
                        }
                    }
                }
                return true;
            }
            catch
            {
                return false;
            }
        }

        public static bool SaveJobList()
        {
            try
            {
                if (JobList != null)
                {
                    lock (_writeJobListLock)
                    {
                        JobList.Persist();
                    }
                }
                return true;
            }
            catch
            {
                return false;
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

        /// <summary>
        /// Clears the cached currently active job. Use this when switching active jobs or when you need to force a reload.
        /// </summary>
        public static void ClearCurrentlyActiveJobCache()
        {
            _cachedCurrentlyActiveJob = null;
        }

    }
}
