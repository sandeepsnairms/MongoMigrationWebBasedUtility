using System;
using System.Collections.Generic;
using OnlineMongoMigrationProcessor;
using OnlineMongoMigrationProcessor.Models;
using OnlineMongoMigrationProcessor.Workers;
using OnlineMongoMigrationProcessor.Helpers;

namespace MongoMigrationWebApp.Service
{

    public class JobManager
    {
        private JobList? _jobList;
        private MigrationWorker? MigrationWorker { get; set; }

        private DateTime _lastJobHeartBeat = DateTime.MinValue;
        private string _lastJobID = string.Empty;
private readonly IConfiguration _configuration;
        private System.Threading.Timer? _resumeTimer;
        private bool _resumeExecuted = false;

        public JobManager(IConfiguration configuration)
        {
            _configuration = configuration;
            
            // Start a timer that fires once after 2 minutes
            _resumeTimer = new System.Threading.Timer(
                ResumeTimerCallback,
                null,
                TimeSpan.FromMinutes(2),
                System.Threading.Timeout.InfiniteTimeSpan
            );
        }

        private void ResumeTimerCallback(object? state)
        {
            // Ensure this only executes once
            if (_resumeExecuted)
                return;

            _resumeExecuted = true;

            try
            {
                LogToFile("Resuming migration job after application restart...");
                var migrationJobs = GetMigrations(out string errorMessage);

                LogToFile("Step 0");

                if (migrationJobs != null && migrationJobs.Count == 1)
                {

                    LogToFile($"Step1 : {migrationJobs[0].IsStarted}-{migrationJobs[0].IsCompleted} -{string.IsNullOrEmpty(migrationJobs[0].SourceConnectionString)} - {string.IsNullOrEmpty(migrationJobs[0].TargetConnectionString)} ");
                    if (migrationJobs[0].IsStarted && !migrationJobs[0].IsCompleted && string.IsNullOrEmpty(migrationJobs[0].SourceConnectionString) && string.IsNullOrEmpty(migrationJobs[0].TargetConnectionString))
                    {
                        try
                        {
                            LogToFile("Step2 : before reading config");


                            var sourceConnectionString = _configuration.GetConnectionString("SourceConnectionString");
                            var targetConnectionString = _configuration.GetConnectionString("TargetConnectionString");

                            if (sourceConnectionString != null && targetConnectionString != null)
                            {
                                LogToFile($"Step 3 :Cluster found" + sourceConnectionString.Contains("cluster"));

                                var tmpSrcEndpoint = Helper.ExtractHost(sourceConnectionString);
                                var tmpTgtEndpoint = Helper.ExtractHost(targetConnectionString);
                                if (migrationJobs[0].SourceEndpoint == tmpSrcEndpoint && migrationJobs[0].TargetEndpoint == tmpTgtEndpoint)
                                {
                                    migrationJobs[0].SourceConnectionString = sourceConnectionString;
                                    migrationJobs[0].TargetConnectionString = targetConnectionString;
                                    //ViewMigration(migrationJobs[0].Id);
                                    StartMigrationAsync(migrationJobs[0], sourceConnectionString, targetConnectionString, migrationJobs[0].NameSpaces ?? string.Empty, migrationJobs[0].JobType, Helper.IsOnline(migrationJobs[0]));
                                }
                            }
                        }
                        catch (Exception ex)
                        {
                            LogToFile($"Exception : {ex}");
                        }
                    }
                }
            }
            finally
            {
                // Dispose the timer after execution
                _resumeTimer?.Dispose();
                _resumeTimer = null;
            }
        }

        #region Logging

        /// <summary>
        /// Logs a message to the debug log file with timestamp
        /// </summary>
        /// <param name="message">The message to log</param>
        private void LogToFile(string message)
        {
            try
            {
                string timestamp = DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff");
                string logEntry = $"[{timestamp} UTC] {message}{Environment.NewLine}";
                System.IO.File.AppendAllText($"{Helper.GetWorkingFolder()}logabc.txt", logEntry);
            }
            catch
            {
                // Silently ignore logging errors to prevent application crashes
            }
        }

        #endregion 
        #region _configuration Management

        public bool UpdateConfig(OnlineMongoMigrationProcessor.MigrationSettings updated_config, out string errorMessage)
        {
            if (updated_config == null)
            {
                errorMessage = "Migration settings cannot be null.";
                return false;
            }
            // Save the updated config
            return updated_config.Save(out errorMessage);
        }

        public OnlineMongoMigrationProcessor.MigrationSettings GetConfig()
        {
            MigrationSettings config = new MigrationSettings();
            config.Load();
            return config;
        }

        #endregion 
        #region Job Management

        public JobList GetJobList()
        {
            return _jobList ??= new JobList();
        }


        //public DateTime GetJobBackupDate()
        //{
        //    return GetJobList().GetBackupDate();
        //}

       
        public List<MigrationUnit> GetMigrationUnits(MigrationJob mj)
        {            
            return Helper.GetMigrationUnitsToMigrate(mj);
        }

        //public bool RestoreJobsFromBackup(out string errorMessage)
        //{
        //    _jobList = null;
        //    _jobList = new JobList();

        //    var success = _jobList.LoadJobs(out errorMessage, true);
        //    if (!success)
        //    {
        //        return false;
        //    }

        //    if (MigrationWorker != null)
        //    {
        //        MigrationWorker.StopMigration();
        //        MigrationWorker = null;
        //    }

        //    MigrationWorker = new MigrationWorker(_jobList);

        //    errorMessage = string.Empty;
        //    return success;
        //}

        //public bool SaveJobs(out string errorMessage)
        //{
        //    return GetJobList().Save(out errorMessage);
        //}

        public MigrationJob? GetMigrationJobById(string id, bool active =true)
        {
            if(active)
            {
                if (MigrationWorker != null && MigrationWorker.IsProcessRunning(id))
                {
                    Console.WriteLine($"GetMigrationJobById from MigrationWorker.CurrentlyActiveJob");
                    var mj = MigrationWorker.CurrentlyActiveJob;
                    if(mj != null)
                        return mj;
                }
            }
            var job = MigrationJobContext.GetMigrationJob(id);
            return job;
        }

        public List<MigrationJob> GetMigrationJobs(List<string> ids)
        {
            List<MigrationJob> jobs = new List<MigrationJob>();
            foreach (var id in ids)
            {
                var job = MigrationJobContext.GetMigrationJob(id);
                if (job != null)
                {
                    jobs.Add(job);
                }
            }
            return jobs;
        }



        public List<string> GetMigrationIds(out string errorMessage)
        {
            errorMessage = string.Empty;
            bool isSucess = true;
            if (_jobList == null)
            {
                _jobList = new JobList();
                isSucess = _jobList.LoadJobList(out errorMessage);          
            }
            else
            {
                errorMessage = string.Empty;
                return _jobList.MigrationJobIds ?? new List<string>();
            }

            // create a empty list if no file found
            if (_jobList.MigrationJobIds == null)
            {
                _jobList.MigrationJobIds = new List<string>();
                if(isSucess)
                    MigrationJobContext.SaveJobList(_jobList);
            }

            return _jobList.MigrationJobIds;
        }

        public void ClearJobFiles(string jobId)
        {
            _jobList.MigrationJobIds?.Remove(jobId);
            MigrationJobContext.SaveJobList(_jobList);
;
            try
            {
                Task.Run(() =>
                {
                    System.IO.Directory.Delete($"{Helper.GetWorkingFolder()}migrationjobs\\{jobId}", true);
                    System.IO.Directory.Delete($"{Helper.GetWorkingFolder()}mongodump\\{jobId}", true);
                    
                });
            }
            catch
            {
            }

            
        }

        #endregion 
        #region Log Management

        public List<LogObject> GetMonitorMessages(string id)
        {
            //verbose messages  are only  there for active jobList so fetech from migration worker.
            if (MigrationWorker != null && MigrationWorker.IsProcessRunning(id))
                return MigrationWorker.GetMonitorMessages(id) ?? new List<LogObject>();
            else
                return new List<LogObject>();
        }

        public bool DidMigrationJobExitRecently(string jobId)
        {
            if (jobId != _lastJobID) return false;

            if (System.DateTime.UtcNow.AddSeconds(-10) > _lastJobHeartBeat)
            {
                _lastJobID = string.Empty;
                return false; ///hear beat can be max 10 seconds old
            }

            return true;
        }

        public LogBucket GetLogBucket(string id, out string fileName, out bool isLiveLog)
        {
            //Check if migration workewr is initialized and active. Return migration workers log bucket if it is.
            LogBucket? bucket = null;
            if (MigrationWorker != null && MigrationWorker.IsProcessRunning(id)) //only if worker's current job Id matches param
            {
                //Console.WriteLine($"Migration worker is running for job ID: {id}");
                bucket = MigrationWorker.GetLogBucket(id);
                _lastJobHeartBeat = DateTime.UtcNow;
                _lastJobID = id;
                isLiveLog = true;
                fileName = string.Empty;
                return bucket ?? new LogBucket { Logs = new List<LogObject>() };
            }

            //If migration worker is not running, get the log bucket from the file.Its static  
            isLiveLog = false;
            Log log = new Log();
            return log.ReadLogFile(id, out fileName) ?? new LogBucket { Logs = new List<LogObject>() };
        }

        #endregion

        #region Migration Worker Management

        public void StopMigration()
        {
            MigrationWorker?.StopMigration();
        }

        /// <summary>
        /// Initiates controlled pause - stops accepting new chunks but allows current chunks to complete
        /// </summary>
        public void ControlledPauseMigration()
        {
            MigrationWorker?.ControlledPauseMigration();
        }

        /// <summary>
        /// Checks if controlled pause is applicable for the given job type and current job state
        /// Controlled pause is only applicable during bulk copy phase, not during change stream processing
        /// </summary>
        public bool IsControlledPauseApplicable(JobType jobType, OnlineMongoMigrationProcessor.MigrationJob? job = null)
        {
            // Controlled pause only applies to chunk-based job types
            if (jobType != JobType.DumpAndRestore &&
                jobType != JobType.MongoDriver)
            {
                return false;
            }

            // If job is provided, check if bulk copy (offline phase) is still ongoing
            if (job != null)
            {
                // If offline job is completed, controlled pause is not applicable
                if (Helper.IsOfflineJobCompleted(job))
                {
                    return false;
                }
            }

            return true;
        }

        /// <summary>
        /// Gets whether controlled pause is currently requested
        /// </summary>
        public bool IsControlledPauseRequested()
        {
            return MigrationWorker?.ControlledPauseRequested ?? false;
        }

        public Task CancelMigration(string id)
        {
            //var list = GetJobList().MigrationJobs;
            //if (list != null)
            //{
            var migration = MigrationJobContext.GetMigrationJob(id);
            if (migration != null)
            {
                migration.IsCancelled = true;
                migration.IsStarted = false;
            }
            //}
            return Task.CompletedTask;
        }

        public Task StartMigrationAsync(MigrationJob job, string sourceConnectionString, string targetConnectionString, string namespacesToMigrate, OnlineMongoMigrationProcessor.Models.JobType jobType,bool trackChangeStreams)
        {
            

            MigrationWorker = new MigrationWorker(GetJobList());
            MigrationJobContext.SourceConnectionString[job.Id] = sourceConnectionString;
            MigrationJobContext.TargetConnectionString[job.Id] = targetConnectionString;

            MigrationJobContext.MigrationJob = job;

            // Fire-and-forget: UI should not block on long-running migration
            _ = MigrationWorker?.StartMigrationAsync(namespacesToMigrate, jobType, trackChangeStreams);
            
            Console.WriteLine($"Started migration for Job ID: {job.Id}");

            return Task.CompletedTask;
        }


        public void SyncBackToSource(string sourceConnectionString, string targetConnectionString, MigrationJob job)
        {
            MigrationWorker = new MigrationWorker(GetJobList());

            MigrationJobContext.SourceConnectionString[job.Id] = sourceConnectionString;
            MigrationJobContext.TargetConnectionString[job.Id] = targetConnectionString;
            MigrationJobContext.MigrationJob = job;

            MigrationWorker?.SyncBackToSource(sourceConnectionString, targetConnectionString);
        }
        public string GetRunningJobId()
        {
            return MigrationWorker?.GetRunningJobId() ?? string.Empty;
        }

        //public JobList.ConnectionAccessor SourceConnectionString
        //{
        //    get => MigrationJobContext.SourceConnectionString;
        //    set
        //    {
        //        // Optional: if you want to copy data from another accessor
        //        if (value != null)
        //        {
        //            foreach (var key in value.Keys)
        //            {
        //                MigrationJobContext.SourceConnectionString[key] = value[key];
        //            }
        //        }
        //    }
        //}

        //public JobList.ConnectionAccessor TargetConnectionString
        //{
        //    get => MigrationJobContext.TargetConnectionString;
        //    set
        //    {
        //        if (value != null)
        //        {
        //            foreach (var key in value.Keys)
        //            {
        //                MigrationJobContext.TargetConnectionString[key] = value[key];
        //            }
        //        }
        //    }
        //}

        public bool IsProcessRunning(string id)
        {
            return MigrationWorker?.IsProcessRunning(id) ?? false;
        }

        public void AdjustDumpWorkers(int newCount)
        {
            MigrationWorker?.AdjustDumpWorkers(newCount);
        }

        public void AdjustRestoreWorkers(int newCount)
        {
            MigrationWorker?.AdjustRestoreWorkers(newCount);
        }

        public void AdjustInsertionWorkers(int newCount)
        {
            MigrationWorker?.AdjustInsertionWorkers(newCount);
        }

        #endregion
                
    }
}

