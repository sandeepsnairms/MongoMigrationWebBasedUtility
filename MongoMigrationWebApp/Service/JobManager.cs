using Microsoft.Extensions.Configuration;
using OnlineMongoMigrationProcessor;
using OnlineMongoMigrationProcessor.Helpers;
using OnlineMongoMigrationProcessor.Models;
using OnlineMongoMigrationProcessor.Workers;
using System;
using System.Collections.Generic;


namespace MongoMigrationWebApp.Service
{

    public class JobManager
    {
        private JobList? _jobList;
        private MigrationWorker? MigrationWorker { get; set; }

        private DateTime _lastJobHeartBeat = DateTime.MinValue;
        private string _lastJobID = string.Empty;

        public JobManager(IConfiguration configuration)
        {
            System.IO.File.AppendAllText($"{Helper.GetWorkingFolder()}logabc.txt", $"Resuming migration job after application restart...");
            var migrationJobs = GetMigrations(out string errorMessage);

            System.IO.File.AppendAllText($"{Helper.GetWorkingFolder()}logabc.txt", $"Step 0");

            if (migrationJobs != null && migrationJobs.Count == 1)
            {

                System.IO.File.AppendAllText($"{Helper.GetWorkingFolder()}logabc.txt", $"Step1 : {migrationJobs[0].IsStarted}-{migrationJobs[0].IsCompleted} -{string.IsNullOrEmpty(migrationJobs[0].SourceConnectionString)} - {string.IsNullOrEmpty(migrationJobs[0].TargetConnectionString)} ");
                if (migrationJobs[0].IsStarted && !migrationJobs[0].IsCompleted && string.IsNullOrEmpty(migrationJobs[0].SourceConnectionString) && string.IsNullOrEmpty(migrationJobs[0].TargetConnectionString))
                {
                    try
                    {
                        System.IO.File.AppendAllText($"{Helper.GetWorkingFolder()}logabc.txt", $"Step2 : before reading config"); 


                        var sourceConnectionString = configuration.GetConnectionString("SourceConnectionString");
                        var targetConnectionString = configuration.GetConnectionString("TargetConnectionString");

                        if(sourceConnectionString != null && targetConnectionString != null)
                            System.IO.File.AppendAllText($"{Helper.GetWorkingFolder()}logabc.txt", $"Step 3 :Cluster found" + sourceConnectionString.Contains("cluster"));

                        var tmpSrcEndpoint = Helper.ExtractHost(sourceConnectionString);
                        var tmpTgtEndpoint = Helper.ExtractHost(targetConnectionString);
                        if (migrationJobs[0].SourceEndpoint == tmpSrcEndpoint && migrationJobs[0].TargetEndpoint == tmpTgtEndpoint)
                        {

                            migrationJobs[0].SourceConnectionString = sourceConnectionString;
                            migrationJobs[0].TargetConnectionString = targetConnectionString;
                            //ViewMigration(migrationJobs[0].Id);
                            StartMigrationAsync(migrationJobs[0], sourceConnectionString, targetConnectionString, migrationJobs[0].NameSpaces, migrationJobs[0].JobType, Helper.IsOnline(migrationJobs[0]));
                        }
                    }
                    catch(Exception ex)
                    {
                        System.IO.File.AppendAllText($"{Helper.GetWorkingFolder()}logabc.txt", $"Exception : {ex}"); //DO NOTHING

                    }
                }
            }
        }

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

        private JobList EnsureJobList()
        {
            return _jobList ??= new JobList();
        }


        public DateTime GetJobBackupDate()
        {
            return EnsureJobList().GetBackupDate();
        }


        public bool RestoreJobsFromBackup(out string errorMessage)
        {
            _jobList = null;
            _jobList = new JobList();

            var success = _jobList.LoadJobs(out errorMessage, true);
            if (!success)
            {
                return false;
            }

            if (MigrationWorker != null)
            {
                MigrationWorker.StopMigration();
                MigrationWorker = null;
            }

            MigrationWorker = new MigrationWorker(_jobList);

            errorMessage = string.Empty;
            return success;
        }

        public bool SaveJobs(out string errorMessage)
        {
            return EnsureJobList().Save(out errorMessage);
        }



        public List<MigrationJob> GetMigrations(out string errorMessage, bool force = false)
        {
            errorMessage = string.Empty;
            bool isSucess = true;
            if (_jobList == null)
            {
                _jobList = new JobList();
                isSucess = _jobList.LoadJobs(out errorMessage, false);
            }
            else
            {
                errorMessage = string.Empty;
                return _jobList.MigrationJobs ?? new List<MigrationJob>();
            }
            // Ensure we always return a non-null list
            if (_jobList.MigrationJobs == null)
            {
                _jobList.MigrationJobs = new List<MigrationJob>();
                if (isSucess || force)
                {
                    SaveJobs(out errorMessage);
                }
            }
            return _jobList.MigrationJobs;
        }

        public void ClearJobFiles(string jobId)
        {
            try
            {
                System.IO.Directory.Delete($"{Helper.GetWorkingFolder()}mongodump\\{jobId}", true);
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
            var list = EnsureJobList().MigrationJobs;
            if (list != null)
            {
                var migration = list.Find(m => m.Id == id);
                if (migration != null)
                {
                    migration.IsCancelled = true;
                    migration.IsStarted = false;
                }
            }
            return Task.CompletedTask;
        }

        public Task StartMigrationAsync(MigrationJob job, string sourceConnectionString, string targetConnectionString, string namespacesToMigrate, OnlineMongoMigrationProcessor.Models.JobType jobType,bool trackChangeStreams)
        {
            MigrationWorker = new MigrationWorker(EnsureJobList());
            // Fire-and-forget: UI should not block on long-running migration
            _ = MigrationWorker?.StartMigrationAsync(job, sourceConnectionString, targetConnectionString, namespacesToMigrate, jobType, trackChangeStreams);
            return Task.CompletedTask;
        }


        public void SyncBackToSource(string sourceConnectionString, string targetConnectionString, MigrationJob job)
        {
            MigrationWorker = new MigrationWorker(EnsureJobList());
            MigrationWorker?.SyncBackToSource(sourceConnectionString, targetConnectionString, job);
        }


        public string GetRunningJobId()
        {
            return MigrationWorker?.GetRunningJobId() ?? string.Empty;
        }

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

