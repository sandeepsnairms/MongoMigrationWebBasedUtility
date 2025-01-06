using System;
using System.Collections.Generic;
using OnlineMongoMigrationProcessor;

namespace MongoMigrationWebApp.Service
{
#pragma warning disable CS8602
#pragma warning disable CS8603
#pragma warning disable CS8604

    public class JobManager
    {
        private Joblist? joblist;
        public MigrationWorker? migrationWorker;

        private List<LogObject>? LogBucket { get; set; }

        public JobManager() {

            if(joblist==null)
            {  
                joblist = new Joblist();
                joblist.Load();
            }

            if (migrationWorker == null)
            {
                migrationWorker=new MigrationWorker(joblist);
            }
                

            if (joblist.MigrationJobs == null)
            {
                joblist.MigrationJobs = new List<MigrationJob>();

                Save();
            }

        }

        public bool Save()
        {
            return joblist.Save();
        }

        public List<MigrationJob> GetMigrations() => joblist.MigrationJobs;

        public LogBucket GetLogBucket(string id) => Log.GetLogBucket(id);

        public void DisposeLogs()
        {
            Log.Dispose();
        }


        public Task CancelMigration(string id)
        {
            var migration = joblist.MigrationJobs.Find(m => m.Id == id);
            if (migration != null)
            {
                migration.IsCancelled = true;
            }
            return Task.CompletedTask;
        }

        public Task ResumeMigration(string id)
        {
            var migration = joblist.MigrationJobs.Find(m => m.Id == id);
            if (migration != null)
            {
                migration.IsCancelled = true;
            }
            return Task.CompletedTask;
        }

        public Task ViewMigration(string id)
        {
            var migration = joblist.MigrationJobs.Find(m => m.Id == id);
            if (migration != null)
            {
                migration.IsCancelled = true;
            }
            return Task.CompletedTask;
        }

        void ClearJobFiles(string jobId)
        {
            try
            {
                System.IO.Directory.Delete($"{Path.GetTempPath()}mongodump", true);
            }
            catch { }   
        }
        public string ExtractHost(string connectionString)
        {
            if (string.IsNullOrWhiteSpace(connectionString))
            {
                return string.Empty;
            }

            try
            {
                // Find the starting position of the host (after "://")
                var startIndex = connectionString.IndexOf("://") + 3;
                if (startIndex < 3 || startIndex >= connectionString.Length)
                    return string.Empty;

                // Find the end position of the host (before "/" or "?")
                var endIndex = connectionString.IndexOf("/", startIndex);
                if (endIndex == -1)
                    endIndex = connectionString.IndexOf("?", startIndex);
                if (endIndex == -1)
                    endIndex = connectionString.Length;

                // Extract and return the host
                return connectionString.Substring(startIndex, endIndex - startIndex).Split('@')[1];
            }
            catch
            {                
                return string.Empty;
            }
        }
    }

}
