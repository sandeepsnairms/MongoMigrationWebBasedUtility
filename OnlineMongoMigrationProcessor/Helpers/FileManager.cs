using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;

namespace OnlineMongoMigrationProcessor.Helpers
{
    public static class FileManager
    {
        private static readonly object _writeMULock = new object();
        private static readonly object _writeJobLock = new object();
        private static readonly object _writeJobListLock = new object();

        public static bool SaveMigrationUnit(MigrationUnit unit, MigrationJob? job=null)
        {
            try
            {
                if (unit == null)
                    return false;

                if (job != null)
                {
                    unit.ParentJob = job;
                    unit.UpdateParentJob();
                }

                lock (_writeMULock)
                {
                    unit.Persist();
                }

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
                var filePath = $"{Helper.GetWorkingFolder()}migrationjobs\\{jobId}\\jobdefinition.json";
                string json = File.ReadAllText(filePath);
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
                var filePath = $"{Helper.GetWorkingFolder()}migrationjobs\\{jobId}\\{unitId}.json";
                string json = File.ReadAllText(filePath);
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
