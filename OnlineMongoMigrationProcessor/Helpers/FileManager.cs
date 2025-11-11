using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

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

    }
}
