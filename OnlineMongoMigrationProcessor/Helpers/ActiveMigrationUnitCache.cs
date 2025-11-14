using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OnlineMongoMigrationProcessor.Helpers
{
    public class ActiveMigrationUnitsCache
    {
        private readonly JobList _jobList;
        private readonly MigrationJob _job;

        private List<MigrationUnit> _migrationUnits;
        public ActiveMigrationUnitsCache(JobList jobList, MigrationJob job)
        {
            _jobList = jobList;
            _job = job;
            _migrationUnits=new List<MigrationUnit>();
        }


        public MigrationUnit GetMigrationUnit(string migrationUnitId)
        {
            MigrationUnit? mu = null;

            if (_migrationUnits.Count > 0)
                mu = _migrationUnits.Find(x => x.Id == migrationUnitId);

            if (mu == null)
            {
                mu = FileManager.GetMigrationUnit(_job.Id, migrationUnitId);

                if (mu != null)
                    _migrationUnits.Add(mu);
            }

            return mu;
        }


        public void RemoveMigrationUnit(string migrationUnitId)
        {
            _migrationUnits.RemoveAll(mu => mu.Id == migrationUnitId);
        }
    }
}
