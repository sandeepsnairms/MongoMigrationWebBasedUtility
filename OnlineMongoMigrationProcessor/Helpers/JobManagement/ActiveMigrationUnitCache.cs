using OnlineMongoMigrationProcessor.Context;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OnlineMongoMigrationProcessor.Helpers.JobManagement
{
    public class ActiveMigrationUnitsCache
    {
        public MigrationJob CurrentlyActiveJob
        {
            get => MigrationJobContext.CurrentlyActiveJob;
        }


        private List<MigrationUnit> _migrationUnits;
        public ActiveMigrationUnitsCache()
        {
            _migrationUnits = new List<MigrationUnit>();
        }


        public MigrationUnit GetMigrationUnit(string migrationUnitId)
        {
            MigrationUnit? mu = null;

            if (_migrationUnits.Count > 0)
                mu = _migrationUnits.Find(x => x.Id == migrationUnitId);

            if (mu == null)
            {
                mu = MigrationJobContext.GetMigrationUnit(CurrentlyActiveJob.Id, migrationUnitId);

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
