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
                mu = MigrationJobContext.GetMigrationUnit(MigrationJobContext.CurrentlyActiveJob.Id, migrationUnitId);

                if (mu != null)
                    _migrationUnits.Add(mu);
            }

            return mu;
        }


        public  bool UpdateMigrationUnit(MigrationUnit migrationUnit)
        {
            var index = _migrationUnits.FindIndex(mu => mu.Id == migrationUnit.Id);
            if (index != -1)
            {
                _migrationUnits[index] = migrationUnit;
                return true;
            }
            return false;
        }

        public void RemoveMigrationUnit(string migrationUnitId)
        {
            _migrationUnits.RemoveAll(mu => mu.Id == migrationUnitId);
        }
    }
}
