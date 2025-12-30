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
            MigrationJobContext.AddVerboseLog($"ActiveMigrationUnitsCache.GetMigrationUnit: migrationUnitId={migrationUnitId}, cacheCount={_migrationUnits.Count}");
            MigrationUnit? mu = null;

            if (_migrationUnits.Count > 0)
                mu = _migrationUnits.Find(x => x.Id == migrationUnitId);

            if (mu == null)
            {
                mu = MigrationJobContext.GetMigrationUnitFromStorage(MigrationJobContext.CurrentlyActiveJob.Id, migrationUnitId);

                if (mu != null)
                    _migrationUnits.Add(mu);
            }

            return mu;
        }


        public  bool UpdateMigrationUnit(MigrationUnit migrationUnit)
        {
            MigrationJobContext.AddVerboseLog($"ActiveMigrationUnitsCache.UpdateMigrationUnit: migrationUnitId={migrationUnit.Id}");
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
            MigrationJobContext.AddVerboseLog($"ActiveMigrationUnitsCache.RemoveMigrationUnit: migrationUnitId={migrationUnitId}");
            _migrationUnits.RemoveAll(mu => mu.Id == migrationUnitId);
        }
    }
}
