using MongoDB.Bson;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Intrinsics.X86;
using System.Text;
using System.Threading.Tasks;

namespace OnlineMongoMigrationProcessor.Models
{
    public enum PartitionerTypes
    {
        SampleCommand,
        TimeBoundaries,
        TimeBoundariesAdjusted,
        EquidistantObjectIds
    }
}
