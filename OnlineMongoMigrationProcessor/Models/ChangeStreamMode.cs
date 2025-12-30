using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OnlineMongoMigrationProcessor.Models
{
    public enum ChangeStreamMode
    {
        Aggressive = 0,
        Immediate = 1,
        Delayed = 2 
    }
}