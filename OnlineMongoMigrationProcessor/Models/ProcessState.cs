using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OnlineMongoMigrationProcessor.Models
{    public enum ProcessState
    {
        Pending,      // In queue, waiting to be processed
        Processing,   // Currently being processed by a worker
        Completed,    // Successfully completed
        Failed        // Failed after retries
    }
}
