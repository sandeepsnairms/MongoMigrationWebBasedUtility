using MongoDB.Bson;
using MongoDB.Driver;
using OnlineMongoMigrationProcessor.Helpers;
using OnlineMongoMigrationProcessor.Models;
using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Linq;
using System.Reflection;
using System.Reflection.Metadata;
using System.Runtime.Intrinsics.X86;
using System.Threading;
using System.Threading.Tasks;
using System.Xml;
using System.Xml.Linq;
using static OnlineMongoMigrationProcessor.MongoHelper;

#pragma warning disable CS8602 // Dereference of a possibly null reference.

namespace OnlineMongoMigrationProcessor
{
    /// <summary>
    /// Factory class that creates and manages the appropriate change stream processor based on configuration
    /// </summary>
    public class MongoChangeStreamProcessor
    {
        private ChangeStreamProcessor _processor;
        private readonly Log _log;
        private readonly MigrationJob _job;

        public bool ExecutionCancelled
        {
            get => _processor?.ExecutionCancelled ?? false;
            set
            {
                if (_processor != null)
                    _processor.ExecutionCancelled = value;
            }
        }

        public MongoChangeStreamProcessor(Log log, MongoClient sourceClient, MongoClient targetClient, JobList jobList, MigrationJob job, MigrationSettings config, bool syncBack = false)
        {
            _log = log;
            _job = job;

            // Create the appropriate processor based on configuration
            _processor = CreateProcessor(log, sourceClient, targetClient, jobList, job, config, syncBack);
        }

        private ChangeStreamProcessor CreateProcessor(Log log, MongoClient sourceClient, MongoClient targetClient, JobList jobList, MigrationJob job, MigrationSettings config, bool syncBack)
        {
            // Determine which processor to use
            bool useServerLevel = job.ChangeStreamLevel == ChangeStreamLevel.Server && job.JobType != JobType.RUOptimizedCopy;

            if (useServerLevel)
            {
                _log.WriteLine($"{(syncBack ? "SyncBack: " : "")}Using server-level change stream processor.");
                return new ServerLevelChangeStreamProcessor(log, sourceClient, targetClient, jobList, job, config, syncBack);
            }
            else
            {
                if (job.ChangeStreamLevel == ChangeStreamLevel.Server && job.JobType == JobType.RUOptimizedCopy)
                {
                    _log.WriteLine($"{(syncBack ? "SyncBack: " : "")}RUOptimizedCopy jobs do not support server-level change streams. Using collection-level processor.");
                }
                else
                {
                    _log.WriteLine($"{(syncBack ? "SyncBack: " : "")}Using collection-level change stream processor.");
                }
                return new CollectionLevelChangeStreamProcessor(log, sourceClient, targetClient, jobList, job, config, syncBack);
            }
        }

        // Delegate methods to the underlying processor
        public bool AddCollectionsToProcess(MigrationUnit mu, CancellationTokenSource cts)
        {
            return _processor.AddCollectionsToProcess(mu, cts);
        }

        public async Task RunCSPostProcessingAsync(CancellationTokenSource cts)
        {
            await _processor.RunCSPostProcessingAsync(cts);
        }

        public async Task CleanupAggressiveCSAllCollectionsAsync()
        {
            await _processor.CleanupAggressiveCSAllCollectionsAsync();
        }
    }
}