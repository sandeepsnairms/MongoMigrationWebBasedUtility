using MongoDB.Bson;
using MongoDB.Driver;
using OnlineMongoMigrationProcessor.Helpers.JobManagement;
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
using static OnlineMongoMigrationProcessor.Helpers.Mongo.MongoHelper;
using OnlineMongoMigrationProcessor.Context;

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
        //private readonly MigrationJob CurrentlyActiveJob;
        public MigrationJob CurrentlyActiveJob
        {
            get => MigrationJobContext.CurrentlyActiveJob;
        }

        public bool ExecutionCancelled
        {
            get => _processor?.ExecutionCancelled ?? false;
            set
            {
                if (_processor != null)
                    _processor.ExecutionCancelled = value;
            }
        }

        public MongoChangeStreamProcessor(Log log, MongoClient sourceClient, MongoClient targetClient,  ActiveMigrationUnitsCache muCache, MigrationSettings config, bool syncBack = false)
        {
            _log = log;

            // Create the appropriate processor based on configuration
            _processor = CreateProcessor(log, sourceClient, targetClient,muCache, config, syncBack);
        }

        private ChangeStreamProcessor CreateProcessor(Log log, MongoClient sourceClient, MongoClient targetClient, ActiveMigrationUnitsCache muCache,  MigrationSettings config, bool syncBack)
        {
            // Determine which processor to use
            bool useServerLevel = CurrentlyActiveJob.ChangeStreamLevel == ChangeStreamLevel.Server && CurrentlyActiveJob.JobType != JobType.RUOptimizedCopy;

            if (useServerLevel)
            {
                _log.WriteLine($"{(syncBack ? "SyncBack: " : "")}Using server-level change stream processor.");
                return new ServerLevelChangeStreamProcessor(log, sourceClient, targetClient,  muCache ,config, syncBack);
            }
            else
            {
                if (CurrentlyActiveJob.ChangeStreamLevel == ChangeStreamLevel.Server && CurrentlyActiveJob.JobType == JobType.RUOptimizedCopy)
                {
                    _log.WriteLine($"{(syncBack ? "SyncBack: " : "")}RUOptimizedCopy jobs do not support server-level change streams. Using collection-level processor.");
                }
                else
                {
                    _log.WriteLine($"{(syncBack ? "SyncBack: " : "")}Using collection-level change stream processor.");
                }
                return new CollectionLevelChangeStreamProcessor(log, sourceClient, targetClient, muCache, config, syncBack);
            }
        }

        // Delegate methods to the underlying processor
        public bool AddCollectionsToProcess(string  migrationUnitId, CancellationTokenSource cts)
        {
            return _processor.AddCollectionsToProcess(migrationUnitId, cts);
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