using MongoDB.Bson;
using MongoDB.Driver;
using OnlineMongoMigrationProcessor.Helpers;
using System;
using System.Linq;
using System.Threading.Tasks;
using OnlineMongoMigrationProcessor.Models;
using OnlineMongoMigrationProcessor.Workers;
using OnlineMongoMigrationProcessor.Context;
using OnlineMongoMigrationProcessor.Processors;
using System.Reflection.Metadata;

namespace OnlineMongoMigrationProcessor
{
    /// <summary>
    /// DumpRestoreProcessor wraps MongoDumpRestoreCordinator to provide MigrationProcessor compatibility.
    /// All dump/restore logic is delegated to the coordinator for centralized management.
    /// </summary>
    internal class DumpRestoreProcessor : MigrationProcessor
    {
        private readonly string _jobId;
        private MongoDumpRestoreCordinator _coordinator;

        public DumpRestoreProcessor(Log log, MongoClient sourceClient, MigrationSettings config, MigrationWorker? migrationWorker = null)
            : base(log, sourceClient, config, migrationWorker)
        {
            MigrationJobContext.AddVerboseLog("DumpRestoreProcessor: Constructor called");
            _jobId = MigrationJobContext.CurrentlyActiveJob.Id ?? throw new InvalidOperationException("Job ID cannot be null");            
            
        }

        /// <summary>
        /// Callback invoked by coordinator when a migration unit completes dump/restore.
        /// Handles post-processing like change stream setup.
        /// </summary>
        private void OnMigrationUnitCompleted(MigrationUnit mu)
        {
            MigrationJobContext.AddVerboseLog($"DumpRestoreProcessor.OnMigrationUnitCompleted: mu={mu.DatabaseName}.{mu.CollectionName}");
            _log.WriteLine($"Processing completion callback for migration unit {mu.DatabaseName}. {mu.CollectionName}", LogType.Debug);

            if (MigrationJobContext.ControlledPauseRequested)
            {
                _log.WriteLine("Controlled pause active - skipping post-processing",LogType.Debug);
                return;
            }

            // Start change stream processing for the completed migration unit
            AddCollectionToChangeStreamQueue(mu.Id);

            PercentageUpdater.RemovePercentageTracker(mu.Id, false, _log);
            PercentageUpdater.RemovePercentageTracker(mu.Id, true, _log);

            _log.WriteLine($"Offline dump/restore processing completed for {mu.DatabaseName}. {mu.CollectionName}",LogType.Debug);

            // Handle post-completion logic -stop if offline, else invoke change streams
            StopOrInvokeChangeStreams();
        }

        /// <summary>
        /// Adjusts the number of dump workers at runtime.
        /// </summary>
        public void AdjustDumpWorkers(int newCount)
        {
            if (_coordinator == null)
                InitializeCoordinator();

            MigrationJobContext.AddVerboseLog($"DumpRestoreProcessor.AdjustDumpWorkers: newCount={newCount}");
            _coordinator.AdjustDumpWorkers(newCount);
        }

        /// <summary>
        /// Adjusts the number of restore workers at runtime.
        /// </summary>
        public void AdjustRestoreWorkers(int newCount)
        {
            if (_coordinator == null)
                InitializeCoordinator();

            MigrationJobContext.AddVerboseLog($"DumpRestoreProcessor.AdjustRestoreWorkers: newCount={newCount}");
            _coordinator.AdjustRestoreWorkers(newCount);
        }

        /// <summary>
        /// Adjusts the number of insertion workers per collection for mongorestore at runtime.
        /// </summary>
        public void AdjustInsertionWorkers(int newCount)
        {
            if (_coordinator == null)
                InitializeCoordinator();

            MigrationJobContext.AddVerboseLog($"DumpRestoreProcessor.AdjustInsertionWorkers: newCount={newCount}");
            _coordinator.AdjustInsertionWorkers(newCount);
        }

        private void InitializeCoordinator()
        {

            MigrationJobContext.AddVerboseLog("DumpRestoreProcessor.InitializeCoordinator: initializing coordinator");

            if (_coordinator == null)
            {
                // Create instance coordinator with completion callback
                _coordinator = new MongoDumpRestoreCordinator();
                _coordinator.Initialize(
                    _jobId,
                    _log,
                    MongoToolsFolder,
                    onMigrationUnitCompleted: OnMigrationUnitCompleted
                );
            }

        }

        private void PrepareDumpProcess(MigrationUnit mu)
        {
            // starting the regular dump and restore process
            if (!mu.BulkCopyStartedOn.HasValue || mu.BulkCopyStartedOn == DateTime.MinValue)
                mu.BulkCopyStartedOn = DateTime.UtcNow;
        }

        public override async Task<TaskResult> StartProcessAsync(string migrationUnitId, string sourceConnectionString, string targetConnectionString, string idField = "_id")
        {
            MigrationJobContext.AddVerboseLog($"DumpRestoreProcessor.StartProcessAsync: migrationUnitId={migrationUnitId}");
            // Perform initial setup required by MigrationProcessor
            MigrationJobContext.ControlledPauseRequested = false;
            ProcessRunning = true;


            var mu = MigrationJobContext.MigrationUnitsCache.GetMigrationUnit(migrationUnitId);
            mu.ParentJob = MigrationJobContext.CurrentlyActiveJob;

            var ctx = SetProcessorContext(mu, sourceConnectionString, targetConnectionString);

            PrepareDumpProcess(mu);            

            // Check if post-upload change stream processing is already in progress
            // This is a processor-level concern, not coordinator concern
            if (CheckChangeStreamAlreadyProcessingAsync(ctx))
                return TaskResult.Success;

            //initialize coordinator if not already done
            InitializeCoordinator();

            // Delegate dump/restore coordination to the coordinator
            _coordinator.StartCoordinatedProcess(ctx);

            _log.WriteLine($"Started coordinated dump/restore processing for {mu.DatabaseName}.{mu.CollectionName}", LogType.Debug);

            //// Fire and forget - monitor completion in background
            //_ = WaitForMigrationUnitCompletionAsync(mu, ctx);
            
            return TaskResult.Success;
        }

        /// <summary>
        /// Polls the coordinator until migration unit processing completes and handles post-completion logic
        /// </summary>
        //private async Task<TaskResult> WaitForMigrationUnitCompletionAsync(MigrationUnit mu, ProcessorContext ctx)
        //{
        //    // Poll coordinator until job processing completes
        //    const int pollingIntervalMs = 1000; // Poll every 1 second
        //    while (!_coordinator.IsMigrationUnitCompleted(mu.Id))
        //    {
        //        // Check for cancellation or controlled pause
        //        if (MigrationJobContext.ControlledPauseRequested)
        //        {
        //            _log.WriteLine($"Controlled pause requested - exiting wait loop for {mu.DatabaseName}.{mu.CollectionName}");
        //            return TaskResult.Canceled;
        //        }

        //        // Wait before next poll
        //        await Task.Delay(pollingIntervalMs);
        //    }

        //    PercentageUpdater.RemovePercentageTracker(mu.Id, false, _log);
        //    PercentageUpdater.RemovePercentageTracker(mu.Id, true, _log);

        //    _log.WriteLine($"Offline dump/restore processing paused/completed for {mu.DatabaseName}.{mu.CollectionName}");

        //    // Handle post-completion logic -stop if offline, else invoke change streams
        //    StopOrInvokeChangeStreams(ctx);

        //    // Return success after completion
        //    return TaskResult.Success;
        //}

        public void StopOrInvokeChangeStreams()
        {
            // Handle offline completion and post-upload CS logic
            
            if (!Helper.IsOnline(MigrationJobContext.CurrentlyActiveJob) && Helper.IsOfflineJobCompleted(MigrationJobContext.CurrentlyActiveJob))
            {
                // Don't mark as completed if this is a controlled pause
                if (!MigrationJobContext.ControlledPauseRequested)
                {
                    _log.WriteLine($"Job {MigrationJobContext.CurrentlyActiveJob.Id} Completed");
                    MigrationJobContext.CurrentlyActiveJob.IsCompleted = true;
                    MigrationJobContext.SaveMigrationJob(MigrationJobContext.CurrentlyActiveJob);
                }
                StopProcessing();
            }
            else
            {
                if (!MigrationJobContext.ControlledPauseRequested)
                {
                    _log.WriteLine($"Invoke RunChangeStreamProcessorForAllCollections.", LogType.Debug);

                    RunChangeStreamProcessorForAllCollections();
                }
            }
            
        }

        public override void InitiateControlledPause()
        {
            base.InitiateControlledPause();
            // Coordinator pause will be handled via MigrationJobContext.ControlledPauseRequested
            _log.WriteLine("DumpRestoreProcessor: Controlled pause initiated");
        }

        public new void StopProcessing(bool updateStatus = true)
        {
            _log.WriteLine("Stopping DumpRestoreProcessor...");
            
            // Stop the coordinator timer and clear manifests
            _coordinator.StopCoordinatedProcessing();
            
            // Call base implementation
            base.StopProcessing(updateStatus);
            
            _log.WriteLine("DumpRestoreProcessor stopped");
        }
    }
}
