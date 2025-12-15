using MongoDB.Bson;
using MongoDB.Driver;
using OnlineMongoMigrationProcessor.Context;
using OnlineMongoMigrationProcessor.Helpers.JobManagement;
using OnlineMongoMigrationProcessor.Helpers.Mongo;
using OnlineMongoMigrationProcessor.Models;
using OnlineMongoMigrationProcessor.Workers;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace OnlineMongoMigrationProcessor.Processors
{
    public abstract class MigrationProcessor
    {
     
        protected MongoClient? _sourceClient;
        protected MongoClient? _targetClient;
        protected IMongoCollection<BsonDocument>? _sourceCollection;
        protected IMongoCollection<BsonDocument>? _targetCollection;
        protected MigrationSettings _config;
        protected CancellationTokenSource _cts;
        protected MongoChangeStreamProcessor? _changeStreamProcessor;
        protected bool _postUploadCSProcessing = false;        
        protected Log _log;
        protected MigrationWorker? _migrationWorker;

        public bool ProcessRunning { get; set; }
        // Add this property to the MigrationProcessor class
        public string? MongoToolsFolder { get; set; }

        // Expose WaitForResumeTokenTaskDelegate from the change stream processor
        public Func<string, Task>? WaitForResumeTokenTaskDelegate
        {
            get => _changeStreamProcessor?.WaitForResumeTokenTaskDelegate;
            set
            {
                if (_changeStreamProcessor != null)
                    _changeStreamProcessor.WaitForResumeTokenTaskDelegate = value;
            }
        }

        protected MigrationProcessor(Log log, MongoClient sourceClient, MigrationSettings config, MigrationWorker? migrationWorker = null)
        {
            _log = log;
            _sourceClient = sourceClient;
            _targetClient = null;
            _config = config;
            _cts = new CancellationTokenSource();
            _migrationWorker = migrationWorker;
        }

        public void StopProcessing(bool updateStatus = true)
        {

            if (MigrationJobContext.CurrentlyActiveJob != null)
            {
                MigrationJobContext.CurrentlyActiveJob.IsStarted = false;
            }

            MigrationJobContext.SaveMigrationJob(MigrationJobContext.CurrentlyActiveJob);

            if (updateStatus)
                ProcessRunning = false;


            _cts?.Cancel();

            if (_changeStreamProcessor != null)
                _changeStreamProcessor.ExecutionCancelled = true;
        }

        /// <summary>
        /// Signals processor to stop accepting new work but complete current tasks
        /// </summary>
        public virtual void InitiateControlledPause()
        {
            MigrationJobContext.ControlledPauseRequested = true;
            _log.WriteLine("Controlled pause initiated in MigrationProcessor");
        }

        protected ProcessorContext SetProcessorContext(MigrationUnit mu, string sourceConnectionString, string targetConnectionString)
        {
            var databaseName = mu.DatabaseName;
            var collectionName = mu.CollectionName;
            var database = _sourceClient?.GetDatabase(databaseName);
            var collection = database?.GetCollection<BsonDocument>(collectionName);

            var context = new ProcessorContext
            {
                Item = mu,
                SourceConnectionString = sourceConnectionString,
                TargetConnectionString = targetConnectionString,
                JobId = MigrationJobContext.CurrentlyActiveJob?.Id ?? string.Empty,
                DatabaseName = databaseName,
                CollectionName = collectionName,
                Database = database!,
                Collection = collection!,
            };

            return context;
        }

        // Fix for CS8604: Ensure _sourceClient is not null before passing to MongoChangeStreamProcessor

        protected bool CheckChangeStreamAlreadyProcessingAsync(ProcessorContext ctx)
        {
            if(MigrationJobContext.CurrentlyActiveJob.ChangeStreamMode == ChangeStreamMode.Aggressive)
                return false; // Skip processing if aggressive change stream resume is enabled


            if (_postUploadCSProcessing && Helper.IsOnline(MigrationJobContext.CurrentlyActiveJob) && Helper.IsOfflineJobCompleted(MigrationJobContext.CurrentlyActiveJob))
                return true; // Skip processing if post-upload CS processing is already in progress

            if (Helper.IsOnline(MigrationJobContext.CurrentlyActiveJob) && Helper.IsOfflineJobCompleted(MigrationJobContext.CurrentlyActiveJob) && !_postUploadCSProcessing)
            {
                _postUploadCSProcessing = true; // Set flag to indicate post-upload CS processing is in progress

                if (_targetClient == null && !MigrationJobContext.CurrentlyActiveJob.IsSimulatedRun)
                    _targetClient = MongoClientFactory.Create(_log, ctx.TargetConnectionString);

                // Ensure _sourceClient is not null before using it
                if (_changeStreamProcessor == null && _sourceClient != null)
#pragma warning disable CS8604 // Possible null reference argument.
                    _changeStreamProcessor = new MongoChangeStreamProcessor(_log, _sourceClient, _targetClient, MigrationJobContext.MigrationUnitsCache, _config, false, _migrationWorker);
#pragma warning restore CS8604 // Possible null reference argument.

                if (_changeStreamProcessor != null)
                {
                    var result = _changeStreamProcessor.RunCSPostProcessingAsync(_cts);
                }
                return true;
            }

            return false;
        }

        public void AddCollectionToChangeStreamQueue(string migrationUnitId, string targetConnectionString)
        {

            if (Helper.IsOnline(MigrationJobContext.CurrentlyActiveJob) && !_cts.Token.IsCancellationRequested && MigrationJobContext.CurrentlyActiveJob.ChangeStreamMode == ChangeStreamMode.Immediate )
            {
                if (_targetClient == null)
                    _targetClient = MongoClientFactory.Create(_log, targetConnectionString);

                // Ensure _sourceClient is not null before using it
                if (_changeStreamProcessor == null && _sourceClient != null)
                    _changeStreamProcessor = new MongoChangeStreamProcessor(_log, _sourceClient, _targetClient!, MigrationJobContext.MigrationUnitsCache, _config);


                _log.WriteLine($"Adding MU:{migrationUnitId} to Change Stream processing queue", LogType.Verbose);
                _changeStreamProcessor?.AddCollectionsToProcess(migrationUnitId, _cts);
            }
        }

        public void RunChangeStreamProcessorForAllCollections(string targetConnectionString)
        {

            if (Helper.IsOnline(MigrationJobContext.CurrentlyActiveJob))
            {
                _log.WriteLine($"Checking ChangeStreamMode:{MigrationJobContext.CurrentlyActiveJob.ChangeStreamMode}, _postUploadCSProcessing:{_postUploadCSProcessing}, Offline Completed:{Helper.IsOfflineJobCompleted(MigrationJobContext.CurrentlyActiveJob)}", LogType.Verbose);
                if (MigrationJobContext.CurrentlyActiveJob.ChangeStreamMode == ChangeStreamMode.Delayed && Helper.IsOfflineJobCompleted(MigrationJobContext.CurrentlyActiveJob) && !_postUploadCSProcessing && !MigrationJobContext.CurrentlyActiveJob.IsSimulatedRun)
                {
                    _log.WriteLine("Running Change Stream Processor for all collections.", LogType.Verbose);

                    _postUploadCSProcessing = true; // Set flag to indicate post-upload CS processing is in progress

                    if (_targetClient == null)
                        _targetClient = MongoClientFactory.Create(_log, targetConnectionString);

                    // Ensure _sourceClient is not null before using it
                    if (_changeStreamProcessor == null && _sourceClient != null)
                        _changeStreamProcessor = new MongoChangeStreamProcessor(_log, _sourceClient, _targetClient!, MigrationJobContext.MigrationUnitsCache, _config, false, _migrationWorker);

                    var _ = _changeStreamProcessor?.RunCSPostProcessingAsync(_cts);
                }
                else
                {
                       _log.WriteLine("Skipping Change Stream Processor for all collections as conditions not met.", LogType.Verbose);  
                }
                
                if (MigrationJobContext.CurrentlyActiveJob.ChangeStreamMode == ChangeStreamMode.Aggressive && (Helper.IsOfflineJobCompleted(MigrationJobContext.CurrentlyActiveJob) || MigrationJobContext.CurrentlyActiveJob.IsSimulatedRun))
                {
                    _log.WriteLine("Running aggressive change stream cleanup for all collections.", LogType.Verbose);
                    // Process cleanup for all collection
                    _ = _changeStreamProcessor?.CleanupAggressiveCSAllCollectionsAsync();
                }

            }      

        }


        protected Task PostCopyChangeStreamProcessor(ProcessorContext ctx, string migratioUnitId)
        {          

            var mu= MigrationJobContext.GetMigrationUnit(MigrationJobContext.CurrentlyActiveJob.Id, migratioUnitId);
            if (MigrationJobContext.CurrentlyActiveJob == null || Helper.IsOnline(MigrationJobContext.CurrentlyActiveJob))
            {
                _log.WriteLine($"CurrentlyActiveJob is null or Offline for {mu.DatabaseName}.{mu.CollectionName}", LogType.Verbose);
                return Task.CompletedTask;
            }

            _log.WriteLine($"PostCopyChangeStreamProcessor called for {mu.DatabaseName},{mu.CollectionName}, RestoreComplete:{mu.RestoreComplete} DumpComplete:{mu.DumpComplete} ", LogType.Verbose);

            if (mu.RestoreComplete && mu.DumpComplete && !_cts.Token.IsCancellationRequested)
            {
                try
                {                   

                    // For aggressive change stream, process cleanup when collection is complete
                    if (MigrationJobContext.CurrentlyActiveJob.ChangeStreamMode == ChangeStreamMode.Aggressive && mu.RestoreComplete)
                    {
                        _log.WriteLine($"PostCopyChangeStreamProcessor adding MU:{migratioUnitId} to aggressive CS cleanup queue", LogType.Verbose);
                        AddCollectionToChangeStreamQueue(migratioUnitId, ctx.TargetConnectionString);
                    }

                    _log.WriteLine($"PostCopyChangeStreamProcessor checking immediate CS for MU:{migratioUnitId}", LogType.Verbose);
                    if (Helper.IsOnline(MigrationJobContext.CurrentlyActiveJob) && !_cts.Token.IsCancellationRequested && MigrationJobContext.CurrentlyActiveJob.ChangeStreamMode == ChangeStreamMode.Immediate)
                    {
                        _log.WriteLine($"PostCopyChangeStreamProcessor adding MU:{migratioUnitId} to immediate CS processing queue", LogType.Verbose);
                        AddCollectionToChangeStreamQueue(migratioUnitId, ctx.TargetConnectionString);
                    }

                    if (!_cts.Token.IsCancellationRequested)
                    {

                        // Check if the job is completed (all collections processed)
                        //if (Helper.IsOfflineJobCompleted(MigrationJobContext.CurrentlyActiveJob))
                        //{
                            _log.WriteLine("Run RunChangeStreamProcessorForAllCollections", LogType.Verbose);
                            // For aggressive change stream jobs, run final cleanup for all collections
                            RunChangeStreamProcessorForAllCollections(ctx.TargetConnectionString);

                            // Don't mark as completed if this is a controlled pause
                            if (!MigrationJobContext.ControlledPauseRequested)
                            {
                                _log.WriteLine($"{MigrationJobContext.CurrentlyActiveJob.Id} completed.");
                                MigrationJobContext.CurrentlyActiveJob.IsCompleted = true;
                                MigrationJobContext.SaveMigrationJob(MigrationJobContext.CurrentlyActiveJob);
                            }                                
                                
                            StopProcessing(true);
                            
                        //}
                        //else if (!_postUploadCSProcessing)
                        //{
                        //    _log.WriteLine("Process delayed change stream for uploaded collections...", LogType.Verbose);
                        //    // If CSDelayedMode is true and the offline job is completed, run post-upload change stream processing
                        //    RunChangeStreamProcessorForAllCollections(ctx.TargetConnectionString);
                        //}
                    }
                }
                catch (Exception ex)
                {
                    _log.WriteLine($"Error in PostCopyChangeStreamProcessor: {ex.Message}", LogType.Error);
                }
            }
            return Task.CompletedTask;
        }


        public virtual Task<TaskResult> StartProcessAsync(string migrationUnitId, string sourceConnectionString, string targetConnectionString, string idField = "_id")
        { return Task.FromResult(TaskResult.Success); }
    }
}
