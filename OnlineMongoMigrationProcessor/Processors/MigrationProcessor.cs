using MongoDB.Bson;
using MongoDB.Bson.Serialization.Conventions;
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
                
        protected Log _log;
        protected MigrationWorker? _migrationWorker;

        public bool ProcessRunning { get; set; }
        // Add this property to the MigrationProcessor class
        public string? MongoToolsFolder { get; set; }

        public bool IsChangeStreamRunning = false;

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
            MigrationJobContext.AddVerboseLog($"MigrationProcessor.StopProcessing: updateStatus={updateStatus}");

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
            _log.WriteLine("Controlled pause initiated in Migration Processor");
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

        public bool AddCollectionToChangeStreamQueue(MigrationUnit mu)
        {
            MigrationJobContext.AddVerboseLog($"MigrationProcessor.AddCollectionToChangeStreamQueue: migrationUnitId={mu.Id}");

            if (!Helper.IsOnline(MigrationJobContext.CurrentlyActiveJob))
                return false;
            
            if (_targetClient == null)
                _targetClient = MongoClientFactory.Create(_log, MigrationJobContext.TargetConnectionString[MigrationJobContext.CurrentlyActiveJob.Id]);

            // Ensure _sourceClient is not null before using it
            if (_changeStreamProcessor == null && _sourceClient != null)
                _changeStreamProcessor = new MongoChangeStreamProcessor(_log, _sourceClient, _targetClient!, MigrationJobContext.MigrationUnitsCache, _config);


            _log.WriteLine($"Adding {mu.DatabaseName}.{mu.CollectionName} to Change Stream processing queue", LogType.Debug);
            _changeStreamProcessor?.AddCollectionsToProcess(mu.Id, _cts);

            // For aggressive change stream, also trigger cleanup when collection is added
            //CleanupAggressiveCSForCollectionAsync(mu).GetAwaiter().GetResult();

            return true;
        }


        public bool RunChangeStreamProcessorForAllCollections()
        {
            MigrationJobContext.AddVerboseLog("MigrationProcessor.RunChangeStreamProcessorForAllCollections: called");

            //only once allowed per job
            if(IsChangeStreamRunning)
                return false;

            if (!Helper.IsOnline(MigrationJobContext.CurrentlyActiveJob))
                return false;

            //for delayed mode only
            if (!Helper.IsOfflineJobCompleted(MigrationJobContext.CurrentlyActiveJob) && MigrationJobContext.CurrentlyActiveJob.ChangeStreamMode == ChangeStreamMode.Delayed)
                return false;

            //for delayed mode only, at the start no collections are valid, hence IsOfflineJobCompleted gives false positive
            if (!Helper.AnyValidCollection(MigrationJobContext.CurrentlyActiveJob) && MigrationJobContext.CurrentlyActiveJob.ChangeStreamMode == ChangeStreamMode.Delayed)
                return false;            


            //only once allowed per job, checking again
            if (IsChangeStreamRunning)
                return false;

            IsChangeStreamRunning = true; // Set flag to indicate post-upload CS processing is in progress

            string targetConnStr = MigrationJobContext.TargetConnectionString[MigrationJobContext.CurrentlyActiveJob.Id];
            if (_targetClient == null && !MigrationJobContext.CurrentlyActiveJob.IsSimulatedRun)
                _targetClient = MongoClientFactory.Create(_log, targetConnStr);

            // Ensure _sourceClient is not null before using it
            if (_changeStreamProcessor == null && _sourceClient != null)
#pragma warning disable CS8604 // Possible null reference argument.
                _changeStreamProcessor = new MongoChangeStreamProcessor(_log, _sourceClient, _targetClient, MigrationJobContext.MigrationUnitsCache, _config, false, _migrationWorker);
#pragma warning restore CS8604 // Possible null reference argument.

            if (_changeStreamProcessor != null)
            {
                var result = _changeStreamProcessor.RunChangeStreamProcessorForAllCollections(_cts);
            }
            return true;            
        }

        //public async Task CleanupAggressiveCSForCollectionAsync(MigrationUnit mu)
        //{
        //    if(! Helper.IsOnline(MigrationJobContext.CurrentlyActiveJob) || MigrationJobContext.CurrentlyActiveJob.ChangeStreamMode != ChangeStreamMode.Aggressive)
        //        return;

        //    if(mu.RestoreComplete== false)
        //        return;

        //    if (_changeStreamProcessor != null)
        //    {
        //       await _changeStreamProcessor.CleanupAggressiveCSForCollectionAsync(mu);
        //    }

        //    // After cleaning up aggressive CS for this collection, check if all collections are done
        //    if (Helper.IsOfflineJobCompleted(MigrationJobContext.CurrentlyActiveJob))
        //    {
        //        _log.WriteLine("All collections have completed aggressive change stream cleanup.", LogType.Info);
        //        await _changeStreamProcessor.CleanupAggressiveTempDBAsync();
        //    }      
        //}


        /*
        protected bool CheckChangeStreamAlreadyProcessingAsync(ProcessorContext ctx)
        {
            MigrationJobContext.AddVerboseLog($"MigrationProcessor.CheckChangeStreamAlreadyProcessingAsync: ChangeStreamMode={MigrationJobContext.CurrentlyActiveJob?.ChangeStreamMode}, IsChangeStreamRunning={IsChangeStreamRunning}");

            if(MigrationJobContext.CurrentlyActiveJob.ChangeStreamMode == ChangeStreamMode.Aggressive)
                return false; // Skip processing if aggressive change stream resume is enabled

            if (IsChangeStreamRunning && Helper.IsOnline(MigrationJobContext.CurrentlyActiveJob) && Helper.IsOfflineJobCompleted(MigrationJobContext.CurrentlyActiveJob))
                return true; // Skip processing if post-upload CS processing is already in progress

            if (Helper.IsOnline(MigrationJobContext.CurrentlyActiveJob) && Helper.IsOfflineJobCompleted(MigrationJobContext.CurrentlyActiveJob) && !IsChangeStreamRunning)
            {
                IsChangeStreamRunning = true; // Set flag to indicate post-upload CS processing is in progress

                if (_targetClient == null && !MigrationJobContext.CurrentlyActiveJob.IsSimulatedRun)
                    _targetClient = MongoClientFactory.Create(_log, ctx.TargetConnectionString);

                // Ensure _sourceClient is not null before using it
                if (_changeStreamProcessor == null && _sourceClient != null)
#pragma warning disable CS8604 // Possible null reference argument.
                    _changeStreamProcessor = new MongoChangeStreamProcessor(_log, _sourceClient, _targetClient, MigrationJobContext.MigrationUnitsCache, _config, false, _migrationWorker);
#pragma warning restore CS8604 // Possible null reference argument.

                if (_changeStreamProcessor != null)
                {
                    var result = _changeStreamProcessor.RunChangeStreamProcessorForAllCollections(_cts);
                }
                return true;
            }

            return false;
        }

        public void AddCollectionToChangeStreamQueue(string migrationUnitId)
        {
            MigrationJobContext.AddVerboseLog($"MigrationProcessor.AddCollectionToChangeStreamQueue: migrationUnitId={migrationUnitId}");

            if (Helper.IsOnline(MigrationJobContext.CurrentlyActiveJob) && !_cts.Token.IsCancellationRequested && (MigrationJobContext.CurrentlyActiveJob.ChangeStreamMode == ChangeStreamMode.Immediate || MigrationJobContext.CurrentlyActiveJob.ChangeStreamMode == ChangeStreamMode.Aggressive))
            {
                if (_targetClient == null)
                    _targetClient = MongoClientFactory.Create(_log, MigrationJobContext.TargetConnectionString[MigrationJobContext.CurrentlyActiveJob.Id]);

                // Ensure _sourceClient is not null before using it
                if (_changeStreamProcessor == null && _sourceClient != null)
                    _changeStreamProcessor = new MongoChangeStreamProcessor(_log, _sourceClient, _targetClient!, MigrationJobContext.MigrationUnitsCache, _config);


                _log.WriteLine($"Adding MU:{migrationUnitId} to Change Stream processing queue", LogType.Debug);
                _changeStreamProcessor?.AddCollectionsToProcess(migrationUnitId, _cts);
            }
        }

        public void RunChangeStreamProcessorForAllCollections(bool isOfflineCompleted=true)
        {
            MigrationJobContext.AddVerboseLog("MigrationProcessor.RunChangeStreamProcessorForAllCollections: called");

            if (Helper.IsOnline(MigrationJobContext.CurrentlyActiveJob))
            {
                _log.WriteLine($"Checking ChangeStreamMode:{MigrationJobContext.CurrentlyActiveJob.ChangeStreamMode}, IsChangeStreamRunning:{IsChangeStreamRunning}, Offline Completed:{Helper.IsOfflineJobCompleted(MigrationJobContext.CurrentlyActiveJob)}",LogType.Debug);

                if (MigrationJobContext.CurrentlyActiveJob.ChangeStreamMode == ChangeStreamMode.Delayed && Helper.IsOfflineJobCompleted(MigrationJobContext.CurrentlyActiveJob) && !IsChangeStreamRunning && !MigrationJobContext.CurrentlyActiveJob.IsSimulatedRun)
                {
                    IsChangeStreamRunning = true; // Set flag to indicate post-upload CS processing is in progress

                    if (_targetClient == null)
                        _targetClient = MongoClientFactory.Create(_log, MigrationJobContext.TargetConnectionString[MigrationJobContext.CurrentlyActiveJob.Id]);

                    // Ensure _sourceClient is not null before using it
                    if (_changeStreamProcessor == null && _sourceClient != null)
                        _changeStreamProcessor = new MongoChangeStreamProcessor(_log, _sourceClient, _targetClient!, MigrationJobContext.MigrationUnitsCache, _config, false, _migrationWorker);

                    _log.WriteLine("Running RunChangeStreamProcessorForAllCollections.", LogType.Debug);

                    var _ = _changeStreamProcessor?.RunChangeStreamProcessorForAllCollections(_cts);
                }
                else
                {
                       _log.WriteLine("Skipping Change Stream Processor for all collections as conditions not met.", LogType.Debug);  
                }
                
                if (isOfflineCompleted && MigrationJobContext.CurrentlyActiveJob.ChangeStreamMode == ChangeStreamMode.Aggressive && (Helper.IsOfflineJobCompleted(MigrationJobContext.CurrentlyActiveJob) || MigrationJobContext.CurrentlyActiveJob.IsSimulatedRun))
                {
                    _log.WriteLine("Running aggressive change stream cleanup for all collections.", LogType.Debug);
                    // Process cleanup for all collection
                    _ = _changeStreamProcessor?.CleanupAggressiveCSForCollectionAsync();
                }

            }      

        }


        protected Task PostCopyChangeStreamProcessor(ProcessorContext ctx, string migratioUnitId)
        {
            
            var mu= MigrationJobContext.GetMigrationUnit(MigrationJobContext.CurrentlyActiveJob.Id, migratioUnitId);

            MigrationJobContext.AddVerboseLog($"MigrationProcessor.PostCopyChangeStreamProcessor: migratioUnitId={mu.DatabaseName}.{mu.CollectionName}");

            if (MigrationJobContext.CurrentlyActiveJob == null)
            {
                _log.WriteLine($"CurrentlyActiveJob is null or Offline for {mu.DatabaseName}.{mu.CollectionName}", LogType.Debug);
                return Task.CompletedTask;
            }

            //if offline and all offline task completed
            if(!Helper.IsOnline(MigrationJobContext.CurrentlyActiveJob) && Helper.IsOfflineJobCompleted(MigrationJobContext.CurrentlyActiveJob) )
            {
                // Don't mark as completed if this is a controlled pause
                if (!MigrationJobContext.ControlledPauseRequested)
                {
                    _log.WriteLine($"{MigrationJobContext.CurrentlyActiveJob.Id} completed.");
                    MigrationJobContext.CurrentlyActiveJob.IsCompleted = true;
                    MigrationJobContext.SaveMigrationJob(MigrationJobContext.CurrentlyActiveJob);
                }

                StopProcessing(true);
            }

            _log.WriteLine($"PostCopyChangeStreamProcessor called for {mu.DatabaseName},{mu.CollectionName}, RestoreComplete:{mu.RestoreComplete} DumpComplete:{mu.DumpComplete} ", LogType.Debug);

            if (mu.RestoreComplete && mu.DumpComplete && !_cts.Token.IsCancellationRequested)
            {
                try
                {                   

                    // For aggressive change stream, process cleanup when collection is complete
                    if (MigrationJobContext.CurrentlyActiveJob.ChangeStreamMode == ChangeStreamMode.Aggressive && mu.RestoreComplete)
                    {
                        _log.WriteLine($"PostCopyChangeStreamProcessor adding MU:{migratioUnitId} to aggressive CS cleanup queue", LogType.Debug);
                        AddCollectionToChangeStreamQueue(migratioUnitId);
                    }

 
                    if (Helper.IsOnline(MigrationJobContext.CurrentlyActiveJob) && !_cts.Token.IsCancellationRequested && MigrationJobContext.CurrentlyActiveJob.ChangeStreamMode == ChangeStreamMode.Immediate)
                    {
                        _log.WriteLine($"PostCopyChangeStreamProcessor adding MU:{migratioUnitId} to immediate CS processing queue", LogType.Debug);
                        AddCollectionToChangeStreamQueue(migratioUnitId);
                    }

                    if (!_cts.Token.IsCancellationRequested)
                    {
                        // Check if the job is completed (all collections processed)
                        if (Helper.IsOfflineJobCompleted(MigrationJobContext.CurrentlyActiveJob))
                        {
                            _log.WriteLine("Running change stream  for all collections", LogType.Info);
                            RunChangeStreamProcessorForAllCollections();
                            _log.WriteLine("Completed running change stream  for all collections", LogType.Info);

                            // Don't mark as completed if this is a controlled pause
                            if (!MigrationJobContext.ControlledPauseRequested)
                            {
                                _log.WriteLine($"{MigrationJobContext.CurrentlyActiveJob.Id} completed.");
                                MigrationJobContext.CurrentlyActiveJob.IsCompleted = true;
                                MigrationJobContext.SaveMigrationJob(MigrationJobContext.CurrentlyActiveJob);
                            }

                            StopProcessing(true);
                        }
                        
                    }
                }
                catch (Exception ex)
                {
                    _log.WriteLine($"Error in PostCopyChangeStreamProcessor: {ex.Message}", LogType.Error);
                }
            }
            return Task.CompletedTask;
        }

        */

        public void StopOfflineOrInvokeChangeStreams()
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
        public virtual Task<TaskResult> StartProcessAsync(string migrationUnitId, string sourceConnectionString, string targetConnectionString, string idField = "_id")
        { return Task.FromResult(TaskResult.Success); }
    }
}
