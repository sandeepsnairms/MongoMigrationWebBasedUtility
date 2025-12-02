using MongoDB.Bson;
using MongoDB.Driver;
using OnlineMongoMigrationProcessor.Context;
using OnlineMongoMigrationProcessor.Helpers.JobManagement;
using OnlineMongoMigrationProcessor.Helpers.Mongo;
using OnlineMongoMigrationProcessor.Models;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace OnlineMongoMigrationProcessor.Processors
{
    public abstract class MigrationProcessor
    {
        //protected JobList _jobList;
        //protected MigrationJob CurrentlyActiveJob;

        public MigrationJob CurrentlyActiveJob
        {
            get => MigrationJobContext.CurrentlyActiveJob;
        }
        // Fix: Make _sourceClient, _sourceCollection, _targetCollection fields and MongoToolsFolder property nullable to resolve CS8618

        protected MongoClient? _sourceClient;
        protected MongoClient? _targetClient;
        protected IMongoCollection<BsonDocument>? _sourceCollection;
        protected IMongoCollection<BsonDocument>? _targetCollection;
        protected MigrationSettings _config;
        protected CancellationTokenSource _cts;
        protected MongoChangeStreamProcessor? _changeStreamProcessor;
        protected bool _postUploadCSProcessing = false;
        protected bool _controlledPauseRequested = false;
        protected ActiveMigrationUnitsCache _muCache;
        protected Log _log;

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

        protected MigrationProcessor(Log log, ActiveMigrationUnitsCache muCache, MongoClient sourceClient, MigrationSettings config)
        {
            _log = log;
            _muCache = muCache;
            _sourceClient = sourceClient;
            _targetClient = null;
            _config = config;
            _cts = new CancellationTokenSource();            
        }

        public void StopProcessing(bool updateStatus = true)
        {

            if (CurrentlyActiveJob != null)
            {
                CurrentlyActiveJob.IsStarted = false;
            }

            MigrationJobContext.SaveMigrationJob(CurrentlyActiveJob);

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
            _controlledPauseRequested = true;
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
                JobId = CurrentlyActiveJob?.Id ?? string.Empty,
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
            if(CurrentlyActiveJob.AggresiveChangeStream)
                return false; // Skip processing if aggressive change stream resume is enabled


            if (_postUploadCSProcessing && Helper.IsOnline(CurrentlyActiveJob) && Helper.IsOfflineJobCompleted(CurrentlyActiveJob))
                return true; // Skip processing if post-upload CS processing is already in progress

            if (Helper.IsOnline(CurrentlyActiveJob) && Helper.IsOfflineJobCompleted(CurrentlyActiveJob) && !_postUploadCSProcessing)
            {
                _postUploadCSProcessing = true; // Set flag to indicate post-upload CS processing is in progress

                if (_targetClient == null && !CurrentlyActiveJob.IsSimulatedRun)
                    _targetClient = MongoClientFactory.Create(_log, ctx.TargetConnectionString);

                // Ensure _sourceClient is not null before using it
                if (_changeStreamProcessor == null && _sourceClient != null)
#pragma warning disable CS8604 // Possible null reference argument.
                    _changeStreamProcessor = new MongoChangeStreamProcessor(_log, _sourceClient, _targetClient, _muCache, _config);
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

            if (Helper.IsOnline(CurrentlyActiveJob) && !_cts.Token.IsCancellationRequested && !CurrentlyActiveJob.CSStartsAfterAllUploads )
            {
                if (_targetClient == null)
                    _targetClient = MongoClientFactory.Create(_log, targetConnectionString);

                // Ensure _sourceClient is not null before using it
                if (_changeStreamProcessor == null && _sourceClient != null)
                    _changeStreamProcessor = new MongoChangeStreamProcessor(_log, _sourceClient, _targetClient!, _muCache, _config);

                _changeStreamProcessor?.AddCollectionsToProcess(migrationUnitId, _cts);
            }
        }

        public void RunChangeStreamProcessorForAllCollections(string targetConnectionString)
        {

            if (Helper.IsOnline(CurrentlyActiveJob))
            {
                if(CurrentlyActiveJob.CSStartsAfterAllUploads && (Helper.IsOfflineJobCompleted(CurrentlyActiveJob) || CurrentlyActiveJob.AggresiveChangeStream) && !_postUploadCSProcessing && !CurrentlyActiveJob.IsSimulatedRun)
                {
                    _postUploadCSProcessing = true; // Set flag to indicate post-upload CS processing is in progress

                    if (_targetClient == null)
                        _targetClient = MongoClientFactory.Create(_log, targetConnectionString);

                    // Ensure _sourceClient is not null before using it
                    if (_changeStreamProcessor == null && _sourceClient != null)
                        _changeStreamProcessor = new MongoChangeStreamProcessor(_log, _sourceClient, _targetClient!, _muCache, _config);

                    var _ = _changeStreamProcessor?.RunCSPostProcessingAsync(_cts);
                }
                if (CurrentlyActiveJob.AggresiveChangeStream && (Helper.IsOfflineJobCompleted(CurrentlyActiveJob) || CurrentlyActiveJob.IsSimulatedRun))
                {
                    // Process cleanup for all collection
                    _ = _changeStreamProcessor?.CleanupAggressiveCSAllCollectionsAsync();
                }
            }      

        }


        protected Task PostCopyChangeStreamProcessor(ProcessorContext ctx, string migratioUnitId)
        {
            var mu= MigrationJobContext.GetMigrationUnit(CurrentlyActiveJob.Id, migratioUnitId);
            if (mu.RestoreComplete && mu.DumpComplete && !_cts.Token.IsCancellationRequested)
            {
                try
                {
                    // For aggressive change stream, process cleanup when collection is complete
                    if (CurrentlyActiveJob.AggresiveChangeStream && Helper.IsOnline(CurrentlyActiveJob) && mu.RestoreComplete)
                    {
                        AddCollectionToChangeStreamQueue(migratioUnitId, ctx.TargetConnectionString);
                    }

                    if (Helper.IsOnline(CurrentlyActiveJob) && !_cts.Token.IsCancellationRequested && !CurrentlyActiveJob.CSStartsAfterAllUploads && !CurrentlyActiveJob.AggresiveChangeStream)
                    {
                        AddCollectionToChangeStreamQueue(migratioUnitId, ctx.TargetConnectionString);
                    }

                    if (!_cts.Token.IsCancellationRequested)
                    {
                        //var migrationJob = _jobList.GetMigrationJob(ctx.JobId);

                        // Check if the job is completed (all collections processed)
                        if (CurrentlyActiveJob != null && Helper.IsOfflineJobCompleted(CurrentlyActiveJob))
                        {
                            // For aggressive change stream jobs, run final cleanup for all collections
                            RunChangeStreamProcessorForAllCollections(ctx.TargetConnectionString);

                            if (!Helper.IsOnline(CurrentlyActiveJob))
                            {
                                // Don't mark as completed if this is a controlled pause
                                if (!_controlledPauseRequested)
                                {
                                    _log.WriteLine($"{CurrentlyActiveJob.Id} completed.");
                                    CurrentlyActiveJob.IsCompleted = true;
                                    MigrationJobContext.SaveMigrationJob(CurrentlyActiveJob);
                                }                                
                                
                                StopProcessing(true);
                            }
                        }
                        else if (!_postUploadCSProcessing && Helper.IsOnline(CurrentlyActiveJob))
                        {
                            // If CSStartsAfterAllUploads is true and the offline job is completed, run post-upload change stream processing
                            RunChangeStreamProcessorForAllCollections(ctx.TargetConnectionString);
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


        public virtual Task<TaskResult> StartProcessAsync(string migrationUnitId, string sourceConnectionString, string targetConnectionString, string idField = "_id")
        { return Task.FromResult(TaskResult.Success); }
    }
}
