using MongoDB.Bson;
using MongoDB.Driver;
using OnlineMongoMigrationProcessor.Helpers;
using OnlineMongoMigrationProcessor.Models;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace OnlineMongoMigrationProcessor.Processors
{
    public abstract class MigrationProcessor
    {
        protected JobList _jobList;
        protected MigrationJob _job;
        // Fix: Make _sourceClient, _sourceCollection, _targetCollection fields and MongoToolsFolder property nullable to resolve CS8618

        protected MongoClient? _sourceClient;
        protected MongoClient? _targetClient;
        protected IMongoCollection<BsonDocument>? _sourceCollection;
        protected IMongoCollection<BsonDocument>? _targetCollection;
        protected MigrationSettings _config;
        protected CancellationTokenSource _cts;
        protected MongoChangeStreamProcessor? _changeStreamProcessor;
        protected bool _postUploadCSProcessing = false;
        protected Log _log;

        public bool ProcessRunning { get; set; }
        // Add this property to the MigrationProcessor class
        public string? MongoToolsFolder { get; set; }

        protected MigrationProcessor(Log log, JobList jobList, MigrationJob job, MongoClient sourceClient, MigrationSettings config)
        {
            _log = log;
            _jobList = jobList;
            _job = job;
            _sourceClient = sourceClient;
            _targetClient = null;
            _config = config;
            _cts = new CancellationTokenSource();            
        }

        public void StopProcessing(bool updateStatus = true)
        {

            if (_job != null)
                _job.IsStarted = false;

            _jobList?.Save();

            if (updateStatus)
                ProcessRunning = false;

            _cts?.Cancel();

            if (_changeStreamProcessor != null)
                _changeStreamProcessor.ExecutionCancelled = true;
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
                JobId = _job?.Id ?? string.Empty,
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
            if(_job.AggresiveChangeStream)
                return false; // Skip processing if aggressive change stream resume is enabled


            if (_postUploadCSProcessing)
                return true; // Skip processing if post-upload CS processing is already in progress

            if (Helper.IsOnline(_job) && Helper.IsOfflineJobCompleted(_job) && !_postUploadCSProcessing)
            {
                _postUploadCSProcessing = true; // Set flag to indicate post-upload CS processing is in progress

                if (_targetClient == null && !_job.IsSimulatedRun)
                    _targetClient = MongoClientFactory.Create(_log, ctx.TargetConnectionString);

                // Ensure _sourceClient is not null before using it
                if (_changeStreamProcessor == null && _sourceClient != null)
#pragma warning disable CS8604 // Possible null reference argument.
                    _changeStreamProcessor = new MongoChangeStreamProcessor(_log, _sourceClient, _targetClient, _jobList, _job, _config);
#pragma warning restore CS8604 // Possible null reference argument.

                if (_changeStreamProcessor != null)
                {
                    var result = _changeStreamProcessor.RunCSPostProcessingAsync(_cts);
                }
                return true;
            }

            return false;
        }

        public void AddCollectionToChangeStreamQueue(MigrationUnit mu, string targetConnectionString)
        {

            if (Helper.IsOnline(_job) && !_cts.Token.IsCancellationRequested && !_job.CSStartsAfterAllUploads )
            {
                if (_targetClient == null)
                    _targetClient = MongoClientFactory.Create(_log, targetConnectionString);

                // Ensure _sourceClient is not null before using it
                if (_changeStreamProcessor == null && _sourceClient != null)
                    _changeStreamProcessor = new MongoChangeStreamProcessor(_log, _sourceClient, _targetClient!, _jobList, _job, _config);

                _changeStreamProcessor?.AddCollectionsToProcess(mu, _cts);
            }
        }

        public void RunChangeStreamProcessorForAllCollections(string targetConnectionString)
        {

            if (Helper.IsOnline(_job))
            {
                if(_job.CSStartsAfterAllUploads && (Helper.IsOfflineJobCompleted(_job) || _job.AggresiveChangeStream) && !_postUploadCSProcessing && !_job.IsSimulatedRun)
                {
                    _postUploadCSProcessing = true; // Set flag to indicate post-upload CS processing is in progress

                    if (_targetClient == null)
                        _targetClient = MongoClientFactory.Create(_log, targetConnectionString);

                    // Ensure _sourceClient is not null before using it
                    if (_changeStreamProcessor == null && _sourceClient != null)
                        _changeStreamProcessor = new MongoChangeStreamProcessor(_log, _sourceClient, _targetClient!, _jobList, _job, _config);

                    var _ = _changeStreamProcessor?.RunCSPostProcessingAsync(_cts);
                }
                if (_job.AggresiveChangeStream && (Helper.IsOfflineJobCompleted(_job) || _job.IsSimulatedRun))
                {
                    // Process cleanup for all collection
                    _ = _changeStreamProcessor?.CleanupAggressiveCSAllCollectionsAsync();
                }
            }      

        }


        protected Task PostCopyChangeStreamProcessor(ProcessorContext ctx, MigrationUnit mu)
        {
            if (mu.RestoreComplete && mu.DumpComplete && !_cts.Token.IsCancellationRequested)
            {
                try
                {
                    // For aggressive change stream, process cleanup when collection is complete
                    if (_job.AggresiveChangeStream && Helper.IsOnline(_job) && mu.RestoreComplete)
                    {
                        AddCollectionToChangeStreamQueue(mu, ctx.TargetConnectionString);
                    }

                    if (Helper.IsOnline(_job) && !_cts.Token.IsCancellationRequested && !_job.CSStartsAfterAllUploads && !_job.AggresiveChangeStream)
                    {
                        AddCollectionToChangeStreamQueue(mu, ctx.TargetConnectionString);
                    }

                    if (!_cts.Token.IsCancellationRequested)
                    {
                        var migrationJob = _jobList.MigrationJobs?.Find(m => m.Id == ctx.JobId);
                        
                        // Check if the job is completed (all collections processed)
                        if (migrationJob != null && Helper.IsOfflineJobCompleted(migrationJob))
                        {
                            // For aggressive change stream jobs, run final cleanup for all collections
                            RunChangeStreamProcessorForAllCollections(ctx.TargetConnectionString);

                            if (!Helper.IsOnline(_job))
                            {
                                _log.WriteLine($"{migrationJob.Id} completed.");
                                migrationJob.IsCompleted = true;
                                StopProcessing(true);
                                _jobList.Save();
                            }
                        }
                        else if (!_postUploadCSProcessing && Helper.IsOnline(_job))
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


        public virtual Task<TaskResult> StartProcessAsync(MigrationUnit mu, string sourceConnectionString, string targetConnectionString, string idField = "_id")
        { return Task.FromResult(TaskResult.Success); }
    }
}
