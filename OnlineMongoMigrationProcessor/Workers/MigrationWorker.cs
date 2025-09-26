using MongoDB.Bson;
using MongoDB.Driver;
using Newtonsoft.Json;
using OnlineMongoMigrationProcessor.Helpers;
using OnlineMongoMigrationProcessor.Models;
using OnlineMongoMigrationProcessor.Partitioner;
using OnlineMongoMigrationProcessor.Processors;
using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;


namespace OnlineMongoMigrationProcessor.Workers
{


    public class MigrationWorker
    {
        public bool ProcessRunning { get; set; }

        private string _toolsDestinationFolder = $"{Helper.GetWorkingFolder()}mongo-tools";
        private string _toolsLaunchFolder = string.Empty;
        private bool _migrationCancelled = false;
        private JobList _jobList;
        private MigrationJob? _job;
        private Log _log;
        private MongoClient? _sourceClient;
        private MigrationProcessor? _migrationProcessor;
        public MigrationSettings? _config;
        
        private CancellationTokenSource? _compare_cts;
        private CancellationTokenSource? _cts;

        public MigrationWorker(JobList jobList)
        {            
            _log = new Log();
            _jobList = jobList;
            jobList.SetLog(_log);
        }

        public LogBucket? GetLogBucket(string jobId)
        {
            // only for active job in migration worker
            if (_job != null && _job.Id == jobId)
                return _log.GetCurentLogBucket(jobId);
            else
                return null;
        }

        public List<LogObject>? GetVerboseMessages(string jobId)
        {
            // only for active job in migration worker
            if (_job != null && _job.Id == jobId)
                return _log.GetVerboseMessages();
            else
                return null;
        }

        public string GetRunningJobId()
        {
            if (_job != null)
            {
                if (_migrationProcessor != null && _migrationProcessor.ProcessRunning)
                {
                    return _job?.Id ?? string.Empty;
                }
                else
                    return string.Empty;
            }
            else
            {
                return string.Empty;
            }
        }

        public bool IsProcessRunning(string id)
        {
            if (id != null && _job!=null && id == _job.Id)
            {
                if (_migrationProcessor != null)
                    return _migrationProcessor.ProcessRunning;
                else
                    return ProcessRunning;
            }
            else
            {
                return false;
            }
        }

        public void StopMigration()
        {
            try
            {
                _cts?.Cancel();
                _compare_cts?.Cancel();
                _jobList.Save();
                _migrationCancelled = true;
                _migrationProcessor?.StopProcessing();
                ProcessRunning = false;
                _migrationProcessor = null;
            }
            catch { }
        }

        private async Task<TaskResult> PrepareForMigration()
        {
            if (_job == null)
                return TaskResult.FailedAfterRetries;
            if (_config == null)
                _config = new MigrationSettings();

            if (string.IsNullOrWhiteSpace(_job.SourceConnectionString))
                return TaskResult.FailedAfterRetries;

            _sourceClient = MongoClientFactory.Create(_log, _job.SourceConnectionString!, false, _config.CACertContentsForSourceServer ?? string.Empty);
            _log.WriteLine("Source client created.");
            if (_job.IsSimulatedRun)
            {
                _log.WriteLine("Simulated Run. No changes will be made to the target.");
            }
            else
            {
                if (_job.AppendMode)
                {
                    _log.WriteLine("No data from existing target collections will be deleted, and no indexes will be modified or created. Only new data will be appended.");
                }
                else
                {
                    if (_job.JobType == JobType.RUOptimizedCopy)
                    {
                        _log.WriteLine("This migration job will not transfer the indexes to the target collections. Use the schema migration script at https://aka.ms/mongoruschemamigrationscript to create the indexes on the target collections.");
                    }
                    else
                    {
                        if (_job.SkipIndexes)
                        {
                            _log.WriteLine("No indexes will be created.");
                        }
                    }
                }
            }


            if (_job.IsOnline)
            {
                _log.WriteLine("Checking if change stream is enabled on source");

                if (_job.MigrationUnits == null || _job.MigrationUnits.Count == 0)
                    return TaskResult.FailedAfterRetries;
                var retValue = await MongoHelper.IsChangeStreamEnabledAsync(_log, _config.CACertContentsForSourceServer ?? string.Empty, _job.SourceConnectionString!, _job.MigrationUnits[0]);
                _job.SourceServerVersion = retValue.Version;
                _jobList.Save();

                if (!retValue.IsCSEnabled)
                {
                    _job.IsCompleted = true;
                    StopMigration();
                    return TaskResult.Abort;
                }

            }

        _migrationProcessor?.StopProcessing(false);

        _migrationProcessor = null;
            switch (_job.JobType)
            {
                case JobType.MongoDriver:
            _migrationProcessor = new CopyProcessor(_log, _jobList, _job, _sourceClient!, _config);
                    break;
                case JobType.DumpAndRestore:
                    _migrationProcessor = new DumpRestoreProcessor(_log, _jobList, _job, _sourceClient!, _config);
                    _migrationProcessor.MongoToolsFolder = _toolsLaunchFolder;
                    break;
                case JobType.RUOptimizedCopy:
            _migrationProcessor = new RUCopyProcessor(_log, _jobList, _job, _sourceClient!, _config);
                    break;
                default:
                    _log.WriteLine($"Unknown JobType: {_job.JobType}. Defaulting to MongoDriver.", LogType.Error);
            _migrationProcessor = new CopyProcessor(_log, _jobList, _job, _sourceClient!, _config);
                    break;
            }
            _migrationProcessor.ProcessRunning = true;

            return TaskResult.Success;
        }

        // Custom exception handler delegate with logic to control retry flow
        private Task<TaskResult> Default_ExceptionHandler(Exception ex, int attemptCount, string processName, int currentBackoff)
        {
            _log.WriteLine($"{processName} attempt {attemptCount} failed. Error details:{ex}. Retrying in {currentBackoff} seconds...", LogType.Error);
            return Task.FromResult(TaskResult.Retry);
        }

        // Custom exception handler delegate with logic to control retry flow
        private Task<TaskResult> MigrateCollections_ExceptionHandler(Exception ex, int attemptCount, string processName, int currentBackoff)
        {
            if(ex is OperationCanceledException)
            {
                _log.WriteLine($"{processName} operation was paused.");
                return Task.FromResult(TaskResult.Canceled);
			}
			if (ex is MongoExecutionTimeoutException)
            {
                _log.WriteLine($"{processName} attempt {attemptCount} failed due to timeout: {ex}.", LogType.Error);
            }
            else
            {
                _log.WriteLine($"{processName} attempt {attemptCount} failed. Details:{ex}. Retrying in {currentBackoff} seconds...", LogType.Error);
            }
        
            return Task.FromResult(TaskResult.Retry);
        }

        private async Task<TaskResult> PreparePartitions(CancellationToken _cts)
        {
            bool checkedCS = false;
            bool serverLevelResumeTokenSet = false; // Track if server-level resume token has been set
            
            if (_job == null || _sourceClient == null)
                return TaskResult.FailedAfterRetries;

            // Determine if we should use server-level processing
            bool useServerLevel = _job.ChangeStreamLevel == ChangeStreamLevel.Server && _job.JobType != JobType.RUOptimizedCopy;

            var unitsForPrep = _job.MigrationUnits ?? new List<MigrationUnit>();
            foreach (var unit in unitsForPrep)
            {
                if (unit.SourceStatus == CollectionStatus.IsView)
                    continue;

                bool checkExist = await MongoHelper.CheckCollectionExistsAsync(_sourceClient!, unit.DatabaseName, unit.CollectionName);
                bool isCollection = true;
                if (checkExist)
                {
                    var ret= await MongoHelper.CheckIsCollectionAsync(_sourceClient!, unit.DatabaseName, unit.CollectionName);
                    isCollection = checkExist && ret.Item2;

                    if (isCollection == false)
                    {
                        unit.SourceStatus = CollectionStatus.IsView;
                        _log.WriteLine($"{unit.DatabaseName}.{unit.CollectionName} is not a collection. Only collections are supported for migration.", LogType.Error);
                        continue;
                    }
                }
                else
                    unit.SourceStatus = CollectionStatus.Unknown;


                if (checkExist && isCollection)
                {
                    unit.SourceStatus = CollectionStatus.OK;

                    if (unit.MigrationChunks == null || unit.MigrationChunks.Count == 0)
                    {

                        var db = _sourceClient!.GetDatabase(unit.DatabaseName);
                        var coll = db.GetCollection<BsonDocument>(unit.CollectionName);

                        unit.EstimatedDocCount = coll.EstimatedDocumentCount();

                        _ = Task.Run(() =>
                        {
                            long count = MongoHelper.GetActualDocumentCount(coll, unit);
                            unit.ActualDocCount = count;
                            _jobList?.Save();
                        }, _cts);

                    }

                    DateTime currrentTime = DateTime.UtcNow;

                    if (_job.IsOnline)
                    {
                        // Handle server-level vs collection-level change stream resume token setup
                        if (useServerLevel)
                        {
                            // For server-level, only set up resume token once per job
                            if (!serverLevelResumeTokenSet)
                            {
                                // For server-level streams, Currently not supported reset of server-level streams

                                // Run server-level resume token setup async, but only once
                                _log.WriteLine($"Setting up server-level change stream resume token for job {_job.Id}.");
                                _ = Task.Run(async () =>
                                {
                                    await MongoHelper.SetChangeStreamResumeTokenAsync(_log, _sourceClient!, _jobList, _job, unit, 300, _cts);
                                });

                                serverLevelResumeTokenSet = true;

                            }

                        }
                        else
                        {
                            // For collection-level, set up resume token for each collection (original behavior)
                            if (unit.ResetChangeStream)
                            {
                                //if  reset CS needto get the latest CS resume token synchronously
                                _log.WriteLine($"Resetting change stream for {unit.DatabaseName}.{unit.CollectionName}.");
                                await MongoHelper.SetChangeStreamResumeTokenAsync(_log, _sourceClient, _jobList, _job, unit, 15, _cts);
                            }
                            else
                            {
                                //run this job async to detect change stream resume token, if no chnage stream is detected, it will not be set and cancel in 5 minutes
                                _ = Task.Run(async () =>
                                {
                                    await MongoHelper.SetChangeStreamResumeTokenAsync(_log, _sourceClient!, _jobList, _job, unit, 300, _cts);
                                });
                            }
                        }
                    }

                    if (unit.MigrationChunks == null || unit.MigrationChunks.Count == 0)
                    {
                        List<MigrationChunk>? chunks = null;

                        if (_job.JobType == JobType.RUOptimizedCopy)
                        {
                            chunks = new RUPartitioner().CreatePartitions(_log, _sourceClient!, unit.DatabaseName, unit.CollectionName, _cts);
                        }
                        else
                        {
                            chunks = await PartitionCollection(unit.DatabaseName, unit.CollectionName, _cts, unit);
                            if (_cts.IsCancellationRequested)
                                return TaskResult.Canceled;

                            if (chunks.Count == 0)
                            {
                                _log.WriteLine($"{unit.DatabaseName}.{unit.CollectionName} has no records to migrate", LogType.Error);
                                unit.SourceStatus = CollectionStatus.NotFound;
                                continue;
                            }
                        }


                        if (!_job.IsSimulatedRun && !_job.AppendMode && !unit.TargetCreated)
                        {
                            var database = _sourceClient!.GetDatabase(unit.DatabaseName);
                            var collection = database.GetCollection<BsonDocument>(unit.CollectionName);
                            if (string.IsNullOrWhiteSpace(_job.TargetConnectionString))
                                return TaskResult.FailedAfterRetries;
                            var result = await MongoHelper.DeleteAndCopyIndexesAsync(_log, unit, _job.TargetConnectionString!, collection, _job.SkipIndexes);

                            if (_cts.IsCancellationRequested)
                                return TaskResult.Canceled;

                            if (!result)
                            {
                                return TaskResult.Retry;
                            }
                            _jobList.Save();
                            if (_job.SyncBackEnabled && !_job.IsSimulatedRun && _job.IsOnline && !checkedCS)
                            {
                                _log.WriteLine("SyncBack: Checking if change stream is enabled on target");

                                var retValue = await MongoHelper.IsChangeStreamEnabledAsync(_log, string.Empty, _job.TargetConnectionString, unit, true);
                                checkedCS = true;
                                if (!retValue.IsCSEnabled)
                                {
                                    return TaskResult.Abort;
                                }
                            }

                        }


                        if (unit.UserFilter != null && unit.UserFilter.Any())
                        {
                            _log.WriteLine($"{unit.DatabaseName}.{unit.CollectionName} has {chunks!.Count} chunk(s) with user filter : {unit.UserFilter}");
                        }
                        else
                            _log.WriteLine($"{unit.DatabaseName}.{unit.CollectionName} has {chunks!.Count} chunk(s)");

                        unit.MigrationChunks = chunks!;
                        unit.ChangeStreamStartedOn = currrentTime;

                    }

                }
                else
                {
                    if (!_cts.IsCancellationRequested)
                    {
                        if (!_job.IsSimulatedRun && !_job.AppendMode && !unit.TargetCreated)
                        {
                            try
                            {
                                //try creating empty collection with necessary indexes.
                                var database = _sourceClient!.GetDatabase(unit.DatabaseName);
                                var collection = database.GetCollection<BsonDocument>(unit.CollectionName);
                                var result = await MongoHelper.DeleteAndCopyIndexesAsync(_log, unit, _job.TargetConnectionString!, collection, _job.SkipIndexes);
                            }
                            catch
                            {
                                //do nothing
                            }
                        }
                        unit.SourceStatus = CollectionStatus.NotFound;
                        _log.WriteLine($"{unit.DatabaseName}.{unit.CollectionName} does not exist on source or has zero records", LogType.Error);
                        _jobList.Save();
                        //return TaskResult.Success;
                    }
                    else
                        return TaskResult.Canceled;
                }
            }
            _jobList.Save();
            return TaskResult.Success;
        }

        private async Task<TaskResult> MigrateJobCollections(CancellationToken ctsToken)
        {
            if (_job == null)
                return TaskResult.FailedAfterRetries;

            var unitsForMigrate = _job.MigrationUnits ?? new List<MigrationUnit>();
            foreach (var migrationUnit in unitsForMigrate)
            {
                if (_migrationCancelled) 
                    return TaskResult.Canceled;

                if (Helper.IsMigrationUnitValid(migrationUnit))
                {
                    if (migrationUnit.SourceStatus == CollectionStatus.IsView)
                        continue;

                    bool checkExist = await MongoHelper.CheckCollectionExistsAsync(_sourceClient!, migrationUnit.DatabaseName, migrationUnit.CollectionName);
                    bool isCollection = true;
                    if (checkExist)
                    {
                        var ret = await MongoHelper.CheckIsCollectionAsync(_sourceClient!, migrationUnit.DatabaseName, migrationUnit.CollectionName);
                        isCollection = checkExist && ret.Item2;

                        if (isCollection == false)
                        {
                            migrationUnit.SourceStatus = CollectionStatus.IsView;
                            _log.WriteLine($"{migrationUnit.DatabaseName}.{migrationUnit.CollectionName} is not a collection. Only collections are supported for migration.", LogType.Error);
                            continue;
                        }
                    }
                    else
                        migrationUnit.SourceStatus = CollectionStatus.Unknown;

                    if (checkExist && isCollection)
                    {
                        MongoClient? targetClient = null;
                        if (!_job.IsSimulatedRun)
                        {
                            if (string.IsNullOrWhiteSpace(_job.TargetConnectionString))
                                return TaskResult.FailedAfterRetries;

                            targetClient = MongoClientFactory.Create(_log, _job.TargetConnectionString!);

                            if (await MongoHelper.CheckCollectionExistsAsync(targetClient, migrationUnit.DatabaseName, migrationUnit.CollectionName))
                            {
                                if (!_job.CSPostProcessingStarted)
                                    _log.WriteLine($"{migrationUnit.DatabaseName}.{migrationUnit.CollectionName} already exists on the target and is ready.");
                            }
                        }
                        if (_migrationProcessor != null)
                        {
                            if (string.IsNullOrWhiteSpace(_job.SourceConnectionString) || string.IsNullOrWhiteSpace(_job.TargetConnectionString))
                                return TaskResult.Abort;

                            var result = await _migrationProcessor.StartProcessAsync(migrationUnit, _job.SourceConnectionString!, _job.TargetConnectionString!);

                            if (result == TaskResult.Success)
                            {
                                // since CS processsing has started, we can break the loop. No need to process all collections
                                if (_job.IsOnline && _job.SyncBackEnabled && (_job.CSPostProcessingStarted && !_job.AggresiveChangeStream) && Helper.IsOfflineJobCompleted(_job))
                                    break;
                            }
                            else
                                return result;
                        }
                        else
                            return TaskResult.Abort;
                    }
                    else
                    {
                        migrationUnit.SourceStatus = CollectionStatus.NotFound;
                        _log.WriteLine($"{migrationUnit.DatabaseName}.{migrationUnit.CollectionName} does not exist on source or has zero records. Created empty collection.", LogType.Error);
                        return TaskResult.Abort;
                    }
                }               
            }

            //wait till all activities are done
            //if there are errors in an actiivty it will stop independently.
            while (_migrationProcessor!=null && _migrationProcessor.ProcessRunning)
            {
                //check back after 10 sec
                Task.Delay(10000, ctsToken).Wait(ctsToken);
            }
            return TaskResult.Success; //all  actiivty completed successfully
        }



        private async Task<TaskResult> StartOnlineForJobCollections(CancellationToken ctsToken, MigrationProcessor processor)
        {
            try
            {
                if (_job == null)
                    return TaskResult.FailedAfterRetries;

                var unitsForMigrate = _job.MigrationUnits ?? new List<MigrationUnit>();
                foreach (var migrationUnit in unitsForMigrate)
                {
                    if (_migrationCancelled)
                        return TaskResult.Canceled;

                    if (Helper.IsMigrationUnitValid(migrationUnit))
                    {
                        if (await MongoHelper.CheckCollectionExistsAsync(_sourceClient!, migrationUnit.DatabaseName, migrationUnit.CollectionName))
                        {
                            processor.AddCollectionToChangeStreamQueue(migrationUnit, _job.TargetConnectionString!);
                            _log.WriteLine($"Change stream processor added {migrationUnit.DatabaseName}.{migrationUnit.CollectionName} to the monitoring queue.");
                        }
                    }
                }
                processor.RunChangeStreamProcessorForAllCollections(_job.TargetConnectionString!);
                _log.WriteLine("Change stream processor started for all collections.");

                return TaskResult.Success;
            }
            catch (Exception ex)
            {
                _log.WriteLine($"Error in starting online migration. Details: {ex}", LogType.Error);
                return TaskResult.FailedAfterRetries;
            }
        }


        public async Task StartMigrationAsync(MigrationJob job, string sourceConnectionString, string targetConnectionString, string namespacesToMigrate, JobType jobtype, bool trackChangeStreams)
        {
            _job = job;
            StopMigration(); //stop any existing
            ProcessRunning = true;


            //encoding speacial characters
            sourceConnectionString = Helper.EncodeMongoPasswordInConnectionString(sourceConnectionString);
            targetConnectionString = Helper.EncodeMongoPasswordInConnectionString(targetConnectionString);

            targetConnectionString = Helper.UpdateAppName(targetConnectionString, $"MSFTMongoWebMigration{_job.IsOnline}-" + job.Id);

            _job.TargetConnectionString = targetConnectionString;
            _job.SourceConnectionString = sourceConnectionString;

            LoadConfig();

            _migrationCancelled = false;
            _cts = new CancellationTokenSource();

            if (string.IsNullOrWhiteSpace(_job.Id)) _job.Id = Guid.NewGuid().ToString("N");
            string logfile = _log.Init(_job.Id);
            if (logfile != _job.Id)
            {
                _log.WriteLine($"Error in reading log. Orginal log backed up as {logfile}");
            }
            _log.WriteLine($"Job {_job.Id} started on {_job.StartedOn} (UTC)");


            if (_job.MigrationUnits == null)
            {
                _job.MigrationUnits = new List<MigrationUnit>();
            }

            var unitsToAdd= await Helper.PopulateJobCollectionsAsync(namespacesToMigrate, sourceConnectionString);
            if (unitsToAdd.Count > 0)
            {
                foreach (var mu in unitsToAdd)
                {
                    Helper.AddMigrationUnit(mu, job);
                }
                _jobList.Save();
            }


            if (_job.JobType==JobType.DumpAndRestore)
			{
                _toolsLaunchFolder = await Helper.EnsureMongoToolsAvailableAsync(_log, _toolsDestinationFolder, _config!);
				if (string.IsNullOrEmpty(_toolsLaunchFolder))
				{
					StopMigration();
					return;
				}
			}


			TaskResult result = await new RetryHelper().ExecuteTask(
                () => PrepareForMigration(),
                (ex, attemptCount, currentBackoff) => Default_ExceptionHandler(
                    ex, attemptCount,
                    "Preperation step", currentBackoff
                ),
                _log
            );

            if (result == TaskResult.Abort || result == TaskResult.FailedAfterRetries || _migrationCancelled)
            {
                StopMigration();
                return;
            }


           
            result = await new RetryHelper().ExecuteTask(
                () => PreparePartitions(_cts.Token),
                (ex, attemptCount, currentBackoff) => Default_ExceptionHandler(
                    ex, attemptCount,
                    "Partition step", currentBackoff
                ),
                _log
            );

            if (result == TaskResult.Abort || result == TaskResult.FailedAfterRetries || _migrationCancelled)
            {
                StopMigration();
                return;
            }
          

            //if run comparison is set by customer.
            if (_job.RunComparison)
            { 
                var compareHelper = new ComparisonHelper();
                _compare_cts = new CancellationTokenSource();
                await compareHelper.CompareRandomDocumentsAsync(_log, _jobList, _job, _config!, _compare_cts.Token);
                compareHelper = null;
                _job.RunComparison = false;
                _jobList.Save();

                _log.WriteLine("Resuming migration.");
            }
            

            if(_job.IsOnline && _job.AggresiveChangeStream)
            {
#pragma warning disable CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed
                //deliberately not awaiting this task, since it is expected to run in parallel with the migration
                StartOnlineForJobCollections(_cts.Token, _migrationProcessor!);
#pragma warning restore CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed

                await Task.Delay(30000);
            }


            result = await new RetryHelper().ExecuteTask(
                () => MigrateJobCollections(_cts.Token),
                (ex, attemptCount, currentBackoff) => MigrateCollections_ExceptionHandler(
                    ex, attemptCount,
                    "Migrate collections", currentBackoff
                ),
                _log
            );

            if (result==TaskResult.Success|| result == TaskResult.Abort || result == TaskResult.FailedAfterRetries || _migrationCancelled)
            {                
                if (result == TaskResult.Success)
                {
                    if (!_job.IsOnline)
                    {
                        _job.IsCompleted = true;
                        _jobList.Save();
                    }
                }

                StopMigration();
                return;
            }

        }

        private void LoadConfig()
        {
            if (_config == null)
                _config = new MigrationSettings();
             _config.Load();
        }


        public void SyncBackToSource(string sourceConnectionString, string targetConnectionString, MigrationJob job)
        {
            _job = job;

            ProcessRunning = true;

            LoadConfig();

            if(_log==null)
                _log = new Log();
            if (string.IsNullOrWhiteSpace(_job.Id)) _job.Id = Guid.NewGuid().ToString("N");
            string logfile = _log.Init(_job.Id);

            _log.WriteLine($"SyncBack: {_job.Id} started on {_job.StartedOn} (UTC)");
            
            job.ProcessingSyncBack = true;
            _jobList.Save();

            if (_migrationProcessor != null)
                _migrationProcessor.StopProcessing();

            _migrationProcessor = null;
            var dummySourceClient = MongoClientFactory.Create(_log, sourceConnectionString);
            _migrationProcessor = new SyncBackProcessor(_log,_jobList, _job, dummySourceClient, _config!);
            _migrationProcessor.ProcessRunning = true;
            var dummyUnit = new MigrationUnit("","", new List<MigrationChunk>());

            //if run comparison is set by customer.
            if (_job.RunComparison)
            {
                _cts = new CancellationTokenSource();
                var compareHelper = new ComparisonHelper();
                compareHelper.CompareRandomDocumentsAsync(_log, _jobList, _job, _config!, _cts.Token).GetAwaiter().GetResult();
                compareHelper = null;
                _job.RunComparison = false;
                _jobList.Save();

                _log.WriteLine("Resuming SyncBack.");
            }

            _migrationProcessor.StartProcessAsync(dummyUnit, sourceConnectionString, targetConnectionString).GetAwaiter().GetResult();
            
        }

        private async Task<List<MigrationChunk>> PartitionCollection(string databaseName, string collectionName, CancellationToken cts, MigrationUnit migrationUnit, string userFilter = "")
        {
            try
            {
                cts.ThrowIfCancellationRequested();

                if (_sourceClient == null || _config == null || _job == null)
                    throw new InvalidOperationException("Worker not initialized");

                var stats = await MongoHelper.GetCollectionStatsAsync(_sourceClient!, databaseName, collectionName);

                long documentCount = stats.DocumentCount;
                long totalCollectionSizeBytes = stats.CollectionSizeBytes;

                var database = _sourceClient!.GetDatabase(databaseName);
                var collection = database.GetCollection<BsonDocument>(collectionName);

                int totalChunks = 0;
                long minDocsInChunk = 0;

                long targetChunkSizeBytes = _config.ChunkSizeInMb * 1024 * 1024;
                var totalChunksBySize = (int)Math.Ceiling((double)totalCollectionSizeBytes / targetChunkSizeBytes);

                if (_job.JobType == JobType.DumpAndRestore)
                {
                    totalChunks = totalChunksBySize;
                    minDocsInChunk = documentCount / (totalChunks == 0 ? 1 : totalChunks);
                    _log.WriteLine($"{databaseName}.{collectionName} storage size: {totalCollectionSizeBytes}");
                }
                else
                {
                    _log.WriteLine($"{databaseName}.{collectionName} estimated document count: {documentCount}");
                    totalChunks = (int)Math.Min(SamplePartitioner.MaxSamples / SamplePartitioner.MaxSegments, documentCount / (SamplePartitioner.MaxSamples == 0 ? 1 : SamplePartitioner.MaxSamples));
                    totalChunks = Math.Max(1, totalChunks); // At least one chunk
                    totalChunks = Math.Max(totalChunks, totalChunksBySize);
                    minDocsInChunk = documentCount / (totalChunks == 0 ? 1 : totalChunks);
                }

                List<MigrationChunk> migrationChunks = new List<MigrationChunk>();

                if (totalChunks > 1 )
                {
                    _log.WriteLine($"Chunking {databaseName}.{collectionName}");

                    List<DataType> dataTypes;

                    // Check if DataTypeFor_Id is specified in the MigrationUnit
                    if (migrationUnit?.DataTypeFor_Id.HasValue == true)
                    {
                        // Use only the specified DataType and skip filtering by other data types
                        dataTypes = new List<DataType> { migrationUnit.DataTypeFor_Id.Value };
                        _log.WriteLine($"Using specified DataType for _id: {migrationUnit.DataTypeFor_Id.Value}");
                    }
                    else
                    {
                        // Use all DataTypes (original behavior)
                        dataTypes = new List<DataType> { DataType.Int, DataType.Int64, DataType.String, DataType.Object, DataType.Decimal128, DataType.Date, DataType.ObjectId };

                        if (_config.ReadBinary)
                        {
                            dataTypes.Add(DataType.BinData);
                        }
                    }

                    foreach (var dataType in dataTypes)
                    {
                        long docCountByType;
                        ChunkBoundaries? chunkBoundaries = SamplePartitioner.CreatePartitions(_log, _job.JobType == JobType.DumpAndRestore, collection, userFilter, totalChunks, dataType, minDocsInChunk, cts, migrationUnit!, out docCountByType);

                        if (docCountByType == 0 || chunkBoundaries == null) continue;

                        CreateSegments(chunkBoundaries, migrationChunks, dataType);
                    }
                }
                else
                {
                    var chunk = new MigrationChunk(string.Empty, string.Empty, DataType.Other, false, false);
                    migrationChunks.Add(chunk);
                    if(_job.JobType == JobType.MongoDriver)
                    {
                        chunk.Segments = new List<Segment>
                        {
                            new Segment { Gte = "", Lt = "", IsProcessed = false, Id = "1" }
                        };
                    }
                }

                return migrationChunks;
            }
            catch (OperationCanceledException)
            {
                return new List<MigrationChunk>();
            }
            catch (Exception ex)
            {
                _log.WriteLine($"Error chunking collection {databaseName}.{collectionName}: {ex.Message}", LogType.Error);
                return new List<MigrationChunk>();
            }
        }
              

        private void CreateSegments(ChunkBoundaries chunkBoundaries, List<MigrationChunk> migrationChunks, DataType dataType)
        {
            for (int i = 0; i < chunkBoundaries.Boundaries.Count; i++)
            {
                var (startId, endId) = GetStartEnd(true, chunkBoundaries.Boundaries[i], chunkBoundaries.Boundaries.Count, i);
                var chunk = new MigrationChunk(startId, endId, dataType, false, false);
                migrationChunks.Add(chunk);

                var boundary = chunkBoundaries.Boundaries[i];
                if (_job != null && _job.JobType == JobType.MongoDriver && (boundary.SegmentBoundaries == null || boundary.SegmentBoundaries.Count == 0))
                {
                    chunk.Segments ??= new List<Segment>();
                    chunk.Segments.Add(new Segment { Gte = startId, Lt = endId, IsProcessed = false, Id = "1" });
                }

                if (_job!.JobType == JobType.MongoDriver && boundary.SegmentBoundaries != null && boundary.SegmentBoundaries.Count > 0)
                {
                    for (int j = 0; j < boundary.SegmentBoundaries.Count; j++)
                    {
                        var segment = boundary.SegmentBoundaries[j];
                        var (segmentStartId, segmentEndId) = GetStartEnd(false, segment, boundary.SegmentBoundaries.Count, j, chunk.Lt ?? string.Empty, chunk.Gte ?? string.Empty);

                        chunk.Segments ??= new List<Segment>();
                        chunk.Segments.Add(new Segment { Gte = segmentStartId, Lt = segmentEndId, IsProcessed = false, Id = (j + 1).ToString() });
                    }
                }
            }
        }
        private Tuple<string, string> GetStartEnd(bool isChunk, Boundary boundary, int totalBoundaries, int currentIndex, string chunkLt = "", string chunkGte = "")
        {
            string startId;
            string endId;

            if (currentIndex == 0)
            {
                startId = isChunk ? "" : chunkGte;
                endId = boundary.EndId?.ToString() ?? "";
            }
            else if (currentIndex == totalBoundaries - 1)
            {
                startId = boundary.StartId?.ToString() ?? "";
                endId = isChunk ? "" : chunkLt;
            }
            else
            {
                startId = boundary.StartId?.ToString() ?? "";
                endId = boundary.EndId?.ToString() ?? "";
            }

            return Tuple.Create(startId, endId);
        }           
 
    }
}
