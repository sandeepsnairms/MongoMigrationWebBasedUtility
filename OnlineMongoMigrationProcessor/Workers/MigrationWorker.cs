using MongoDB.Bson;
using MongoDB.Driver;
using Newtonsoft.Json;
using OnlineMongoMigrationProcessor.Context;
using OnlineMongoMigrationProcessor.Helpers;
using OnlineMongoMigrationProcessor.Helpers.JobManagement;
using OnlineMongoMigrationProcessor.Helpers.Mongo;
using OnlineMongoMigrationProcessor.Models;
using OnlineMongoMigrationProcessor.Partitioner;
using OnlineMongoMigrationProcessor.Processors;
using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
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

        private string _activeJobId = string.Empty;
        private Log _log;
        private MongoClient? _sourceClient;
        private MigrationProcessor? _migrationProcessor;
        public MigrationSettings? _config;

       
        private CancellationTokenSource? _compare_cts;
        private CancellationTokenSource? _cts;

        private bool JobStarting = false;
        
        // Track resume token setup tasks per collection to enable per-collection waiting
        private Dictionary<string, Task> _resumeTokenTasksByCollection = new Dictionary<string, Task>();
       
        public MigrationWorker()
        {            
            _log = new Log();          
            MigrationJobContext.JobList.SetLog(_log);
        }

        public LogBucket? GetLogBucket(string jobId)
        {
            // only for active job in migration worker
            if (MigrationJobContext.CurrentlyActiveJob != null && MigrationJobContext.CurrentlyActiveJob.Id == jobId)
                return _log.GetCurentLogBucket(jobId);
            else
                return null;
        }

        public List<LogObject>? GetMonitorMessages(string jobId)
        {
            // only for active job in migration worker
            if (MigrationJobContext.CurrentlyActiveJob != null && MigrationJobContext.CurrentlyActiveJob.Id == jobId)
                return _log.GetMonitorMessages();
            else
                return null;
        }

        public string GetRunningJobId()
        {
            Console.WriteLine($"GetRunningJobId :{_activeJobId}");
            return _activeJobId;           
        }

        public bool IsProcessRunning(string id)
        {
            if (string.IsNullOrWhiteSpace(_activeJobId))
            {
                Console.WriteLine( $"IsProcessRunning false,  IsNullOrWhiteSpace :true");
                return false;
            }
            else
            {
                if (JobStarting && _activeJobId == id)//To handle the time between setting JobStarting and actual start of processor
                {
                    Console.WriteLine($"IsProcessRunning true, JobStarting :true");
                    return true;
                }
                else
                {
                    if (_activeJobId == id && _migrationProcessor != null && _migrationProcessor.ProcessRunning)
                    {
                        Console.WriteLine($"IsProcessRunning true,  ProcessorRunning :true");
                        return true;
                    }
                    else
                    {
                        Console.WriteLine($"IsProcessRunning false,  ProcessorRunning :false");
                        return false;
                    }
                }
                //Console.WriteLine($"IsProcessRunning :{_activeJobId == id}");
                //return _activeJobId == id;
            }                        
        }

        public void StopMigration()
        {
            try
            {
                _activeJobId=string.Empty;
                _log.WriteLine("StopMigration called - cancelling all tokens and stopping processor", LogType.Debug);
                _cts?.Cancel();
                _compare_cts?.Cancel();
                MigrationJobContext.SaveMigrationJob(MigrationJobContext.CurrentlyActiveJob);
                _migrationCancelled = true;
                _migrationProcessor?.StopProcessing();
                ProcessRunning = false;
                _migrationProcessor = null;
                MigrationJobContext.ControlledPauseRequested = false; // Reset controlled pause flag
                MigrationJobContext.MigrationUnitsCache = null;
                
                // Stop all percentage timers to prevent interference with new jobs
                ProcessExecutor.StopAllPercentageTimers();
                
                // Clear the centralized cache when stopping
                MigrationJobContext.ClearCurrentlyActiveJobCache();
                
                _log.WriteLine("StopMigration completed - all resources released", LogType.Verbose);
            }
            catch { }
        }

        /// <summary>
        /// Initiates controlled pause - stops accepting new chunks but allows current chunks to complete
        /// </summary>
        public void ControlledPauseMigration()
        {
            _log.WriteLine("Controlled pause requested - will stop after at logical point.");
            
            MigrationJobContext.ControlledPauseRequested = true;            
            _migrationProcessor?.InitiateControlledPause();
        }

        /// <summary>
        /// Adjusts the number of dump workers at runtime for DumpAndRestore jobs.
        /// </summary>
        public void AdjustDumpWorkers(int newCount)
        {
            if (_migrationProcessor is DumpRestoreProcessor dumpRestoreProcessor)
            {
                dumpRestoreProcessor.AdjustDumpWorkers(newCount);
            }
        }

        /// <summary>
        /// Adjusts the number of restore workers at runtime for DumpAndRestore jobs.
        /// </summary>
        public void AdjustRestoreWorkers(int newCount)
        {
            if (_migrationProcessor is DumpRestoreProcessor dumpRestoreProcessor)
            {
                dumpRestoreProcessor.AdjustRestoreWorkers(newCount);
            }
        }

        /// <summary>
        /// Adjusts the number of insertion workers per collection for mongorestore at runtime.
        /// </summary>
        public void AdjustInsertionWorkers(int newCount)
        {
            if (_migrationProcessor is DumpRestoreProcessor dumpRestoreProcessor)
            {
                dumpRestoreProcessor.AdjustInsertionWorkers(newCount);
            }
        }

        public async Task WaitForResumeTokenTask(string collectionKey)
        {
            if (_resumeTokenTasksByCollection.TryGetValue(collectionKey, out var task))
            {
                _log.WriteLine($"Waiting for resume token setup task to complete for {collectionKey}", LogType.Verbose);
                try
                {
                    await task;
                    _log.WriteLine($"Resume token setup task completed for {collectionKey}", LogType.Verbose);
                }
                catch (Exception ex)
                {
                    _log.WriteLine($"Resume token setup task failed for {collectionKey}: {ex.Message}", LogType.Error);
                }
            }
        }

        private async Task<TaskResult> PrepareForMigration()
        {
            _log.WriteLine("PrepareForMigration started", LogType.Verbose);
            if (MigrationJobContext.CurrentlyActiveJob == null)
                return TaskResult.FailedAfterRetries;
            if (_config == null)
                _config = new MigrationSettings();

            if (string.IsNullOrWhiteSpace(MigrationJobContext.SourceConnectionString[MigrationJobContext.CurrentlyActiveJob.Id]))
                return TaskResult.FailedAfterRetries;

            _sourceClient = MongoClientFactory.Create(_log, MigrationJobContext.SourceConnectionString[MigrationJobContext.CurrentlyActiveJob.Id], false, _config.CACertContentsForSourceServer ?? string.Empty);
            _log.WriteLine("Source client created.");
            _log.WriteLine($"Source client initialized - ConnectionString masked, JobType: {MigrationJobContext.CurrentlyActiveJob.JobType}, IsSimulated: {MigrationJobContext.CurrentlyActiveJob.IsSimulatedRun}", LogType.Debug);
            if (MigrationJobContext.CurrentlyActiveJob.IsSimulatedRun)
            {
                _log.WriteLine("Simulated Run. No changes will be made to the target.", LogType.Warning);
            }
            else
            {
                if (MigrationJobContext.CurrentlyActiveJob.AppendMode)
                {
                    _log.WriteLine("Target collections will not be dropped, and no indexes will be modified or created. Only new data will be migrated.", LogType.Warning);
                }
                else
                {
                    if (MigrationJobContext.CurrentlyActiveJob.JobType == JobType.RUOptimizedCopy)
                    {
                        _log.WriteLine("This migration job will not transfer the indexes to the target collections. Use the schema migration script at https://aka.ms/mongoruschemamigrationscript to create the indexes on the target collections.", LogType.Warning);
                    }
                    else
                    {
                        if (MigrationJobContext.CurrentlyActiveJob.SkipIndexes)
                        {
                            _log.WriteLine("No indexes will be created.", LogType.Warning);
                        }
                    }
                }
            }


            if (Helper.IsOnline(MigrationJobContext.CurrentlyActiveJob))
            {
                _log.WriteLine("Checking if change stream is enabled on source");

                if (MigrationJobContext.CurrentlyActiveJob.MigrationUnitBasics == null || MigrationJobContext.CurrentlyActiveJob.MigrationUnitBasics.Count == 0)
                    return TaskResult.FailedAfterRetries;

                var migrationUnit = MigrationJobContext.GetMigrationUnit(MigrationJobContext.CurrentlyActiveJob.Id, MigrationJobContext.CurrentlyActiveJob.MigrationUnitBasics[0].Id);
                var retValue = await MongoHelper.IsChangeStreamEnabledAsync(_log, _config.CACertContentsForSourceServer ?? string.Empty, MigrationJobContext.SourceConnectionString[MigrationJobContext.CurrentlyActiveJob.Id], migrationUnit);
                MigrationJobContext.CurrentlyActiveJob.SourceServerVersion = retValue.Version;
                MigrationJobContext.SaveMigrationJob(MigrationJobContext.CurrentlyActiveJob);

                if (!retValue.IsCSEnabled)
                {
                    MigrationJobContext.CurrentlyActiveJob.IsCompleted = true;
                    StopMigration();
                    return TaskResult.Abort;
                }

            }

        _migrationProcessor?.StopProcessing(false);

        _migrationProcessor = null;
            _log.WriteLine($"Creating migration processor for JobType: {MigrationJobContext.CurrentlyActiveJob.JobType}", LogType.Debug);
            switch (MigrationJobContext.CurrentlyActiveJob.JobType)
            {
                case JobType.MongoDriver:
                    _migrationProcessor = new CopyProcessor(_log, _sourceClient!, _config);
                    _log.WriteLine("CopyProcessor created for MongoDriver job type", LogType.Verbose);
                    break;
                case JobType.DumpAndRestore:
                    _migrationProcessor = new DumpRestoreProcessor(_log,_sourceClient!, _config);
                    _migrationProcessor.MongoToolsFolder = _toolsLaunchFolder;
                    _log.WriteLine("DumpRestoreProcessor created for DumpAndRestore job type", LogType.Verbose);
                    break;
                case JobType.RUOptimizedCopy:
            _migrationProcessor = new RUCopyProcessor(_log, _sourceClient!, _config);
                    _log.WriteLine("RUCopyProcessor created for RUOptimizedCopy job type", LogType.Verbose);
                    break;
                default:
                    _log.WriteLine($"Unknown JobType: {MigrationJobContext.CurrentlyActiveJob.JobType}. Defaulting to MongoDriver.", LogType.Error);
            _migrationProcessor = new CopyProcessor(_log, _sourceClient!, _config);
                    break;
            }
            _migrationProcessor.ProcessRunning = true;
            
            // Set the delegate to wait for resume token tasks before processing collections
            _migrationProcessor.WaitForResumeTokenTaskDelegate = WaitForResumeTokenTask;
            _log.WriteLine("WaitForResumeTokenTaskDelegate set for migration processor", LogType.Verbose);

            return TaskResult.Success;
        }

        // Custom exception handler delegate with logic to control retry flow
        private Task<TaskResult> Default_ExceptionHandler(Exception ex, int attemptCount, string processName, int currentBackoff)
        {
            _log.WriteLine($"{processName} attempt {attemptCount} failed. Error details:{ex}. Retrying in {currentBackoff} seconds...", LogType.Error);
            _log.WriteLine($"Exception type: {ex.GetType().Name}, Will retry after {currentBackoff}s", LogType.Debug);
            return Task.FromResult(TaskResult.Retry);
        }

        // Custom exception handler delegate with logic to control retry flow
        private Task<TaskResult> MigrateCollections_ExceptionHandler(Exception ex, int attemptCount, string processName, int currentBackoff)
        {
            if(ex is OperationCanceledException)
            {
                _log.WriteLine($"{processName} operation was paused", LogType.Debug);
                return Task.FromResult(TaskResult.Canceled);
			}
			if (ex is MongoExecutionTimeoutException)
            {
                _log.WriteLine($"{processName} attempt {attemptCount} failed due to timeout: {ex}.", LogType.Error);
                _log.WriteLine($"MongoExecutionTimeoutException - will retry after {currentBackoff}s", LogType.Debug);
            }
            else
            {
                _log.WriteLine($"{processName} attempt {attemptCount} failed. Details:{ex}. Retrying in {currentBackoff} seconds...", LogType.Error);
                _log.WriteLine($"Exception type: {ex.GetType().Name}, Will retry after {currentBackoff}s", LogType.Debug);
            }
        
            return Task.FromResult(TaskResult.Retry);
        }


        private async Task<TaskResult> CreatePartitionsAsync(MigrationUnit mu,  CancellationToken _cts)
        {
            _log.WriteLine($"CreatePartitionsAsync started for {mu.DatabaseName}.{mu.CollectionName}", LogType.Verbose);
            if(mu.MigrationChunks!=null && mu.MigrationChunks.Count>0)
            {
                _log.WriteLine($"Partitions already exist for {mu.DatabaseName}.{mu.CollectionName} - Count: {mu.MigrationChunks.Count}", LogType.Debug);
                return TaskResult.Success; //partitions already created
            }

			List<MigrationChunk>? chunks = null;

            DateTime currrentTime = DateTime.UtcNow;
            if (MigrationJobContext.CurrentlyActiveJob?.JobType == JobType.RUOptimizedCopy)
            {
                _log.WriteLine($"Creating RU-optimized partitions for {mu.DatabaseName}.{mu.CollectionName}", LogType.Debug);
                chunks=new RUPartitioner().CreatePartitions(_log, _sourceClient!, mu.DatabaseName, mu.CollectionName, _cts);
                //return TaskResult.Success;
            }
            else
            {
                chunks = await PartitionCollectionAsync(mu.DatabaseName, mu.CollectionName, _cts, mu);
                if (_cts.IsCancellationRequested)
                    return TaskResult.Canceled;
                if (chunks.Count == 0)
                {
                    _log.WriteLine($"{mu.DatabaseName}.{mu.CollectionName} has no records to migrate", LogType.Warning);
                }

                if (mu.UserFilter != null && mu.UserFilter.Any())
                {
                    _log.WriteLine($"{mu.DatabaseName}.{mu.CollectionName} has {chunks!.Count} chunk(s) with user filter : {mu.UserFilter}");
                }
                else
                    _log.WriteLine($"{mu.DatabaseName}.{mu.CollectionName} has {chunks!.Count} chunk(s)");

                
            }
			mu.MigrationChunks = chunks!;
            mu.ChangeStreamStartedOn = currrentTime;
            _log.WriteLine($"Partitions created successfully - Chunks: {chunks!.Count}, ChangeStreamStartedOn: {currrentTime}", LogType.Verbose);
            return TaskResult.Success;
        }

        private async Task<TaskResult> SetResumeTokens(MigrationUnit mu, CancellationToken _cts, List<Task> resumeTokenTasks)
        {
            _log.WriteLine($"SetResumeTokens called for {mu.DatabaseName}.{mu.CollectionName} - ResetChangeStream: {mu.ResetChangeStream}", LogType.Verbose);
            bool useServerLevel = MigrationJobContext.CurrentlyActiveJob.ChangeStreamLevel == ChangeStreamLevel.Server && MigrationJobContext.CurrentlyActiveJob.JobType != JobType.RUOptimizedCopy;
            if (useServerLevel)
            {
                _log.WriteLine("Server-level change stream detected, skipping collection-level resume token setup", LogType.Debug);
                return TaskResult.Success; //server-level handled separately
            }

            // For collection-level, set up resume token for each collection (original behavior)
            if (mu.ResetChangeStream)
            {
                //if reset CS need to get the latest CS resume token synchronously
                _log.WriteLine($"Resetting change stream for {mu.DatabaseName}.{mu.CollectionName}.", LogType.Info);
                _log.WriteLine($"Synchronous resume token setup initiated (30s timeout) for {mu.DatabaseName}.{mu.CollectionName}", LogType.Verbose);
                await MongoHelper.SetChangeStreamResumeTokenAsync(_log, _sourceClient, MigrationJobContext.CurrentlyActiveJob, mu, 30, _cts);
                _log.WriteLine($"Synchronous resume token setup completed for {mu.DatabaseName}.{mu.CollectionName}", LogType.Verbose);
            }
            else
            {
                _log.WriteLine($"Asynchronous resume token setup initiated (300s timeout) for {mu.DatabaseName}.{mu.CollectionName}", LogType.Verbose);
                
                string collectionKey = $"{mu.DatabaseName}.{mu.CollectionName}";
                try
                {
                    var task = Task.Run(async () =>
                    {
                        try
                        {
                            _log.WriteLine($"[ASYNC] Starting SetChangeStreamResumeTokenAsync for {mu.DatabaseName}.{mu.CollectionName}", LogType.Verbose);
                            await MongoHelper.SetChangeStreamResumeTokenAsync(_log, _sourceClient!, MigrationJobContext.CurrentlyActiveJob, mu, 300, _cts);
                            _log.WriteLine($"[ASYNC] Completed SetChangeStreamResumeTokenAsync for {mu.DatabaseName}.{mu.CollectionName}", LogType.Verbose);
                        }
                        catch (Exception ex)
                        {
                            _log.WriteLine($"[ASYNC] ERROR in SetChangeStreamResumeTokenAsync for {mu.DatabaseName}.{mu.CollectionName}: {ex.Message}", LogType.Error);
                        }
                    });
                    
                    // Store task in dictionary by collection key
                    _resumeTokenTasksByCollection[collectionKey] = task;
                    // Also add to list for tracking
                    resumeTokenTasks.Add(task);
                }
                catch (Exception ex)
                {
                    _log.WriteLine($"ERROR creating async task for SetChangeStreamResumeTokenAsync for {mu.DatabaseName}.{mu.CollectionName}: {ex.Message}", LogType.Error);
                }
            }
           
            return TaskResult.Success;
        }

        private async Task<TaskResult> PreparePartitionsAsync(CancellationToken _cts, bool skipPartitioning)
        {
            _log.WriteLine($"PreparePartitionsAsync started - SkipPartitioning: {skipPartitioning}", LogType.Debug);
            bool checkedCS = false;
            bool serverLevelResumeTokenSet = false; // Track if server-level resume token has been set
            List<Task> resumeTokenTasks = new List<Task>(); // Track all async resume token setup tasks
            
            if (MigrationJobContext.CurrentlyActiveJob == null || _sourceClient == null)
                return TaskResult.FailedAfterRetries;

            // Determine if we should use server-level processing
            bool useServerLevel = MigrationJobContext.CurrentlyActiveJob.ChangeStreamLevel == ChangeStreamLevel.Server && MigrationJobContext.CurrentlyActiveJob.JobType != JobType.RUOptimizedCopy;
            _log.WriteLine($"Change stream level determination - UseServerLevel: {useServerLevel}, ChangeStreamLevel: {MigrationJobContext.CurrentlyActiveJob.ChangeStreamLevel}, JobType: {MigrationJobContext.CurrentlyActiveJob.JobType}", LogType.Verbose);


            var unitsForPrep = Helper.GetMigrationUnitsToMigrate(MigrationJobContext.CurrentlyActiveJob);

            _log.WriteLine($"Processing {unitsForPrep.Count} migration units for preparation", LogType.Debug);

            foreach (var mu in unitsForPrep)
            {
                if (HandleControlPause())
                    return TaskResult.Canceled;

                if (mu.SourceStatus == CollectionStatus.IsView)
                    continue;

                bool checkExist;
                if (MigrationJobContext.CurrentlyActiveJob.JobType== JobType.RUOptimizedCopy)
                    checkExist = await MongoHelper.CheckRUCollectionExistsAsync(_sourceClient!, mu.DatabaseName, mu.CollectionName);
                else
                    checkExist = await MongoHelper.CheckCollectionExistsAsync(_sourceClient!, mu.DatabaseName, mu.CollectionName);

                bool isCollection = true;
                if (checkExist)
                {
                    (bool Exits, bool IsCollection) ret;
                    try
                    {
                        ret = await MongoHelper.CheckIsCollectionAsync(_sourceClient, mu.DatabaseName, mu.CollectionName); //fails if connnected to secondary
                        isCollection = checkExist && ret.Item2;
                    }
                    catch
                    {
                        isCollection=true;
                    }                   

                    if (isCollection == false)
                    {
                        mu.SourceStatus = CollectionStatus.IsView;
                        _log.WriteLine($"{mu.DatabaseName}.{mu.CollectionName} is not a collection. Only collections are supported for migration.", LogType.Warning);                        
                        continue;
                    }
                }
                else
                    mu.SourceStatus = CollectionStatus.Unknown;


                if (checkExist && isCollection)
                {
                    mu.SourceStatus = CollectionStatus.OK;

                    MigrationJobContext.SaveMigrationUnit(mu,true);

                    if (mu.MigrationChunks == null || mu.MigrationChunks.Count == 0)
                    {

                        var db = _sourceClient!.GetDatabase(mu.DatabaseName);
                        var coll = db.GetCollection<BsonDocument>(mu.CollectionName);

                        mu.EstimatedDocCount = coll.EstimatedDocumentCount();

                        _ = Task.Run(() =>
                        {
                            long count = MongoHelper.GetActualDocumentCount(coll, mu);
                            mu.ActualDocCount = count;
                            MigrationJobContext.SaveMigrationUnit(mu,false);
                        }, _cts);

                    }

                    

                    if (Helper.IsOnline(MigrationJobContext.CurrentlyActiveJob))
                    {
                        // Handle server-level vs collection-level change stream resume token setup
                        if (useServerLevel)
                        {
                            // For server-level, only set up resume token once per job
                            if (!serverLevelResumeTokenSet)
                            {
                                // For server-level streams, Currently not supported reset of server-level streams

                                // Run server-level resume token setup async, but only once
                                _log.WriteLine($"Setting up server-level change stream resume token for job {MigrationJobContext.CurrentlyActiveJob.Id}.");
                                _ = Task.Run(async () =>
                                {
                                    await MongoHelper.SetChangeStreamResumeTokenAsync(_log, _sourceClient!, MigrationJobContext.CurrentlyActiveJob, mu, 300, _cts);
                                });

                                serverLevelResumeTokenSet = true;

                            }

                        }
                       
                    }

                    if (mu.MigrationChunks == null || mu.MigrationChunks.Count == 0)
                    {                     

                        if (!MigrationJobContext.CurrentlyActiveJob.IsSimulatedRun && !MigrationJobContext.CurrentlyActiveJob.AppendMode && !mu.TargetCreated)
                        {
                            var database = _sourceClient!.GetDatabase(mu.DatabaseName);
                            var collection = database.GetCollection<BsonDocument>(mu.CollectionName);
                            if (string.IsNullOrWhiteSpace(MigrationJobContext.TargetConnectionString[MigrationJobContext.CurrentlyActiveJob.Id]))
                                return TaskResult.FailedAfterRetries;
                            var result = await MongoHelper.DeleteAndCopyIndexesAsync(_log, mu, MigrationJobContext.TargetConnectionString[MigrationJobContext.CurrentlyActiveJob.Id], collection, MigrationJobContext.CurrentlyActiveJob.SkipIndexes);

                            if (_cts.IsCancellationRequested)
                                return TaskResult.Canceled;

                            if (!result)
                            {
                                return TaskResult.Retry;
                            }
                            MigrationJobContext.SaveMigrationUnit(mu,false);
                            if (MigrationJobContext.CurrentlyActiveJob.SyncBackEnabled && !MigrationJobContext.CurrentlyActiveJob.IsSimulatedRun && Helper.IsOnline(MigrationJobContext.CurrentlyActiveJob) && !checkedCS)
                            {
                                _log.WriteLine("SyncBack: Checking if change stream is enabled on target");

                                var retValue = await MongoHelper.IsChangeStreamEnabledAsync(_log, string.Empty, MigrationJobContext.TargetConnectionString[MigrationJobContext.CurrentlyActiveJob.Id], mu, true);
                                checkedCS = true;
                                if (!retValue.IsCSEnabled)
                                {
                                    return TaskResult.Abort;
                                }
                            }

                        }

                        
                        if (!skipPartitioning)
                        {
                            var ret= await CreatePartitionsAsync(mu, _cts); 
                            if(ret!= TaskResult.Success)
                                return ret;
                        }

                        

                    }

                }
                else
                {
                    if (!_cts.IsCancellationRequested)
                    {
                        if (!MigrationJobContext.CurrentlyActiveJob.IsSimulatedRun && !MigrationJobContext.CurrentlyActiveJob.AppendMode && !mu.TargetCreated)
                        {
                            try
                            {
                                //try creating empty collection with necessary indexes.
                                var database = _sourceClient!.GetDatabase(mu.DatabaseName);
                                var collection = database.GetCollection<BsonDocument>(mu.CollectionName);
                                var result = await MongoHelper.DeleteAndCopyIndexesAsync(_log, mu, MigrationJobContext.TargetConnectionString[MigrationJobContext.CurrentlyActiveJob.Id], collection, MigrationJobContext.CurrentlyActiveJob.SkipIndexes);
                            }
                            catch
                            {
                                //do nothing
                            }
                        }
                        mu.SourceStatus = CollectionStatus.NotFound;
                        _log.WriteLine($"{mu.DatabaseName}.{mu.CollectionName} does not exist on source", LogType.Error);
                        MigrationJobContext.SaveMigrationUnit(mu,true);
                        //return TaskResult.Success;
                    }
                    else
                        return TaskResult.Canceled;
                }
                
                
                MigrationJobContext.SaveMigrationUnit(mu,false);
            }
            
            // Note: No need to wait for all resume token tasks here.
            // The change stream processor checks mu.ResumeToken for each collection before processing.
            // This allows collections with ready tokens to start processing immediately.
            
            return TaskResult.Success;
        }

        private async Task<TaskResult> MigrateJobCollections(CancellationToken ctsToken)
        {
            _log.WriteLine("MigrateJobCollections started", LogType.Debug);
            if (MigrationJobContext.CurrentlyActiveJob == null)
                return TaskResult.FailedAfterRetries;
            
            //var unitsForMigrate = Helper.GetMigrationUnitsToMigrate(_jobList, MigrationJobContext.CurrentlyActiveJob);
           
            // Track resume token tasks for this migration path as well
            List<Task> resumeTokenTasks = new List<Task>();

            _log.WriteLine($"Processing {MigrationJobContext.CurrentlyActiveJob.MigrationUnitBasics.Count} migration units", LogType.Verbose);
            foreach (var mub in MigrationJobContext.CurrentlyActiveJob.MigrationUnitBasics)
            {
                if (_migrationCancelled) 
                    return TaskResult.Canceled;

                if (HandleControlPause())
                    return TaskResult.Canceled;

                var migrationUnit = MigrationJobContext.MigrationUnitsCache.GetMigrationUnit(mub.Id);
                migrationUnit.ParentJob = MigrationJobContext.CurrentlyActiveJob;
                if (Helper.IsMigrationUnitValid(migrationUnit))
                {
                    if (migrationUnit.SourceStatus == CollectionStatus.IsView)
                        continue;

                    bool checkExist;
                    if (MigrationJobContext.CurrentlyActiveJob.JobType == JobType.RUOptimizedCopy)
                        checkExist = await MongoHelper.CheckRUCollectionExistsAsync(_sourceClient!, migrationUnit.DatabaseName, migrationUnit.CollectionName);
                    else
                        checkExist = await MongoHelper.CheckCollectionExistsAsync(_sourceClient!, migrationUnit.DatabaseName, migrationUnit.CollectionName);

                    bool isCollection = true;
                    if (checkExist)
                    {
                        var ret = await MongoHelper.CheckIsCollectionAsync(_sourceClient!, migrationUnit.DatabaseName, migrationUnit.CollectionName);
                        isCollection = checkExist && ret.IsCollection;

                        if (isCollection == false)
                        {
                            migrationUnit.SourceStatus = CollectionStatus.IsView;
                            _log.WriteLine($"{migrationUnit.DatabaseName}.{migrationUnit.CollectionName} is not a collection. Only collections are supported for migration.", LogType.Warning);
                            continue;
                        }
                    }
                    else
                        migrationUnit.SourceStatus = CollectionStatus.Unknown;

                    if (checkExist && isCollection)
                    {
                        MongoClient? targetClient = null;
                        if (!MigrationJobContext.CurrentlyActiveJob.IsSimulatedRun)
                        {
                            if (string.IsNullOrWhiteSpace(MigrationJobContext.TargetConnectionString[MigrationJobContext.CurrentlyActiveJob.Id]))
                                return TaskResult.FailedAfterRetries;

                            targetClient = MongoClientFactory.Create(_log, MigrationJobContext.TargetConnectionString[MigrationJobContext.CurrentlyActiveJob.Id]);

                            if (Helper.IsRU(MigrationJobContext.TargetConnectionString[MigrationJobContext.CurrentlyActiveJob.Id]))
                                checkExist = await MongoHelper.CheckRUCollectionExistsAsync(_sourceClient!, migrationUnit.DatabaseName, migrationUnit.CollectionName);
                            else
                                checkExist = await MongoHelper.CheckCollectionExistsAsync(_sourceClient!, migrationUnit.DatabaseName, migrationUnit.CollectionName);

                            if (checkExist)
                            {
                                if (!MigrationJobContext.CurrentlyActiveJob.CSPostProcessingStarted)
                                    _log.WriteLine($"{migrationUnit.DatabaseName}.{migrationUnit.CollectionName} already exists on the target and is ready.", LogType.Debug);
                            }
                        }
                        if (_migrationProcessor != null)
                        {
                            // Create Chunks , will return sucess if already created
                            var createPartitionsResult = await CreatePartitionsAsync(migrationUnit, ctsToken);
                            if (createPartitionsResult != TaskResult.Success)
                                return createPartitionsResult;

                            if (HandleControlPause())
                                return TaskResult.Canceled;


                            if (Helper.IsOnline(MigrationJobContext.CurrentlyActiveJob))
                            {
                                // For online jobs, ensure change stream resume tokens are set
                                var setResumeResult = await SetResumeTokens(migrationUnit, ctsToken, resumeTokenTasks);
                                if (setResumeResult != TaskResult.Success)
                                    return setResumeResult;
                            }

                            if (HandleControlPause())
                                return TaskResult.Canceled;

                            if (string.IsNullOrWhiteSpace(MigrationJobContext.SourceConnectionString[MigrationJobContext.CurrentlyActiveJob.Id]) || string.IsNullOrWhiteSpace(MigrationJobContext.TargetConnectionString[MigrationJobContext.CurrentlyActiveJob.Id]))
                                return TaskResult.Abort;

                            _log.WriteLine($"Starting migration processor for {migrationUnit.DatabaseName}.{migrationUnit.CollectionName}", LogType.Debug);
                            var result = await _migrationProcessor.StartProcessAsync(migrationUnit.Id, MigrationJobContext.SourceConnectionString[MigrationJobContext.CurrentlyActiveJob.Id], MigrationJobContext.TargetConnectionString[MigrationJobContext.CurrentlyActiveJob.Id]);

                            if (result == TaskResult.Success)
                            {
                                if (HandleControlPause())
                                    return TaskResult.Canceled;

                                if (MigrationJobContext.CurrentlyActiveJob.JobType == JobType.DumpAndRestore)
                                    _log.WriteLine($"Dump processor completed successfully for {migrationUnit.DatabaseName}.{migrationUnit.CollectionName}", LogType.Verbose);
                                else
                                    _log.WriteLine($"Migration processor completed successfully for {migrationUnit.DatabaseName}.{migrationUnit.CollectionName}", LogType.Verbose);

                                // since CS processsing has started, we can break the loop. No need to process all collections
                                if (Helper.IsOnline(MigrationJobContext.CurrentlyActiveJob) && MigrationJobContext.CurrentlyActiveJob.SyncBackEnabled && (MigrationJobContext.CurrentlyActiveJob.CSPostProcessingStarted && MigrationJobContext.CurrentlyActiveJob.ChangeStreamMode != ChangeStreamMode.Aggressive) && Helper.IsOfflineJobCompleted(MigrationJobContext.CurrentlyActiveJob))
                                {
                                    _log.WriteLine("Breaking loop: CS post-processing started and offline job completed", LogType.Debug);
                                    break;
                                }
                            }
                            else
                            {
                                _log.WriteLine($"Migration processor returned {result} for {migrationUnit.DatabaseName}.{migrationUnit.CollectionName}", LogType.Debug);
                                return result;
                            }
                        }
                        else
                            return TaskResult.Abort;
                    }
                    else
                    {
                        migrationUnit.SourceStatus = CollectionStatus.NotFound;
                        _log.WriteLine($"{migrationUnit.DatabaseName}.{migrationUnit.CollectionName} does not exist on source. Created empty collection.", LogType.Warning);
                        return TaskResult.Abort;
                    }
                }               
            }

            //wait till all activities are done
            //if there are errors in an actiivty it will stop independently.
            _log.WriteLine("Waiting for migration processor to complete all activities", LogType.Verbose);
            while (_migrationProcessor!=null && _migrationProcessor.ProcessRunning)
            {
                if (HandleControlPause())
                    return TaskResult.Canceled;

                //check back after 10 sec
                Task.Delay(10000, ctsToken).Wait(ctsToken);
            }           

            // Note: No need to wait for all resume token tasks here.
            // The change stream processor checks mu.ResumeToken for each collection before processing.
            // This allows collections with ready tokens to start processing immediately.

            _log.WriteLine("MigrateJobCollections completed - all activities finished", LogType.Debug);
            return TaskResult.Success; //all  actiivty completed successfully
        }



        private async Task<TaskResult> StartOnlineForJobCollections(CancellationToken ctsToken, MigrationProcessor processor, bool clearCache=false)
        {
            try
            {
                MigrationJobContext.Log = _log;
                _log.WriteLine("StartOnlineForJobCollections started", LogType.Debug);
                if (MigrationJobContext.CurrentlyActiveJob == null)
                    return TaskResult.FailedAfterRetries;

                var unitsForMigrate = Helper.GetMigrationUnitsToMigrate(MigrationJobContext.CurrentlyActiveJob);

                _log.WriteLine($"Adding {unitsForMigrate.Count} collections to change stream queue", LogType.Verbose);
                foreach (var migrationUnit in unitsForMigrate)
                {
                    if (_migrationCancelled)
                        return TaskResult.Canceled;

                    if (HandleControlPause())
                        return TaskResult.Canceled;
 

                    if (Helper.IsMigrationUnitValid(migrationUnit))
                    {
                        bool checkExist;

                        if (MigrationJobContext.CurrentlyActiveJob.JobType== JobType.RUOptimizedCopy)
                            checkExist = await MongoHelper.CheckRUCollectionExistsAsync(_sourceClient!, migrationUnit.DatabaseName, migrationUnit.CollectionName);
                        else
                            checkExist = await MongoHelper.CheckCollectionExistsAsync(_sourceClient!, migrationUnit.DatabaseName, migrationUnit.CollectionName);


                        if (await MongoHelper.CheckCollectionExistsAsync(_sourceClient!, migrationUnit.DatabaseName, migrationUnit.CollectionName))
                        {
                            processor.AddCollectionToChangeStreamQueue(migrationUnit.Id, MigrationJobContext.TargetConnectionString[MigrationJobContext.CurrentlyActiveJob.Id]);
                            _log.WriteLine($"Added {migrationUnit.DatabaseName}.{migrationUnit.CollectionName} to change stream queue", LogType.Verbose);
                            
                        }
                    }

                    if(clearCache)
                    {
                        try
                        {
                            //clear cache to free memory
                            MigrationJobContext.MigrationUnitsCache.RemoveMigrationUnit(migrationUnit.Id);
                        }
                        catch (Exception ex)
                        {
                            _log.WriteLine($"Error clearing cache for {migrationUnit.DatabaseName}.{migrationUnit.CollectionName}: {ex}", LogType.Error);
                        }
                    }
                }
                processor.RunChangeStreamProcessorForAllCollections(MigrationJobContext.TargetConnectionString[MigrationJobContext.CurrentlyActiveJob.Id]);
                _log.WriteLine("Change stream processor started for all collections", LogType.Debug);

                return TaskResult.Success;
            }
            catch (Exception ex)
            {
                _log.WriteLine($"Error in starting online migration. Details: {ex}", LogType.Error);
                return TaskResult.FailedAfterRetries;
            }
        }

        private bool HandleControlPause()
        {
            if (MigrationJobContext.ControlledPauseRequested)
            {
                _log.WriteLine("Controlled pause requested - stopping migration processor", LogType.Info);
                _migrationProcessor?.StopProcessing(true);
                ProcessRunning = false;
                JobStarting = false;
                StopMigration();
                return true;
            }
            return false;
        }

        public async Task StartMigrationAsync(string namespacesToMigrate, JobType jobtype, bool trackChangeStreams)
        {
            try
            {
                
                JobStarting = true;
                if (string.IsNullOrWhiteSpace(MigrationJobContext.CurrentlyActiveJob.Id))
                {
                    StopMigration(); //stop any existing
                    return;
                }

                StopMigration(); //stop any existing
                ProcessRunning = true;
                MigrationJobContext.ControlledPauseRequested = false;
                _activeJobId = MigrationJobContext.CurrentlyActiveJob.Id;
                Console.WriteLine($"_activeJobId: {_activeJobId}");
                MigrationJobContext.MigrationUnitsCache = new ActiveMigrationUnitsCache();

                string logfile = _log.Init(MigrationJobContext.CurrentlyActiveJob.Id);
                if (logfile != MigrationJobContext.CurrentlyActiveJob.Id)
                {
                    _log.WriteLine($"Error in reading log. Orginal log backed up as {logfile}", LogType.Error);
                }
                _log.WriteLine($"Job {MigrationJobContext.CurrentlyActiveJob.Id} started on {MigrationJobContext.CurrentlyActiveJob.StartedOn} (UTC)", LogType.Warning);

                _log.WriteLine($"StartMigrationAsync called - JobType: {jobtype}, TrackChangeStreams: {trackChangeStreams}", LogType.Debug);
                _log.SetJob(MigrationJobContext.CurrentlyActiveJob); // Set job reference for log level filtering

                _log.WriteLine($"Working folder is {Environment.GetEnvironmentVariable("ResourceDrive")}");

                MigrationJobContext.Log = _log;

                //encoding speacial characters
                var sourceConnectionString = Helper.EncodeMongoPasswordInConnectionString(MigrationJobContext.SourceConnectionString[MigrationJobContext.CurrentlyActiveJob.Id]);
                var targetConnectionString = Helper.EncodeMongoPasswordInConnectionString(MigrationJobContext.TargetConnectionString[MigrationJobContext.CurrentlyActiveJob.Id]);

                targetConnectionString = Helper.UpdateAppName(targetConnectionString, $"MSFTMongoWebMigration{Helper.IsOnline(MigrationJobContext.CurrentlyActiveJob)}-" + MigrationJobContext.CurrentlyActiveJob.Id);

                _log.WriteLine($"Connection strings prepared - Job ID: {MigrationJobContext.CurrentlyActiveJob.Id}", LogType.Verbose);

                LoadConfig();

                _migrationCancelled = false;

                // Reset controlled pause flag when resuming/starting job
                MigrationJobContext.ControlledPauseRequested = false;

                _cts = new CancellationTokenSource();

                MigrationJobContext.SaveMigrationJob(MigrationJobContext.CurrentlyActiveJob);


                if (MigrationJobContext.CurrentlyActiveJob.MigrationUnitBasics == null)
                {
                    MigrationJobContext.CurrentlyActiveJob.MigrationUnitBasics = new List<MigrationUnitBasic>();
                }

                _log.WriteLine($"Populating job collections from namespaces: {namespacesToMigrate.Replace(",", ", ")}", LogType.Verbose);
                var unitsToAdd = await Helper.PopulateJobCollectionsAsync(MigrationJobContext.CurrentlyActiveJob, namespacesToMigrate, sourceConnectionString, MigrationJobContext.CurrentlyActiveJob.AllCollectionsUseObjectId);

                //find new units to add

                var newUnits = unitsToAdd
                    .Where(mu => !MigrationJobContext.CurrentlyActiveJob.MigrationUnitBasics
                    .Any(mub => mub.Id == Helper.GenerateMigrationUnitId(mu.DatabaseName, mu.CollectionName)))
                    .ToList();

                if (newUnits.Count > 0)
                {

                    _log.WriteLine($"Adding {newUnits.Count} migration units to job", LogType.Debug);
                    foreach (var mu in newUnits)
                    {
                        MigrationJobContext.SaveMigrationUnit(mu, false);
                        Helper.AddMigrationUnit(mu, MigrationJobContext.CurrentlyActiveJob);
                    }
                    // Save job after all units are added to persist changes
                    MigrationJobContext.SaveMigrationJob(MigrationJobContext.CurrentlyActiveJob);
                }


                if (HandleControlPause())
                    return;



                if (MigrationJobContext.CurrentlyActiveJob.JobType == JobType.DumpAndRestore)
                {
                    if (!Helper.IsWindows())
                    {
                        //ACA
                        _log.WriteLine("Ensuring MongoDB tools are available for DumpAndRestore job", LogType.Debug);
                        if (!await Helper.ValidateMongoToolsAvailableAsync(_log))
                        {
                            StopMigration();
                            return;
                        }
                        _toolsLaunchFolder = string.Empty;
                    }
                    else
                    {
                        //WebApp
                        _log.WriteLine("Ensuring MongoDB tools are available for DumpAndRestore job", LogType.Debug);
                        _toolsLaunchFolder = await Helper.EnsureMongoToolsAvailableAsync(_log, _toolsDestinationFolder, _config!);
                        if (string.IsNullOrEmpty(_toolsLaunchFolder))
                        {
                            _log.WriteLine("MongoDB tools not available - stopping migration", LogType.Error);
                            StopMigration();
                            return;
                        }
                        _log.WriteLine($"MongoDB tools ready at: {_toolsLaunchFolder}", LogType.Verbose);
                        _log.WriteLine($"Working directory  is {Helper.GetWorkingFolder()}", LogType.Verbose);
                        _toolsLaunchFolder = $"{_toolsLaunchFolder}\\";
                    }
                }

                if (HandleControlPause())
                    return;


                _log.WriteLine("Starting PrepareForMigration with retry logic", LogType.Debug);
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
                    _log.WriteLine($"PrepareForMigration returned {result} - stopping migration", LogType.Debug);
                    StopMigration();
                    return;
                }
                JobStarting = false;


                if (HandleControlPause())
                    return;


                if (Helper.IsOnline(MigrationJobContext.CurrentlyActiveJob) && MigrationJobContext.CurrentlyActiveJob.ChangeStreamMode == ChangeStreamMode.Immediate)
                {
                    _log.WriteLine("Starting online change stream processor in background for Immediate mode", LogType.Debug);
#pragma warning disable CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed
                    //deliberately not awaiting this task, since it is expected to run in parallel with the migration
                    StartOnlineForJobCollections(_cts.Token, _migrationProcessor!, true);
#pragma warning restore CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed


                    await Task.Delay(30000);
                }



                if (HandleControlPause())
                    return;

                bool skipPartitioning = false;// in all case it Off for now.
                _log.WriteLine("Starting PreparePartitionsAsync with retry logic", LogType.Debug);
                result = await new RetryHelper().ExecuteTask(
                    () => PreparePartitionsAsync(_cts.Token, skipPartitioning),
                    (ex, attemptCount, currentBackoff) => Default_ExceptionHandler(
                        ex, attemptCount,
                        "Partition step", currentBackoff
                    ),
                    _log
                );

                if (result == TaskResult.Abort || result == TaskResult.FailedAfterRetries || _migrationCancelled)
                {
                    _log.WriteLine($"PreparePartitionsAsync returned {result} - stopping migration", LogType.Debug);
                    StopMigration();
                    return;
                }

                if (HandleControlPause())
                    return;


                //if run comparison is set by customer.
                if (MigrationJobContext.CurrentlyActiveJob.RunComparison)
                {
                    _log.WriteLine("RunComparison flag is set - starting comparison", LogType.Debug);
                    var compareHelper = new ComparisonHelper();
                    _compare_cts = new CancellationTokenSource();
                    await compareHelper.CompareRandomDocumentsAsync(_log, MigrationJobContext.CurrentlyActiveJob, _config!, _compare_cts.Token);
                    compareHelper = null;
                    MigrationJobContext.CurrentlyActiveJob.RunComparison = false;

                    MigrationJobContext.SaveMigrationJob(MigrationJobContext.CurrentlyActiveJob);
                    _log.WriteLine("Comparison completed - resuming migration", LogType.Verbose);
                }



                if (HandleControlPause())
                    return;

                _log.WriteLine("Starting MigrateJobCollections.", LogType.Debug);
                result = await new RetryHelper().ExecuteTask(
                    () => MigrateJobCollections(_cts.Token),
                    (ex, attemptCount, currentBackoff) => MigrateCollections_ExceptionHandler(
                        ex, attemptCount,
                        "Migrate collections", currentBackoff
                    ),
                    _log
                );

                if (HandleControlPause())
                    return;


                if (result == TaskResult.Success || result == TaskResult.Abort || result == TaskResult.FailedAfterRetries || _migrationCancelled)
                {
                    _log.WriteLine($"MigrateJobCollections completed with result: {result}", LogType.Debug);
                    if (result == TaskResult.Success)
                    {
                        if (!Helper.IsOnline(MigrationJobContext.CurrentlyActiveJob))
                        {
                            // Don't mark as completed if this is a controlled pause
                            if (!MigrationJobContext.ControlledPauseRequested)
                            {
                                MigrationJobContext.CurrentlyActiveJob.IsCompleted = true;
                                _log.WriteLine("Job marked as completed", LogType.Verbose);
                            }

                            MigrationJobContext.SaveMigrationJob(MigrationJobContext.CurrentlyActiveJob);
                        }
                    }

                    StopMigration();
                    return;
                }
            }
            catch (Exception ex)
            {
                _log.WriteLine($"Fatal error in StartMigrationAsync: {ex}", LogType.Error);
                StopMigration();
            }

        }

        private void LoadConfig()
        {
            if (_config == null)
                _config = new MigrationSettings();
             _config.Load();
        }


        public void SyncBackToSource(string sourceConnectionString, string targetConnectionString)
        {
            JobStarting= true;

            if (string.IsNullOrWhiteSpace(MigrationJobContext.CurrentlyActiveJob.Id)) 
            {
                StopMigration(); //stop any existing
                return;
            }
            MigrationJobContext.ControlledPauseRequested = false;
            ProcessRunning = true;
            
            LoadConfig();

            if(_log==null)
                _log = new Log();
            
            string logfile = _log.Init(MigrationJobContext.CurrentlyActiveJob.Id);
            _log.SetJob(MigrationJobContext.CurrentlyActiveJob); // Set job reference for log level filtering
            _log.WriteLine($"SyncBack: {MigrationJobContext.CurrentlyActiveJob.Id} started on {MigrationJobContext.CurrentlyActiveJob.StartedOn} (UTC)");

            MigrationJobContext.CurrentlyActiveJob.ProcessingSyncBack = true;
            MigrationJobContext.SaveMigrationJob(MigrationJobContext.CurrentlyActiveJob);
            MigrationJobContext.MigrationUnitsCache = new ActiveMigrationUnitsCache();
            //_jobList.Save();

            if (_migrationProcessor != null)
                _migrationProcessor.StopProcessing();

            _migrationProcessor = null;
            var dummySourceClient = MongoClientFactory.Create(_log, sourceConnectionString);
            _migrationProcessor = new SyncBackProcessor(_log, dummySourceClient, _config!);
            _migrationProcessor.ProcessRunning = true;
            JobStarting = false;
            var dummyUnit = new MigrationUnit(MigrationJobContext.CurrentlyActiveJob,"", "", new List<MigrationChunk>());

            MigrationJobContext.SaveMigrationUnit(dummyUnit,false);

            //if run comparison is set by customer.
            if (MigrationJobContext.CurrentlyActiveJob.RunComparison)
            {
                _cts = new CancellationTokenSource();
                var compareHelper = new ComparisonHelper();
                compareHelper.CompareRandomDocumentsAsync(_log, MigrationJobContext.CurrentlyActiveJob, _config!, _cts.Token).GetAwaiter().GetResult();
                compareHelper = null;
                MigrationJobContext.CurrentlyActiveJob.RunComparison = false;

                MigrationJobContext.SaveMigrationJob(MigrationJobContext.CurrentlyActiveJob);

                _log.WriteLine("Resuming SyncBack.");
            }

            _migrationProcessor.StartProcessAsync(dummyUnit.Id, sourceConnectionString, targetConnectionString).GetAwaiter().GetResult();
            
        }

        private async Task<List<MigrationChunk>> PartitionCollectionAsync(string databaseName, string collectionName, CancellationToken cts, MigrationUnit migrationUnit)
        {
            try
            {
                _log.WriteLine($"PartitionCollectionAsync started for {databaseName}.{collectionName}", LogType.Debug);
                cts.ThrowIfCancellationRequested();

                if (_sourceClient == null || _config == null || MigrationJobContext.CurrentlyActiveJob == null)
                    throw new InvalidOperationException("Worker not initialized");

                var stats = await MongoHelper.GetCollectionStatsAsync(_sourceClient!, databaseName, collectionName);

                long documentCount = stats.DocumentCount;
                long totalCollectionSizeBytes = stats.CollectionSizeBytes;

                _log.WriteLine($"{databaseName}.{collectionName} - docCount: {documentCount}, size: {totalCollectionSizeBytes} bytes", LogType.Verbose);
                var database = _sourceClient!.GetDatabase(databaseName);
                var collection = database.GetCollection<BsonDocument>(collectionName);

                int totalChunks = 0;
                long minDocsInChunk = 0;

                long targetChunkSizeBytes = _config.ChunkSizeInMb * 1024 * 1024;
                var totalChunksBySize = (int)Math.Ceiling((double)totalCollectionSizeBytes / targetChunkSizeBytes);
                
                bool optimizeForObjectId = false;

                if (MigrationJobContext.CurrentlyActiveJob.JobType == JobType.DumpAndRestore)
                {
                    totalChunks = totalChunksBySize;
                    minDocsInChunk = documentCount / (totalChunks == 0 ? 1 : totalChunks);
                    _log.WriteLine($"{databaseName}.{collectionName} storage size: {totalCollectionSizeBytes}", LogType.Verbose);
                }
                else
                {
                    _log.WriteLine($"{databaseName}.{collectionName} estimated document count: {documentCount}", LogType.Verbose);
                    totalChunks = (int)Math.Min(SamplePartitioner.MaxSamples / SamplePartitioner.MaxSegments, documentCount / (SamplePartitioner.MaxSamples == 0 ? 1 : SamplePartitioner.MaxSamples));
                    totalChunks = Math.Max(1, totalChunks); // At least one chunk
                    totalChunks = Math.Max(totalChunks, totalChunksBySize);
                    minDocsInChunk = documentCount / (totalChunks == 0 ? 1 : totalChunks);
                }

                _log.WriteLine($"Partitioning strategy: totalChunks={totalChunks}, minDocsInChunk={minDocsInChunk}, chunkSizeBytes={targetChunkSizeBytes}", LogType.Debug);
                List<MigrationChunk> migrationChunks = new List<MigrationChunk>();

                if (totalChunks > 1 )
                {
                    _log.WriteLine($"Chunking {databaseName}.{collectionName}", LogType.Debug);

                    List<DataType> dataTypes;

                    // Check if DataTypeFor_Id is specified in the MigrationUnit
                    if (migrationUnit?.DataTypeFor_Id.HasValue == true)
                    {
                        // Use only the specified DataType and skip filtering by other data types
                        dataTypes = new List<DataType> { migrationUnit.DataTypeFor_Id.Value };
                        _log.WriteLine($"Using specified DataType for _id: {migrationUnit.DataTypeFor_Id.Value}", LogType.Debug);


                        if( migrationUnit.DataTypeFor_Id.Value == DataType.ObjectId)// && !MongoHelper.UsesIdFieldInFilter(MongoHelper.GetFilterDoc(migrationUnit.UserFilter)))
                        {
                            optimizeForObjectId = true;
                            _log.WriteLine("ObjectId optimization enabled", LogType.Verbose);
                        }
                    }
                    else
                    {
                        // Use all DataTypes (original behavior)
                        dataTypes = new List<DataType> { DataType.Int, DataType.Int64, DataType.String, DataType.Object, DataType.Decimal128, DataType.Date, DataType.ObjectId };
                        _log.WriteLine($"Using all DataTypes for partitioning ({dataTypes.Count} types)", LogType.Verbose);

                        if (_config.ReadBinary)
                        {
                            dataTypes.Add(DataType.BinData);
                        }
                    }

                    foreach (var dataType in dataTypes)
                    {
                        if (HandleControlPause())
                        {
                            return null;
                        }

                        long docCountByType;
                        _log.WriteLine($"Creating partitions for DataType: {dataType}", LogType.Verbose);
                        ChunkBoundaries? chunkBoundaries = SamplePartitioner.CreatePartitions(_log, MigrationJobContext.CurrentlyActiveJob.JobType == JobType.DumpAndRestore, collection, totalChunks, dataType, minDocsInChunk, cts, migrationUnit!,optimizeForObjectId , _config,out docCountByType);

                        if (docCountByType == 0 || chunkBoundaries == null)
                        {
                            _log.WriteLine($"No documents found for DataType: {dataType}", LogType.Verbose);
                            continue;
                        }

                        if (chunkBoundaries.Boundaries.Count == 0)
                        {
                            _log.WriteLine($"No boundaries created for DataType: {dataType}, creating single chunk", LogType.Debug);
                            var chunk = new MigrationChunk(string.Empty, string.Empty, DataType.Other, false, false);
                            migrationChunks.Add(chunk);
                            if (MigrationJobContext.CurrentlyActiveJob.JobType == JobType.MongoDriver)
                            {
                                chunk.Segments = new List<Segment>
                                {
                                    new Segment { Gte = "", Lt = "", IsProcessed = false, Id = "1" }
                                };
                            }
                        }
                        else
                        {
                            _log.WriteLine($"Creating segments from {chunkBoundaries.Boundaries.Count} boundaries for DataType: {dataType}", LogType.Debug);
#pragma warning disable CS8604 // Possible null reference argument.
                            CreateSegments(chunkBoundaries, migrationChunks, dataType, migrationUnit?.UserFilter);
#pragma warning restore CS8604 // Possible null reference argument.
                        }
                    }
                    
                }
                else
                {
                    _log.WriteLine($"Single chunk (no partitioning) for {databaseName}.{collectionName}", LogType.Debug);
                    var chunk = new MigrationChunk(string.Empty, string.Empty, DataType.Other, false, false);
                    migrationChunks.Add(chunk);
                    if(MigrationJobContext.CurrentlyActiveJob.JobType == JobType.MongoDriver)
                    {
                        chunk.Segments = new List<Segment>
                        {
                            new Segment { Gte = "", Lt = "", IsProcessed = false, Id = "1" }
                        };
                    }
                }

                _log.WriteLine($"PartitionCollectionAsync completed - {migrationChunks.Count} chunks created for {databaseName}.{collectionName}", LogType.Debug);
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
              

        private void CreateSegments(ChunkBoundaries chunkBoundaries, List<MigrationChunk> migrationChunks, DataType dataType, string userFilter)
        {
            _log.WriteLine($"CreateSegments started - creating segments for {chunkBoundaries.Boundaries.Count} boundaries", LogType.Verbose);
            for (int i = 0; i < chunkBoundaries.Boundaries.Count; i++)
            {               
                var (startId, endId) = GetStartEnd(true, chunkBoundaries.Boundaries[i], chunkBoundaries.Boundaries.Count, i, userFilter);
                var chunk = new MigrationChunk(startId, endId, dataType, false, false);
                migrationChunks.Add(chunk);

                var boundary = chunkBoundaries.Boundaries[i];
                if (MigrationJobContext.CurrentlyActiveJob != null && MigrationJobContext.CurrentlyActiveJob.JobType == JobType.MongoDriver && (boundary.SegmentBoundaries == null || boundary.SegmentBoundaries.Count == 0))
                {
                    chunk.Segments ??= new List<Segment>();
                    chunk.Segments.Add(new Segment { Gte = startId, Lt = endId, IsProcessed = false, Id = "1" });
                }

                if (MigrationJobContext.CurrentlyActiveJob!.JobType == JobType.MongoDriver && boundary.SegmentBoundaries != null && boundary.SegmentBoundaries.Count > 0)
                {
                    _log.WriteLine($"Creating {boundary.SegmentBoundaries.Count} segments for boundary {i}", LogType.Verbose);
                    for (int j = 0; j < boundary.SegmentBoundaries.Count; j++)
                    {
                        var segment = boundary.SegmentBoundaries[j];
                        var (segmentStartId, segmentEndId) = GetStartEnd(false, segment, boundary.SegmentBoundaries.Count, j, userFilter, chunk.Lt ?? string.Empty, chunk.Gte ?? string.Empty);

                        chunk.Segments ??= new List<Segment>();
                        chunk.Segments.Add(new Segment { Gte = segmentStartId, Lt = segmentEndId, IsProcessed = false, Id = (j + 1).ToString() });
                    }
                }
            }
            _log.WriteLine($"CreateSegments completed - {migrationChunks.Count} total chunks created", LogType.Debug);
        }
        private Tuple<string, string> GetStartEnd(bool isChunk, Boundary boundary, int totalBoundaries, int currentIndex, string userFilter,string chunkLt = "", string chunkGte = "")
        {
            string startId;
            string endId;

            if (currentIndex == 0)
            {
                string min = string.Empty;
               
                var filterDoc = MongoHelper.GetFilterDoc(userFilter);
                var minValue = MongoHelper.GetIdRangeMin(filterDoc);

                string minId =string.Empty;

#pragma warning disable CS8600 // Converting null literal or possible null value to non-nullable type.
                try
                {
                    if (minValue != BsonMinKey.Value)
                        minId = minValue.AsBsonValue.ToString();
                }
                catch
                {
                    minId = string.Empty;
                }

                startId = isChunk ? minId : chunkGte;
#pragma warning restore CS8600 // Converting null literal or possible null value to non-nullable type.
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
