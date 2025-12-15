using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Driver;
using OnlineMongoMigrationProcessor.Context;
using OnlineMongoMigrationProcessor.Helpers;
using OnlineMongoMigrationProcessor.Helpers.JobManagement;
using OnlineMongoMigrationProcessor.Helpers.Mongo;
using OnlineMongoMigrationProcessor.Models;
using OnlineMongoMigrationProcessor.Processors;
using OnlineMongoMigrationProcessor.Workers;


namespace OnlineMongoMigrationProcessor
{
    internal class CopyProcessor: MigrationProcessor
    {
        public CopyProcessor(Log log, MongoClient sourceClient, MigrationSettings config, MigrationWorker? migrationWorker = null)
            : base(log, sourceClient, config, migrationWorker)
        {
            // Constructor body can be empty or contain initialization logic if needed
        }

        /// <summary>
        /// Override to handle controlled pause for CopyProcessor
        /// </summary>
        public override void InitiateControlledPause()
        {
            base.InitiateControlledPause();
            _log.WriteLine("CopyProcessor: Controlled pause initiated");
        }

        // Custom exception handler delegate with logic to control retry flow
        private Task<TaskResult> CopyProcess_ExceptionHandler(Exception ex, int attemptCount, string processName, string dbName, string colName, int chunkIndex, int currentBackoff)
        {
            if (ex is OperationCanceledException)
            {
                _log.WriteLine($"Document copy operation was paused for {dbName}.{colName}[{chunkIndex}]");
                return Task.FromResult(TaskResult.Abort);
            }
            else if (ex is MongoExecutionTimeoutException)
            {
                _log.WriteLine($" {processName} attempt {attemptCount} for {dbName}.{colName}[{chunkIndex}] failed due to timeout. Details:{ex}. Retrying in {currentBackoff} seconds...", LogType.Error);
                return Task.FromResult(TaskResult.Retry);
            }
            else if (ex.Message== "Copy Document Failed")
            {
                _log.WriteLine($"{processName} attempt {attemptCount} for {dbName}.{colName}[{chunkIndex}] failed. Retrying in {currentBackoff} seconds...");
                return Task.FromResult(TaskResult.Retry);
            }
            else
            {
                _log.WriteLine($"{processName} attempt for {dbName}.{colName}[{chunkIndex}] failed. Details:{ex}. Retrying in {currentBackoff} seconds...", LogType.Error);
                return Task.FromResult(TaskResult.Retry);
            }
        }
       

        
        private async Task <TaskResult> ProcessChunkAsync(MigrationUnit mu, int chunkIndex, ProcessorContext ctx, double initialPercent, double contributionFactor)
        {
            long docCount;
            FilterDefinition<BsonDocument> filter;

            if (mu.MigrationChunks.Count > 1)
            {
                var gteStr = mu.MigrationChunks[chunkIndex].Gte ?? string.Empty;
                var ltStr = mu.MigrationChunks[chunkIndex].Lt ?? string.Empty;
                var bounds = SamplePartitioner.GetChunkBounds(gteStr, ltStr, mu.MigrationChunks[chunkIndex].DataType);
                var gte = bounds.gte;
                var lt = bounds.lt;

                _log.WriteLine($"{ctx.DatabaseName}.{ctx.CollectionName}-Chunk [{chunkIndex}] generating query");

                // Generate query and get document count
                filter = MongoHelper.GenerateQueryFilter(gte, lt, mu.MigrationChunks[chunkIndex].DataType,MongoHelper.GetFilterDoc(mu.UserFilter), mu.DataTypeFor_Id.HasValue);

                docCount = MongoHelper.GetDocumentCount(ctx.Collection, filter, new BsonDocument());//filter already has user filter.
                mu.MigrationChunks[chunkIndex].DumpQueryDocCount = docCount;

                ctx.DownloadCount += mu.MigrationChunks[chunkIndex].DumpQueryDocCount;

                _log.WriteLine($"Count for {ctx.DatabaseName}.{ctx.CollectionName}[{chunkIndex}] is  {docCount}");

            }
            else
            {
                filter = Builders<BsonDocument>.Filter.Empty;
                docCount = MongoHelper.GetDocumentCount(ctx.Collection, filter, MongoHelper.GetFilterDoc(mu.UserFilter!));

                mu.MigrationChunks[chunkIndex].DumpQueryDocCount = docCount;
                ctx.DownloadCount = docCount;
            }


            //checkCounts(ctx.Collection, mu.MigrationChunks[chunkIndex], MongoHelper.GetFilterDoc(mu.UserFilter));


            if (_targetClient == null && !MigrationJobContext.CurrentlyActiveJob.IsSimulatedRun)
                _targetClient = MongoClientFactory.Create(_log, ctx.TargetConnectionString);

            var documentCopier = new DocumentCopyWorker();
            documentCopier.Initialize(_log, _targetClient!, ctx.Collection, ctx.DatabaseName, ctx.CollectionName, _config.MongoCopyPageSize);
            var result = await documentCopier.CopyDocumentsAsync(mu, chunkIndex, initialPercent, contributionFactor, docCount, filter, _cts.Token, MigrationJobContext.CurrentlyActiveJob.IsSimulatedRun);

            if (result == TaskResult.Success)
            {
                if (!_cts.Token.IsCancellationRequested)
                {                        
                    mu.MigrationChunks[chunkIndex].IsDownloaded = true;
                    mu.MigrationChunks[chunkIndex].IsUploaded = true;
                }

                MigrationJobContext.SaveMigrationUnit(mu,false);
                return TaskResult.Success;
            }
            else if(result == TaskResult.Canceled)
            {
                _log.WriteLine($"Document copy operation for {ctx.DatabaseName}.{ctx.CollectionName}[{chunkIndex}] was paused.");
				return TaskResult.Canceled;
			}
			else
            {
                _log.WriteLine($"Document copy operation for {ctx.DatabaseName}.{ctx.CollectionName}[{chunkIndex}] failed.", LogType.Error);
				return TaskResult.Retry;
			}

        }



        public override async Task<TaskResult> StartProcessAsync(string migrationUnitId, string sourceConnectionString, string targetConnectionString, string idField = "_id")
        {
            var mu=MigrationJobContext.MigrationUnitsCache.GetMigrationUnit(migrationUnitId);
            mu.ParentJob= MigrationJobContext.CurrentlyActiveJob;
            ProcessRunning = true;
            ProcessorContext ctx;

            ctx=SetProcessorContext(mu, sourceConnectionString, targetConnectionString);

            //when resuming a job, we need to check if post-upload change stream processing is already in progress
            if (CheckChangeStreamAlreadyProcessingAsync(ctx))
                return TaskResult.Success;

            // starting the  regular document copy process
            _log.WriteLine($"{ctx.DatabaseName}.{ctx.CollectionName} Document copy started");

            if (!mu.DumpComplete && !_cts.Token.IsCancellationRequested)
            {
                for (int i = 0; i < mu.MigrationChunks.Count; i++)
                {
                    // Check for controlled pause before starting new chunk
                    if (MigrationJobContext.ControlledPauseRequested)
                    {
                        _log.WriteLine($"Controlled pause: Stopping before chunk {i}, {mu.MigrationChunks.Count - i} chunks not started");
                        break;
                    }
                    
                    _cts.Token.ThrowIfCancellationRequested();

                    double initialPercent = ((double)100 / mu.MigrationChunks.Count) * i;
                    double contributionFactor = 1.0 / mu.MigrationChunks.Count;

                    if (!mu.MigrationChunks[i].IsDownloaded == true)
                    {
                        TaskResult result = await new RetryHelper().ExecuteTask(
                            () => ProcessChunkAsync(mu, i, ctx, initialPercent, contributionFactor),
                            (ex, attemptCount, currentBackoff) => CopyProcess_ExceptionHandler(
                                ex, attemptCount,
                                "Chunk processor", ctx.DatabaseName, ctx.CollectionName, i, currentBackoff
                            ),
                            _log
                        );

                        if (result== TaskResult.Abort || result== TaskResult.FailedAfterRetries)
                        {
                            _log.WriteLine($"Document copy operation for {ctx.DatabaseName}.{ctx.CollectionName}[{i}] failed after multiple attempts.", LogType.Error);
                            StopProcessing();
                            return result;
                        }
                    }
                    else
                    {
                        ctx.DownloadCount += mu.MigrationChunks[i].DumpQueryDocCount;
                    }
                }

                // Check if controlled pause completed
                if (MigrationJobContext.ControlledPauseRequested && mu.DumpComplete)
                {
                    _log.WriteLine("Controlled pause completed for CopyProcessor");
                    StopProcessing();
                    return TaskResult.Success;
                }

                mu.SourceCountDuringCopy = mu.MigrationChunks.Sum(chunk => (long)(chunk.Segments?.Sum(seg => seg.QueryDocCount) ?? 0));
                mu.DumpGap = Helper.GetMigrationUnitDocCount(mu) - mu.SourceCountDuringCopy;
                mu.RestoreGap = mu.SourceCountDuringCopy - mu.MigrationChunks.Sum(chunk => chunk.DumpResultDocCount);
                

                long failed= mu.MigrationChunks.Sum(chunk => chunk.RestoredFailedDocCount);
                // don't compare counts source vs target as some documents may have been deleted in source
                //only  check for failed documents
                if (failed == 0)
                {
                    mu.BulkCopyEndedOn = DateTime.UtcNow;

                    mu.DumpPercent = 100;
                    mu.DumpComplete = true;

                    mu.RestorePercent = 100;
                    mu.RestoreComplete = true;


                    MigrationJobContext.SaveMigrationUnit(mu,true);

                    MigrationJobContext.MigrationUnitsCache.RemoveMigrationUnit(mu.Id);
                }
                else
                {
                    _log.WriteLine($"Document copy operation for {{ctx.DatabaseName}}.{{ctx.CollectionName}}[{{i}}] failed because of count mismatch.\", LogType.Error");
                    return TaskResult.Retry;
                }
                             
  
            }

            await PostCopyChangeStreamProcessor(ctx, mu.Id);

            return TaskResult.Success;
        }
    }
}
