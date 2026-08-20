using MongoDB.Bson;
using MongoDB.Driver;
using OnlineMongoMigrationProcessor.Context;
using OnlineMongoMigrationProcessor.Helpers.Mongo;
using OnlineMongoMigrationProcessor.Models;
using OnlineMongoMigrationProcessor.Processors;
using OnlineMongoMigrationProcessor.Workers;

namespace OnlineMongoMigrationProcessor
{
    internal class StorageValidationProcessor : MigrationProcessor
    {
        private const int MaximumSampleSize = 1000;

        public StorageValidationProcessor(Log log, MongoClient sourceClient, MigrationSettings config, MigrationWorker migrationWorker)
            : base(log, sourceClient, config, migrationWorker)
        {
        }

        public override async Task<TaskResult> StartProcessAsync(string migrationUnitId, string sourceConnectionString, string targetConnectionString)
        {
            var migrationUnit = MigrationJobContext.GetMigrationUnit(migrationUnitId);
            migrationUnit.ParentJob = MigrationJobContext.CurrentlyActiveJob;

            if (migrationUnit.DumpComplete && migrationUnit.RestoreComplete)
                return TaskResult.Success;

            var context = SetProcessorContext(migrationUnit, sourceConnectionString, targetConnectionString);
            _targetClient ??= MongoClientFactory.Create(_log, targetConnectionString);
            var targetCollection = _targetClient
                .GetDatabase(context.TargetDatabaseName)
                .GetCollection<BsonDocument>(context.TargetCollectionName);

            try
            {
                migrationUnit.BulkCopyStartedOn = DateTime.UtcNow;
                var documents = await context.Collection.Aggregate()
                    .Sample(MaximumSampleSize)
                    .ToListAsync(_cts.Token);

                if (documents.Count > 0)
                {
                    await targetCollection.InsertManyAsync(
                        documents,
                        new InsertManyOptions { IsOrdered = false },
                        _cts.Token);
                }

                var chunk = migrationUnit.MigrationChunks[0];
                chunk.DumpQueryDocCount = documents.Count;
                chunk.DumpResultDocCount = documents.Count;
                chunk.RestoredSuccessDocCount = documents.Count;
                chunk.IsDownloaded = true;
                chunk.IsUploaded = true;

                migrationUnit.SourceCountDuringCopy = documents.Count;
                migrationUnit.DumpPercent = 100;
                migrationUnit.RestorePercent = 100;
                migrationUnit.DumpComplete = true;
                migrationUnit.RestoreComplete = true;
                migrationUnit.BulkCopyEndedOn = DateTime.UtcNow;
                MigrationJobContext.SaveMigrationUnit(migrationUnit, true);

                _migrationWorker!.StartBackgroundIndexBuildAndQueue(migrationUnit);
                StopOfflineOrInvokeChangeStreams();
                return TaskResult.Success;
            }
            catch (OperationCanceledException)
            {
                return TaskResult.Canceled;
            }
            catch (Exception ex)
            {
                _log.WriteLine($"Storage validation copy failed for {context.DatabaseName}.{context.CollectionName}: {ex}", LogType.Error);
                return TaskResult.Retry;
            }
        }
    }
}