using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using static System.Reflection.Metadata.BlobBuilder;

namespace OnlineMongoMigrationProcessor.Processors
{
    internal class SyncBackProcessor : IMigrationProcessor
    {
        public bool ProcessRunning { get; set; }
        private MongoChangeStreamProcessor _syncBackToSource;
        private JobList? _jobs;
        private MigrationJob? _job;
        private bool _executionCancelled = false;

        private MigrationSettings? _config;


        public SyncBackProcessor(JobList jobs, MigrationJob job, MongoClient sourceClient, MigrationSettings config, string toolsLaunchFolder)
        {
            _jobs = jobs;
            _job = job;
            _config = config;
        }

        public void StopProcessing()
        {
            ProcessRunning = false;

            _executionCancelled = true;

            if (_syncBackToSource != null)
                _syncBackToSource.ExecutionCancelled = true;

            _syncBackToSource = null;   
        }

        public void StartProcess(MigrationUnit item, string sourceConnectionString, string targetConnectionString, string idField = "_id")
        {
            int maxRetries = 10;
            int attempts = 0;
            TimeSpan backoff = TimeSpan.FromSeconds(2);

            var sourceClient = new MongoClient(sourceConnectionString);
            var targetClient = new MongoClient(targetConnectionString);

            _syncBackToSource = null;
            _syncBackToSource = new MongoChangeStreamProcessor(sourceClient, targetClient, _jobs, _config,true);

            bool continueProcessing = true;

            ProcessRunning=true;
            while (attempts < maxRetries && !_executionCancelled && continueProcessing)
            {
                attempts++;
                try
                {
                    foreach (var migrationUnit in _job.MigrationUnits)
                    {
                        if (_executionCancelled) break;

                        if (migrationUnit.SourceStatus == CollectionStatus.OK)
                        {
                            Task.Run(() => _syncBackToSource.ProcessCollectionChangeStream(_job, migrationUnit));
                        }
                    }
                    continueProcessing = false;
                }
                catch (MongoExecutionTimeoutException ex)
                {
                    Log.WriteLine($"Attempt {attempts} failed due to timeout: {ex.ToString()}. Details:{ex.ToString()}", LogType.Error);

                    Log.WriteLine($"Retrying in {backoff.TotalSeconds} seconds...", LogType.Error);
                    Thread.Sleep(backoff);
                    Log.Save();

                    continueProcessing = true;
                    backoff = TimeSpan.FromTicks(backoff.Ticks * 2);
                }
                catch (Exception ex)
                {
                    Log.WriteLine($"Attempt {attempts} failed: {ex.ToString()}. Details:{ex.ToString()}", LogType.Error);

                    Log.WriteLine($"Retrying in {backoff.TotalSeconds} seconds...", LogType.Error);
                    Thread.Sleep(backoff);
                    Log.Save();

                    continueProcessing = true;
                    backoff = TimeSpan.FromTicks(backoff.Ticks * 2);

                }
            }
            if (attempts == maxRetries)
            {
                Log.WriteLine("Maximum retry attempts reached. Aborting operation.", LogType.Error);
                Log.Save();

                _job.CurrentlyActive = false;
                _jobs?.Save();
                continueProcessing = false;

                ProcessRunning=false;

                _syncBackToSource.ExecutionCancelled = true;
                _syncBackToSource = null;
            }
        }
    }
}
