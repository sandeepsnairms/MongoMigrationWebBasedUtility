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
        private MongoChangeStreamProcessor? _syncBackToSource;
        private JobList? _jobs;
        private MigrationJob? _job;
        private bool _executionCancelled = false;

        private MigrationSettings? _config;
        private CancellationTokenSource _cts;


        public SyncBackProcessor(JobList jobs, MigrationJob job, MongoClient sourceClient, MigrationSettings config, string toolsLaunchFolder)
        {
            _jobs = jobs ?? throw new ArgumentNullException(nameof(jobs), "JobList cannot be null.");
            _job = job ?? throw new ArgumentNullException(nameof(job), "MigrationJob cannot be null.");
            _config = config ?? throw new ArgumentNullException(nameof(config), "MigrationSettings cannot be null.");
        }

        public void StopProcessing()
        {
            ProcessRunning = false;

            _executionCancelled = true;
            _cts?.Cancel();
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
            _syncBackToSource = new MongoChangeStreamProcessor(sourceClient, targetClient, _jobs, _job,_config,true);

            _cts=new CancellationTokenSource();

            bool continueProcessing = true;

            ProcessRunning=true;
            while (attempts < maxRetries && !_executionCancelled && continueProcessing)
            {
                attempts++;
                try
                {
                    Log.WriteLine($"Sync back to source starting.");
                    Log.Save();
                    
                    var result = _syncBackToSource.RunCSPostProcessingAsync(_cts);

                    //Log.WriteLine($"Sync back to source completed successfully.");
                    //Log.Save();
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
