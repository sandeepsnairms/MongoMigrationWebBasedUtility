﻿using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

#pragma warning disable CS8602
#pragma warning disable CS8604
#pragma warning disable CS8600
#pragma warning disable CS8618 // Non-nullable field must contain a non-null value when exiting constructor. Consider adding the 'required' modifier or declaring as nullable.
namespace OnlineMongoMigrationProcessor
{
    internal class CopyProcessor : IMigrationProcessor
    {
        private JobList? _jobs;
        private MigrationJob? _job;
        private bool _executionCancelled = false;
        private MongoClient? _sourceClient;
        private MongoClient? _targetClient;
        private MigrationSettings? _config;
        private CancellationTokenSource _cts;
        private MongoChangeStreamProcessor _changeStreamProcessor;

        public bool ProcessRunning { get; set; }

        public CopyProcessor(JobList jobs, MigrationJob job, MongoClient sourceClient, MigrationSettings config)
        {
            _jobs = jobs;
            _job = job;
            _sourceClient = sourceClient;
            _config = config;
        }

        public void StopProcessing()
        {
            ProcessRunning = false;
            _executionCancelled = true;

            _cts?.Cancel();

            if (_changeStreamProcessor != null)
                _changeStreamProcessor.ExecutionCancelled = true;
            
            _changeStreamProcessor = null;
        }


        public void StartProcess(MigrationUnit item, string sourceConnectionString, string targetConnectionString, string idField = "_id")
        {
            int maxRetries = 10;
            string jobId = _job.Id;

            TimeSpan backoff = TimeSpan.FromSeconds(2);

            string dbName = item.DatabaseName;
            string colName = item.CollectionName;

            var database = _sourceClient.GetDatabase(dbName);
            var collection = database.GetCollection<BsonDocument>(colName);

            DateTime migrationJobStartTime = DateTime.Now;

            Log.WriteLine($"{dbName}.{colName} Document copy started");

            if (!item.DumpComplete && !_executionCancelled)
            {
                item.EstimatedDocCount = collection.EstimatedDocumentCount();

#pragma warning disable CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed
                Task.Run(() =>
                {
                    long count = MongoHelper.GetActualDocumentCount(collection, item);
                    item.ActualDocCount = count;
                    _jobs?.Save();
                });
#pragma warning restore CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed

                long downloadCount = 0;

                for (int i = 0; i < item.MigrationChunks.Count; i++)
                {
                    if (_executionCancelled || !_job.CurrentlyActive) return;

                    double initialPercent = ((double)100 / item.MigrationChunks.Count) * i;
                    double contributionFactor = 1.0 / item.MigrationChunks.Count;

                    long docCount = 0;

                    if (!item.MigrationChunks[i].IsDownloaded == true)
                    {
                        int dumpAttempts = 0;
                        backoff = TimeSpan.FromSeconds(2);
                        bool continueProcessing = true;

                        while (dumpAttempts < maxRetries && !_executionCancelled && continueProcessing && _job.CurrentlyActive)
                        {
                            dumpAttempts++;
                            FilterDefinition<BsonDocument> filter;
                            try
                            {
                                if (item.MigrationChunks.Count > 1)
                                {
                                    var bounds = SamplePartitioner.GetChunkBounds(item.MigrationChunks[i].Gte, item.MigrationChunks[i].Lt, item.MigrationChunks[i].DataType);
                                    var gte = bounds.gte;
                                    var lt = bounds.lt;

                                    Log.WriteLine($"{dbName}.{colName}-Chunk [{i}] generating query");
                                    Log.Save();

                                    // Generate query and get document count
                                    filter = MongoHelper.GenerateQueryFilter(gte, lt, item.MigrationChunks[i].DataType);

                                    docCount = MongoHelper.GetDocumentCount(collection, filter);

                                    item.MigrationChunks[i].DumpQueryDocCount = docCount;

                                    downloadCount += item.MigrationChunks[i].DumpQueryDocCount;

                                    Log.WriteLine($"{dbName}.{colName}- Chunk [{i}] Count is  {docCount}");
                                    Log.Save();
                                }
                                else
                                {
                                    filter = Builders<BsonDocument>.Filter.Empty;
                                    docCount = MongoHelper.GetDocumentCount(collection, filter);

                                    item.MigrationChunks[i].DumpQueryDocCount = docCount;
                                    downloadCount = docCount;
                                }

                                _cts = new CancellationTokenSource();

                                if (_targetClient == null)
                                    _targetClient = new MongoClient(targetConnectionString);

                                var documentCopier = new MongoDocumentCopier();
                                documentCopier.Initialize(_targetClient, collection, dbName, colName, _config.MongoCopyPageSize);
                                var result = documentCopier.CopyDocumentsAsync(_jobs, item, i, initialPercent, contributionFactor, docCount, filter, _cts.Token,_job.IsSimulatedRun).GetAwaiter().GetResult();

                                if (result)
                                {
                                    if (!_cts.IsCancellationRequested)
                                    {
                                        continueProcessing = false;
                                        item.MigrationChunks[i].IsDownloaded = true;
                                        item.MigrationChunks[i].IsUploaded = true;                                        
                                    }
                                    _jobs?.Save(); // Persist state
                                    dumpAttempts = 0;
                                }
                                else
                                {
                                    Log.WriteLine($"Attempt {dumpAttempts} {dbName}.{colName}-{i} of Document copy failed. Retrying in {backoff.TotalSeconds} seconds...");
                                    Thread.Sleep(backoff);
                                    backoff = TimeSpan.FromTicks(backoff.Ticks * 2);
                                }
                            }
                            catch (MongoExecutionTimeoutException ex)
                            {
                                Log.WriteLine($" Document copy attempt {dumpAttempts} failed due to timeout: {ex.ToString()}.Details:{ex.ToString()}", LogType.Error);

                                if (dumpAttempts >= maxRetries)
                                {
                                    Log.WriteLine("Maximum Document copy attempts reached. Aborting operation.", LogType.Error);
                                    Log.Save();

                                    _job.CurrentlyActive = false;
                                    _jobs?.Save();

                                    ProcessRunning = false;
                                }

                                // Wait for the backoff duration before retrying
                                Log.WriteLine($"Retrying in {backoff.TotalSeconds} seconds...", LogType.Error);
                                Thread.Sleep(backoff);
                                Log.Save();

                                // Exponentially increase the backoff duration
                                backoff = TimeSpan.FromTicks(backoff.Ticks * 2);
                            }
                            catch (Exception ex)
                            {
                                Log.WriteLine(ex.ToString(), LogType.Error);
                                Log.Save();

                                _job.CurrentlyActive = false;
                                _jobs?.Save();
                                ProcessRunning = false;
                            }
                        }
                        if (dumpAttempts == maxRetries)
                        {
                            _job.CurrentlyActive = false;
                            _jobs?.Save();
                            ProcessRunning = false;
                        }
                    }
                    else
                    {
                        downloadCount += item.MigrationChunks[i].DumpQueryDocCount;
                    }
                }

                item.DumpGap = Math.Max(item.ActualDocCount, item.EstimatedDocCount) - downloadCount;
                item.RestoreGap = item.DumpGap;
                item.DumpPercent = 100;
                item.RestorePercent = 100;
                item.DumpComplete = true;
                item.RestoreComplete = true;
            }
            if (item.RestoreComplete && item.DumpComplete && !_executionCancelled)
            {
                try
                {
                    // Process change streams
                    if (_job.IsOnline && !_executionCancelled)
                    {
                        if (_targetClient == null)
                            _targetClient = new MongoClient(targetConnectionString);

                        Log.WriteLine($"{dbName}.{colName} ProcessCollectionChangeStream invoked");

                        if (_changeStreamProcessor == null)
                            _changeStreamProcessor = new MongoChangeStreamProcessor(_sourceClient, _targetClient, _jobs, _config);

#pragma warning disable CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed
                        Task.Run(() => _changeStreamProcessor.ProcessCollectionChangeStream(_job,item));
#pragma warning restore CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed
                    }

                    if (!_job.IsOnline && !_executionCancelled)
                    {
                        var migrationJob = _jobs.MigrationJobs.Find(m => m.Id == jobId);
                        if (Helper.IsOfflineJobCompleted(migrationJob))
                        {
                            Log.WriteLine($"{migrationJob.Id} Completed");

                            migrationJob.IsCompleted = true;
                            migrationJob.CurrentlyActive = false;
                            _job.CurrentlyActive = false;
                            ProcessRunning = false;
                            _jobs?.Save();
                        }
                    }
                }
                catch
                {
                    // Do nothing
                }
            }
        }
    }
}
