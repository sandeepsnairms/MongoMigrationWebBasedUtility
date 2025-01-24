using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

#pragma warning disable CS8602
#pragma warning disable CS8604
#pragma warning disable CS8600

namespace OnlineMongoMigrationProcessor
{
    internal class CopyProcessor: IMigrationProcessor
    {
        private Joblist? Jobs;
        MigrationJob? Job;

        bool ExecutionCancelled = false;

        private MongoClient? sourceClient;
        private MongoClient? targetClient;
        MigrationSettings? Config;

        public bool ProcessRunning { get; set; }

        CancellationTokenSource cts;
        MongoChangeStreamProcessor CSProcessor;

        public CopyProcessor(Joblist _Jobs,MigrationJob _Job, MongoClient _sourceClient, MigrationSettings _config)
        {
            Jobs = _Jobs;
            Job = _Job;

            sourceClient = _sourceClient;
            Config= _config;
        }

        public void StopProcessing()
        {
            ProcessRunning = false;
            ExecutionCancelled = true;

            if(cts!=null)
                cts.Cancel();

            if (CSProcessor != null)
                CSProcessor.ExecutionCancelled = true;
        }

        public void Upload(MigrationUnit item, string targetConnectionString)
        {
            throw new NotImplementedException();    
        }

        public async void Download(MigrationUnit item, string sourceConnectionString, string targetConnectionstring, string idField = "_id")
        {

            int maxRetries = 10;
            string jobId = Job.Id;

            TimeSpan backoff = TimeSpan.FromSeconds(2);

            string dbName = item.DatabaseName;
            string colName = item.CollectionName;


            var database = sourceClient.GetDatabase(dbName);
            var collection = database.GetCollection<BsonDocument>(colName);
              

            DateTime MigrationJobStartTime = DateTime.Now;

            Log.WriteLine($"{dbName}.{colName} DocumentCopy started");

 
            if (!item.DumpComplete && !ExecutionCancelled)
            {
                item.EstimatedDocCount = collection.EstimatedDocumentCount();

                Task.Run(() =>
                {
                    long count = MongoHelper.GetActualDocumentCount(collection, item);
                    item.ActualDocCount = count;
                    Jobs?.Save();
                });

                long downloadCount = 0;

                for (int i = 0; i < item.MigrationChunks.Count; i++)
                {

                    if (ExecutionCancelled || !Job.CurrentlyActive) return;

                    double initialPercent = ((double)100 / ((double)item.MigrationChunks.Count)) * i;
                    double contributionfactor = (double)1 / (double)item.MigrationChunks.Count;

                    long docCount = 0;

                    if (!item.MigrationChunks[i].IsDownloaded == true)
                    {
                        int dumpAttempts = 0;
                        backoff = TimeSpan.FromSeconds(2);
                        bool continueProcessing = true;

                        while (dumpAttempts < maxRetries && !ExecutionCancelled && continueProcessing && Job.CurrentlyActive)
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

                                    //// Generate query and get document count
                                    filter = MongoHelper.GenerateQueryFilter(gte, lt, item.MigrationChunks[i].DataType);

                                    docCount = MongoHelper.GetDocCount(collection, filter);

                                    item.MigrationChunks[i].DumpQueryDocCount = docCount;

                                    downloadCount = downloadCount + item.MigrationChunks[i].DumpQueryDocCount;

                                    Log.WriteLine($"{dbName}.{colName}- Chunk [{i}] Count is  {docCount}");
                                    Log.Save();

                                }
                                else
                                {
                                    filter = Builders<BsonDocument>.Filter.Empty;
                                    docCount = MongoHelper.GetDocCount(collection, filter);

                                    item.MigrationChunks[i].DumpQueryDocCount = docCount;
                                    downloadCount = docCount;
                                }

                                cts = new CancellationTokenSource();
                                                                                                

                                if (targetClient == null)
                                    targetClient = new MongoClient(targetConnectionstring);

                                var MCopier = new MongoDocumentCopier();
                                MCopier.Initialize(sourceClient, targetClient, collection, dbName, colName);
                                var result=  await MCopier.CopyDocumentsAsync(Jobs, item, i, initialPercent, contributionfactor, docCount, filter, cts.Token);

                                if (result)
                                {
                                    continueProcessing = false;
                                    item.MigrationChunks[i].IsDownloaded = true;
                                    item.MigrationChunks[i].IsUploaded = true;
                                    Jobs?.Save(); //persists state 
                                    dumpAttempts = 0;
                                }
                                else
                                {
                                    Log.WriteLine($"Attempt {dumpAttempts} {dbName}.{colName}-{i} of DocumentCopy failed. Retrying in {backoff.TotalSeconds} seconds...");
                                    Thread.Sleep(backoff);
                                    backoff = TimeSpan.FromTicks(backoff.Ticks * 2);
                                    //System.Threading.Thread.Sleep(10000);
                                }
                            }
                            catch (MongoExecutionTimeoutException ex)
                            {

                                Log.WriteLine($" DocumentCopy attempt {dumpAttempts} failed due to timeout: {ex.Message}", LogType.Error);

                                if (dumpAttempts >= maxRetries)
                                {
                                    Log.WriteLine("Maximum DocumentCopy attempts reached. Aborting operation.", LogType.Error);
                                    Log.Save();

                                    Job.CurrentlyActive = false;
                                    Jobs?.Save();

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

                                Job.CurrentlyActive = false;
                                Jobs?.Save();
                                ProcessRunning = false;

                            }

                        }
                        if (dumpAttempts == maxRetries)
                        {
                            Job.CurrentlyActive = false;
                            Jobs?.Save();
                        }

                    }
                    else
                    {
                        downloadCount = downloadCount + item.MigrationChunks[i].DumpQueryDocCount;
                    }
                }
                
                item.DumpGap = Math.Max(item.ActualDocCount, item.EstimatedDocCount) - downloadCount;
                item.RestoreGap = item.DumpGap;
                item.DumpPercent = 100;
                item.RestorePercent = 100;
                item.DumpComplete = true;
                item.RestoreComplete = true;
            }
            if( item.RestoreComplete && item.DumpComplete && !ExecutionCancelled)
            {

                try
                {

                    // Process change streams
                    if (Job.IsOnline && !ExecutionCancelled)
                    {
                        if (targetClient == null)
                            targetClient = new MongoClient(targetConnectionstring);

                        Log.WriteLine($"{dbName}.{colName} ProcessCollectionChangeStream invoked");

                        if (CSProcessor == null)
                            CSProcessor = new MongoChangeStreamProcessor(sourceClient, targetClient, Jobs, Config);

                        Task.Run(() => CSProcessor.ProcessCollectionChangeStream(item));

                    }

                    if (!Job.IsOnline && !ExecutionCancelled)
                    {

                        var migrationJob = Jobs.MigrationJobs.Find(m => m.Id == jobId);
                        if (Helper.IsOfflineJobCompleted(migrationJob))
                        {
                            Log.WriteLine($"{migrationJob.Id} Terminated");

                            migrationJob.IsCompleted = true;
                            migrationJob.CurrentlyActive = false;
                            Job.CurrentlyActive = false;
                            ProcessRunning = false;
                            Jobs?.Save();
                        }
                    }
                }
                catch
                {
                    //do nothing
                }


            }
        }
        

       


        
    }
}
