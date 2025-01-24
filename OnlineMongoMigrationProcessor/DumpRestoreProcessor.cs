using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

#pragma warning disable CS8602
#pragma warning disable CS8604
#pragma warning disable CS8600

namespace OnlineMongoMigrationProcessor
{
    internal class DumpRestoreProcessor: IMigrationProcessor
    {
        private Joblist? Jobs;
        MigrationJob? Job;
        string toolsLaunchFolder = string.Empty;
        bool ExecutionCancelled = false;
        string MongoDumpOutputFolder = $"{Path.GetTempPath()}mongodump";
        private MongoClient? sourceClient;
        private MongoClient? targetClient;
        MigrationSettings? Config;

        public bool ProcessRunning { get; set; }

        ProcessExecutor PExecutor;

        MongoChangeStreamProcessor CSProcessor;

        public DumpRestoreProcessor(Joblist _Jobs,MigrationJob _Job, MongoClient _sourceClient, MigrationSettings _config, string _toolsLaunchFolder)
        {
            Jobs = _Jobs;
            Job = _Job;
            toolsLaunchFolder = _toolsLaunchFolder;
            sourceClient = _sourceClient;
            Config= _config;

            PExecutor = new ProcessExecutor();
        }

        public void StopProcessing()
        {
            ProcessRunning = false;
            ExecutionCancelled = true;
            PExecutor.Terminate();

            if (CSProcessor != null)
                CSProcessor.ExecutionCancelled = true;  
        }

        public void Download(MigrationUnit item, string sourceConnectionString, string targetConnectionstring, string idField = "_id")
        {

            int maxRetries = 10;
            string jobId = Job.Id;

            TimeSpan backoff = TimeSpan.FromSeconds(2);

            string dbName = item.DatabaseName;
            string colName = item.CollectionName;

            //mongodump output folder create if not exist
            string folder = $"{MongoDumpOutputFolder}\\{jobId}\\{dbName}.{colName}";
            System.IO.Directory.CreateDirectory(folder);

            var database = sourceClient.GetDatabase(dbName);
            var collection = database.GetCollection<BsonDocument>(colName);

            bool restoreInvoked = false;


            DateTime MigrationJobStartTime = DateTime.Now;

            Log.WriteLine($"{dbName}.{colName} Downloader started");

            // MongoDump
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
                            string args = $" --uri=\"{sourceConnectionString}\" --gzip --db={dbName} --collection={colName}  --out {folder}\\{i}.bson";
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
                                    string query = MongoHelper.GenerateQueryString(gte, lt, item.MigrationChunks[i].DataType);

                                    docCount = MongoHelper.GetDocCount(collection, gte, lt, item.MigrationChunks[i].DataType);


                                    item.MigrationChunks[i].DumpQueryDocCount = docCount;

                                    downloadCount = downloadCount + item.MigrationChunks[i].DumpQueryDocCount;

                                    Log.WriteLine($"{dbName}.{colName}- Chunk [{i}] Count is  {docCount}");
                                    Log.Save();

                                    args = $"{args} --query=\"{query}\"";
                                }

                                if (System.IO.Directory.Exists($"folder\\{i}.bson"))
                                    System.IO.Directory.Delete($"folder\\{i}.bson", true);


                                var task = Task.Run(() => PExecutor.Execute(Jobs, item, item.MigrationChunks[i], initialPercent, contributionfactor, docCount, $"{toolsLaunchFolder}\\mongodump.exe", args));
                                task.Wait(); // Wait for the task to complete
                                bool result = task.Result; // Capture the result after the task completes
                                                                
                                if (result)
                                {
                                    continueProcessing = false;
                                    item.MigrationChunks[i].IsDownloaded = true;
                                    Jobs?.Save(); //persists state
                                    dumpAttempts = 0;

                                    if (!restoreInvoked)
                                    {
                                        Log.WriteLine($"{dbName}.{colName} Uploader invoked");

                                        restoreInvoked = true;
                                        Task.Run(() => Upload(item, targetConnectionstring));
                                    }
                                }
                                else
                                {
                                    Log.WriteLine($"Attempt {dumpAttempts} {dbName}.{colName}-{i} of Dump Executor failed. Retrying in {backoff.TotalSeconds} seconds...");
                                    Thread.Sleep(backoff);
                                    backoff = TimeSpan.FromTicks(backoff.Ticks * 2);
                                    //System.Threading.Thread.Sleep(10000);
                                }
                            }
                            catch (MongoExecutionTimeoutException ex)
                            {

                                Log.WriteLine($" Dump attempt {dumpAttempts} failed due to timeout: {ex.Message}", LogType.Error);

                                if (dumpAttempts >= maxRetries)
                                {
                                    Log.WriteLine("Maximum dump attempts reached. Aborting operation.", LogType.Error);
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
                item.DumpPercent = 100;
                item.DumpComplete = true;
            }
            else if (item.DumpComplete && !ExecutionCancelled)
            {
                if (!restoreInvoked)
                {
                    Log.WriteLine($"{dbName}.{colName} Uploader invoked");

                    restoreInvoked = true;
                    Task.Run(() => Upload(item, targetConnectionstring));
                }
            }
        }

        public void Upload(MigrationUnit item, string targetConnectionString)
        {
            string dbName = item.DatabaseName;
            string colName = item.CollectionName;
            int maxRetries = 10;
            string jobId = Job.Id;

            TimeSpan backoff = TimeSpan.FromSeconds(2);

            string folder = $"{MongoDumpOutputFolder}\\{jobId}\\{dbName}.{colName}";

            Log.WriteLine($"{dbName}.{colName} Uploader started");

            while (!item.RestoreComplete && System.IO.Directory.Exists(folder) && !ExecutionCancelled && Job.CurrentlyActive)
            {
                int restoredChunks = 0;
                long restoredDocs = 0;
                // MongoRestore
                if (!item.RestoreComplete && !ExecutionCancelled)
                {
                    for (int i = 0; i < item.MigrationChunks.Count; i++)
                    {
                        if (ExecutionCancelled) return;

                        if (!item.MigrationChunks[i].IsUploaded == true && item.MigrationChunks[i].IsDownloaded == true)
                        {
                            string args = $" --uri=\"{targetConnectionString}\" --gzip {folder}\\{i}.bson";

                            //if first item drop collection, else append
                            if (i == 0)
                                args = $"{args} --drop";
                            else
                                args = $"{args} --noIndexRestore"; //no index for subsequent items.


                            double initialPercent = ((double)100 / ((double)item.MigrationChunks.Count)) * i;
                            //double contributionfactor = (double)1 / (double)item.MigrationChunks.Count;

                            double contributionfactor = (double)item.MigrationChunks[i].DumpQueryDocCount / (double)Math.Max(item.ActualDocCount, item.EstimatedDocCount);
                            if (item.MigrationChunks.Count == 1) contributionfactor = 1;

                            Log.WriteLine($"{dbName}.{colName}-{i} Uploader processing");

                            int restoreAttempts = 0;
                            backoff = TimeSpan.FromSeconds(2);
                            bool continueProcessing = true;
                            while (restoreAttempts < maxRetries && !ExecutionCancelled && continueProcessing && !item.RestoreComplete && Job.CurrentlyActive)
                            {
                                restoreAttempts++;
                                try
                                {
                                    if (PExecutor.Execute(Jobs, item, item.MigrationChunks[i], initialPercent, contributionfactor, 0, $"{toolsLaunchFolder}\\mongorestore.exe", args))
                                    {
                                        continueProcessing = false;
                                        item.MigrationChunks[i].IsUploaded = true;
                                        Jobs?.Save(); //persists state

                                        if (item.MigrationChunks[i].RestoredFailedDocCount > 0)
                                        {
                                            if (targetClient == null)
                                                targetClient = new MongoClient(targetConnectionString);

                                            var targetDb = targetClient.GetDatabase(item.DatabaseName);
                                            var targetCollection = targetDb.GetCollection<BsonDocument>(item.CollectionName);

                                            var bounds = SamplePartitioner.GetChunkBounds(item.MigrationChunks[i].Gte, item.MigrationChunks[i].Lt, item.MigrationChunks[i].DataType);
                                            var gte = bounds.gte;
                                            var lt = bounds.lt;

                                            item.MigrationChunks[i].DocCountInTarget = MongoHelper.GetDocCount(targetCollection, gte, lt, item.MigrationChunks[i].DataType);

                                            if (item.MigrationChunks[i].DocCountInTarget == item.MigrationChunks[i].DumpResultDocCount)
                                            {
                                                Log.WriteLine($"{dbName}.{colName}-{i} No documents missing, count in Target: {item.MigrationChunks[i].DocCountInTarget}");
                                                Log.Save();
                                            }

                                            Jobs?.Save(); //persists state
                                        }

                                        restoreAttempts = 0;

                                        restoredChunks++;
                                        restoredDocs = restoredDocs + Math.Max(item.MigrationChunks[i].RestoredSucessDocCount, item.MigrationChunks[i].DocCountInTarget);
                                        try
                                        {
                                            System.IO.Directory.Delete($"{folder}\\{i}.bson", true);
                                        }
                                        catch { }
                                    }
                                    else
                                    {
                                        Log.WriteLine($"Attempt {restoreAttempts} {dbName}.{colName}-{i} of Restore Executor failed");
                                        //System.Threading.Thread.Sleep(10000);
                                    }
                                }
                                catch (MongoExecutionTimeoutException ex)
                                {
                                    Log.WriteLine($" Restore attempt {restoreAttempts} failed due to timeout: {ex.Message}", LogType.Error);

                                    if (restoreAttempts >= maxRetries)
                                    {
                                        Log.WriteLine("Maximum retry attempts reached. Aborting operation.", LogType.Error);
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
                            if (restoreAttempts == maxRetries)
                            {
                                Log.WriteLine("Maximum restore attempts reached. Aborting operations.", LogType.Error);
                                Log.Save();

                                Job.CurrentlyActive = false;
                                Jobs?.Save();
                                ProcessRunning = false;
                            }
                        }
                        else if (item.MigrationChunks[i].IsUploaded == true)
                        {
                            restoredChunks++;
                            restoredDocs = restoredDocs + item.MigrationChunks[i].RestoredSucessDocCount;
                        }
                    }

                    if (restoredChunks == item.MigrationChunks.Count && !ExecutionCancelled)
                    {
                        item.RestoreGap = Math.Max(item.ActualDocCount, item.EstimatedDocCount) - restoredDocs;
                        item.RestorePercent = 100;
                        item.RestoreComplete = true;
                        Jobs?.Save(); //persists state
                    }
                    else
                    {
                        System.Threading.Thread.Sleep(10000);
                    }
                }
            }
            if (item.RestoreComplete && item.DumpComplete)
            {
                try
                {
                    System.IO.Directory.Delete(folder, true);
                    // Process change streams
                    if (Job.IsOnline && !ExecutionCancelled)
                    {
                        if (targetClient == null)
                            targetClient = new MongoClient(targetConnectionString);

                        Log.WriteLine($"{dbName}.{colName} ProcessCollectionChangeStream invoked");

                        if(CSProcessor== null)
                            CSProcessor = new MongoChangeStreamProcessor(sourceClient,targetClient, Jobs, Config);

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
