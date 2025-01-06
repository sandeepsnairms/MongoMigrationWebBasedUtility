using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Driver;
using System.IO.Compression;
using System.Net.Http;
using System.Xml.Linq;
using OnlineMongoMigrationProcessor;
using System.Threading;
using MongoDB.Driver.Core.Configuration;
using System.Collections;
using MongoDB.Bson.IO;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json;
using JsonConvert = Newtonsoft.Json.JsonConvert;
using System.Reflection.Metadata.Ecma335;
using System.Security.Cryptography;
using static System.Net.WebRequestMethods;
using System.Collections.Concurrent;
using System.ComponentModel.DataAnnotations;
using System.Numerics;

namespace OnlineMongoMigrationProcessor
{
#pragma warning disable CS8629
#pragma warning disable CS8600
#pragma warning disable CS8602
#pragma warning disable CS8603
#pragma warning disable CS8604
#pragma warning disable CS8625

    public class MigrationWorker
    {

        string toolsDestinationFolder=$"{Path.GetTempPath()}mongo-tools";
        string toolsLaunchFolder=string.Empty;
        //string MongoDumpOutputFolder= $"{Path.GetTempPath()}mongodump";
        //DateTime MigrationJobStartTime = DateTime.Now;
        //bool Online = true;
        //bool BulkCopy = true;
        bool MigrationCancelled = false;

        public bool ProcessRunning { get; set; }

        private Joblist? Jobs;

        public MigrationSettings? Config;

        private MongoClient? sourceClient;
        //private MongoClient? targetClient;

        MigrationJob? Job;
        DataProcessor DProcessor;


        public string? CurrentJobId { get; set; }

        public MigrationWorker(Joblist jobs)
        {
            this.Jobs= jobs;            
        }

        public  void StopMigration()
        {
            MigrationCancelled = true;
            DProcessor.StopProcessing();
            ProcessRunning = false;

            DProcessor = null;
        }


        public async Task StartMigrationAsync(MigrationJob _job, string sourceConnectionString, string targetConnectionString, string namespacesToMigrate, bool doBulkCopy, bool trackChangeStreams)
        {
            int maxRetries = 10;
            int attempts = 0;

            TimeSpan backoff = TimeSpan.FromSeconds(2);

            if (Config == null)
            {
                Config = new MigrationSettings();
                Config.Load();
            }

            try
            {
                if (DProcessor != null)
                {
                    DProcessor.StopProcessing();
                    DProcessor = null;
                }
            }
            catch { }   

            

            Job = _job;

            ProcessRunning = true;
            MigrationCancelled = false;
            CurrentJobId = Job.Id;

            Log.init(Job.Id);

            Log.WriteLine($"{Job.Id} Started on  {Job.StartedOn.ToString()}");
            Log.Save();

            string[] collectionsInput = namespacesToMigrate
                .Split(',')
                .Select(item => item.Trim())
                .ToArray();


            // Ensure MongoDB tools are available        
            toolsLaunchFolder = await Helper.EnsureMongoToolsAvailableAsync(toolsDestinationFolder, Config.MongoToolsDownloadURL);          

            bool continueProcessing =true;   


            if (Job.MigrationUnits == null)
            {
                // Storing migration metadata
                Job.MigrationUnits = new List<MigrationUnit>();
            }
            if (Job.MigrationUnits.Count == 0)
            {
                // pocess collections one by one
                foreach (var fullName in collectionsInput)
                {
                    if (MigrationCancelled) return;

                    string[] parts = fullName.Split('.');
                    if (parts.Length != 2) continue;

                    string dbName = parts[0].Trim();
                    string colName = parts[1].Trim();

                    var mu = new MigrationUnit(dbName, colName, null);
                    Job.MigrationUnits.Add(mu);
                    Jobs?.Save();
                }
            }

            

            attempts = 0;
            backoff = TimeSpan.FromSeconds(2);

            while (attempts < maxRetries && !MigrationCancelled && continueProcessing)
            {
                attempts++;
                try
                {
                    sourceClient = new MongoClient(sourceConnectionString);
                    Log.WriteLine($"Source Connection Sucessfull");
                    Log.Save();

                    if(DProcessor==null)
                        DProcessor = new DataProcessor(Jobs, Job, toolsLaunchFolder, sourceClient);

                    foreach (var unit in Job.MigrationUnits)
                    {
                        if (MigrationCancelled)
                            return;

                        if(unit.MigrationChunks==null|| unit.MigrationChunks.Count == 0)
                        { 
                            var chunks = await PartitionCollection(unit.DatabaseName, unit.CollectionName);

                            Log.WriteLine($"{unit.DatabaseName}.{unit.CollectionName} has {chunks.Count} Chunks");
                            Log.Save();

                            unit.MigrationChunks = chunks;
                        }

                    }
                    Jobs?.Save();
                    Log.Save();

                    if (true) //only used for debugging
                    {
                        // Process each group
                        foreach (var migrationUnit in Job.MigrationUnits)
                        {
                            if (MigrationCancelled) break;
                            //Downloader(migrationUnit, sourceConnectionString, targetConnectionString);
                                                        
                            DProcessor.Download(migrationUnit, sourceConnectionString, targetConnectionString);
                        }
                    }
                    else
                        Log.WriteLine("Skipping Bulk Copy");

                    continueProcessing = false;
                }
                catch (MongoExecutionTimeoutException ex)
                {
                    Log.WriteLine($"Attempt {attempts} failed due to timeout: {ex.Message}", LogType.Error);

                    if (attempts >= maxRetries)
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

                    continueProcessing = true;
                    // Exponentially increase the backoff duration
                    backoff = TimeSpan.FromTicks(backoff.Ticks * 2);
                }
                catch (Exception ex)
                {
                    Log.WriteLine(ex.ToString(), LogType.Error);
                    Log.Save();

                    Job.CurrentlyActive = false;
                    Jobs?.Save();
                    continueProcessing = false;
                    ProcessRunning = false;
                }

            }
            
        }

        

        private async Task<List<MigrationChunk>> PartitionCollection(string databaseName, string collectionName, string idField = "_id")
        {
            var database = sourceClient.GetDatabase(databaseName);
            var collection = database.GetCollection<BsonDocument>(collectionName);

            // Target chunk size in bytes
            long targetChunkSizeBytes = Config.ChunkSizeInMB * 1024 *1024; // converting to bytes

            // Get the total size of the collection in bytes
            var statsCommand = new BsonDocument { { "collStats", collectionName } };
            var stats = await database.RunCommandAsync<BsonDocument>(statsCommand);
            long totalCollectionSizeBytes = stats["storageSize"].ToInt64();
            var documentCount = stats["count"].AsInt32;

            Log.WriteLine($"{databaseName}.{collectionName}Storage Size: {totalCollectionSizeBytes}");
                        
            int totalChunks = (int)Math.Ceiling((double)totalCollectionSizeBytes / targetChunkSizeBytes);
            List<MigrationChunk> migrationChunks = new List<MigrationChunk>();
            if (totalChunks > 1)
            {
                Log.WriteLine($"Creating Partitions for { databaseName}.{ collectionName}");
                Log.Save();


                var partitioner = new SamplePartitioner(collection);

                // List of data types to process
                List<DataType> dataTypes = new List<DataType>{DataType.Int, DataType.Int64, DataType.String, DataType.Object, DataType.Decimal128, DataType.Date, DataType.ObjectId };

                if(Config.HasUUID)
                    dataTypes.Add(DataType.UUID);

                foreach (var dataType in dataTypes)
                {
                    // Create partitions for the current data type
                    List<(BsonValue Min, BsonValue Max)> partitions = partitioner.CreatePartitions(idField, totalChunks, dataType, documentCount/ totalChunks);

                    if (partitions == null)
                        continue;


                    // Add the partitions to migrationChunks
                    for (int i = 0; i < partitions.Count; i++)
                    {
                        string startId;
                        string endId;

                        if (i == 0) // First partition, no gte
                        {
                            startId = "";
                            endId = partitions[0].Max.ToString();
                        }
                        else if (i == partitions.Count - 1) // Last partition, no lte
                        {
                            startId = partitions[i].Min.ToString();
                            endId = "";
                        }
                        else // Middle partitions
                        {
                            startId = partitions[i].Min.ToString();
                            endId = partitions[i].Max.ToString();
                        }

                        // Add the current partition as a migration chunk
                        migrationChunks.Add(new MigrationChunk(startId, endId, dataType, false, false));
                    }
                }

            }
            else
            {
                //single chunk in case of data set being small.
                migrationChunks.Add(new MigrationChunk(null, null,DataType.String ,false, false));
            }

            return migrationChunks;
        }

        

        //void Downloader( MigrationUnit item, string sourceConnectionString, string targetConnectionstring,string idField="_id")
        //{
            
        //    int maxRetries = 10;
        //    string jobId = Job.Id;

        //    TimeSpan backoff = TimeSpan.FromSeconds(2);

        //    string dbName = item.DatabaseName;
        //    string colName = item.CollectionName;

        //    //mongodump output folder create if not exist
        //    string folder = $"{MongoDumpOutputFolder}\\{jobId}\\{dbName}.{colName}";
        //    System.IO.Directory.CreateDirectory(folder);
                        
        //    var database = sourceClient.GetDatabase(dbName);
        //    var collection = database.GetCollection<BsonDocument>(colName);

        //    bool restoreInvoked=false;


        //    Log.WriteLine($"{dbName}.{colName} Downloader started");

        //    // MongoDump
        //    if (!item.DumpComplete && !MigrationCancelled)
        //    {
        //        item.EstimatedDocCount = collection.EstimatedDocumentCount();

        //        Task.Run(() =>
        //        {
        //            long count=MongoHelper.GetActualDocumentCount(collection, item);
        //            item.ActualDocCount = count;
        //            Jobs?.Save();
        //        });

        //        long downloadCount=0;

        //        for (int i = 0; i < item.MigrationChunks.Count; i++)
        //        {

        //            if (MigrationCancelled || !Job.CurrentlyActive) return;

        //            double initialPercent = ((double)100 / ((double)item.MigrationChunks.Count)) * i;
        //            double contributionfactor = (double)1 / (double)item.MigrationChunks.Count;

        //            long docCount=0;

        //            if (!item.MigrationChunks[i].IsDownloaded == true)
        //            {
        //                int dumpAttempts = 0;
        //                backoff = TimeSpan.FromSeconds(2);
        //                bool continueProcessing = true;

        //                while (dumpAttempts < maxRetries && !MigrationCancelled && continueProcessing && Job.CurrentlyActive)
        //                {


        //                    dumpAttempts++;
        //                    string args = $" --uri=\"{sourceConnectionString}\" --gzip --db={dbName} --collection={colName}  --out {folder}\\{i}.bson";
        //                    try
        //                    {
        //                        if (item.MigrationChunks.Count > 1)
        //                        {

        //                            var bounds = SamplePartitioner.GetChunkBounds(item.MigrationChunks[i]);
        //                            var gte = bounds.gte;
        //                            var lt = bounds.lt;


        //                            Log.WriteLine($"{dbName}.{colName}-Chunk[{i}] generating query");
        //                            Log.Save();

        //                            // Generate query and get document count
        //                            string query = MongoHelper.GenerateQueryString(gte, lt, item.MigrationChunks[i].DataType); 

        //                            docCount = MongoHelper.GetDocCount(collection, gte, lt,item.MigrationChunks[i].DataType );


        //                            item.MigrationChunks[i].DumpQueryDocCount = docCount;

        //                            downloadCount = downloadCount + item.MigrationChunks[i].DumpQueryDocCount;

        //                            Log.WriteLine($"{dbName}.{colName}- Chunk[{i}] Count is  {docCount}");
        //                            Log.Save();

        //                            args = $"{args} --query=\"{query}\"";
        //                        }

        //                        if(System.IO.Directory.Exists($"folder\\{i}.bson"))
        //                            System.IO.Directory.Delete($"folder\\{ i}.bson",true);


        //                        var task = Task.Run(() => ProcessExecutor.Execute(Jobs, item, item.MigrationChunks[i], initialPercent, contributionfactor, docCount, $"{toolsLaunchFolder}\\mongodump.exe", args));
        //                        task.Wait(); // Wait for the task to complete
        //                        bool result = task.Result; // Capture the result after the task completes

        //                        //if (ProcessExecutor.Execute(Jobs, item, item.MigrationChunks[i], initialPercent, contributionfactor, docCount, $"{toolsLaunchFolder}\\mongodump.exe", args))
        //                        if (result)
        //                        {
        //                            continueProcessing = false;
        //                            item.MigrationChunks[i].IsDownloaded = true;
        //                            Jobs?.Save(); //persists state
        //                            dumpAttempts = 0;

        //                            if (!restoreInvoked)
        //                            {
        //                                Log.WriteLine($"{dbName}.{colName} Uploader invoked");

        //                                restoreInvoked = true;
        //                                Task.Run(() => Uploader(item, targetConnectionstring));
        //                            }
        //                        }
        //                        else
        //                        {
        //                            Log.WriteLine($"Attempt {dumpAttempts} {dbName}.{colName}-{i} of Dump Executor failed");

        //                            //System.Threading.Thread.Sleep(10000);
        //                        }
        //                    }                            
        //                    catch (MongoExecutionTimeoutException ex)
        //                    {

        //                        Log.WriteLine($" Dump attempt {dumpAttempts} failed due to timeout: {ex.Message}", LogType.Error);

        //                        if (dumpAttempts >= maxRetries)
        //                        {
        //                            Log.WriteLine("Maximum dump attempts reached. Aborting operation.", LogType.Error);
        //                            Log.Save();

        //                            Job.CurrentlyActive = false;
        //                            Jobs?.Save();

        //                            ProcessRunning = false;
                                    
        //                        }

        //                        // Wait for the backoff duration before retrying
        //                        Log.WriteLine($"Retrying in {backoff.TotalSeconds} seconds...", LogType.Error);
        //                        Thread.Sleep(backoff);
        //                        Log.Save();

        //                        // Exponentially increase the backoff duration
        //                        backoff = TimeSpan.FromTicks(backoff.Ticks * 2);
        //                    }
        //                    catch (Exception ex)
        //                    {
        //                        Log.WriteLine(ex.ToString(), LogType.Error);
        //                        Log.Save();

        //                        Job.CurrentlyActive = false;
        //                        Jobs?.Save();
        //                        ProcessRunning = false;

        //                    }

        //                }
        //                if (dumpAttempts == maxRetries)
        //                {
        //                    Job.CurrentlyActive = false;
        //                    Jobs?.Save();
        //                }
                                
        //            }
        //            else
        //            {
        //                downloadCount= downloadCount+ item.MigrationChunks[i].DumpQueryDocCount;
        //            }
        //        }
        //        item.DumpGap = Math.Max(item.ActualDocCount, item.EstimatedDocCount) - downloadCount;
        //        item.DumpPercent = 100;
        //        item.DumpComplete = true;               
        //    }

        //    //// MongoRestore
        //    if (!restoreInvoked && !MigrationCancelled)
        //    {
        //        Log.WriteLine($"{dbName}.{colName} Uploader invoked - skipped Downloader loop");

        //        restoreInvoked = true;
        //        Task.Run(() => Uploader(item, targetConnectionstring));
        //    }           
        //}


        

        //void Uploader(MigrationUnit item,string targetConnectionString)
        //{
        //    string dbName = item.DatabaseName;
        //    string colName = item.CollectionName;
        //    int maxRetries = 10;
        //    string jobId=Job.Id;

        //    TimeSpan backoff = TimeSpan.FromSeconds(2);

        //    string folder = $"{MongoDumpOutputFolder}\\{jobId}\\{dbName}.{colName}";

        //    Log.WriteLine($"{dbName}.{colName} Uploader started");

        //    while (!item.RestoreComplete && System.IO.Directory.Exists(folder) && !MigrationCancelled && Job.CurrentlyActive)
        //    {
        //        int restoredChunks=0;
        //        long restoredDocs = 0;
        //        // MongoRestore
        //        if (!item.RestoreComplete && !MigrationCancelled)
        //        {
        //            for (int i = 0; i < item.MigrationChunks.Count; i++)
        //            {
        //                if (MigrationCancelled) return;

        //                if (!item.MigrationChunks[i].IsUploaded == true && item.MigrationChunks[i].IsDownloaded == true)
        //                {
        //                    string args = $" --uri=\"{targetConnectionString}\" --gzip {folder}\\{i}.bson";

        //                    //if first item drop collection, else append
        //                    if (i == 0)
        //                        args = $"{args} --drop";
        //                    else
        //                        args = $"{args} --noIndexRestore"; //no index for subsequent items.


        //                    double initialPercent = ((double)100 / ((double)item.MigrationChunks.Count)) * i;
        //                    //double contributionfactor = (double)1 / (double)item.MigrationChunks.Count;
                            
        //                    double contributionfactor = (double)item.MigrationChunks[i].DumpQueryDocCount / (double)Math.Max(item.ActualDocCount, item.EstimatedDocCount);
        //                    if (item.MigrationChunks.Count == 1) contributionfactor = 1;

        //                        Log.WriteLine($"{dbName}.{colName}-{i} Uploader processing");

        //                    int restoreAttempts = 0;
        //                    backoff = TimeSpan.FromSeconds(2);
        //                    bool continueProcessing = true;
        //                    while (restoreAttempts < maxRetries && !MigrationCancelled && continueProcessing && !item.RestoreComplete && Job.CurrentlyActive)
        //                    {
        //                        restoreAttempts++;
        //                        try
        //                        {
        //                            if (ProcessExecutor.Execute(Jobs, item, item.MigrationChunks[i], initialPercent, contributionfactor, 0, $"{toolsLaunchFolder}\\mongorestore.exe", args))
        //                            {
        //                                continueProcessing = false;
        //                                item.MigrationChunks[i].IsUploaded = true;
        //                                Jobs?.Save(); //persists state

        //                                if(item.MigrationChunks[i].RestoredFailedDocCount>0)
        //                                {
        //                                    if(targetClient==null)
        //                                        targetClient = new MongoClient(targetConnectionString);

        //                                    var targetDb = targetClient.GetDatabase(item.DatabaseName);
        //                                    var targetCollection = targetDb.GetCollection<BsonDocument>(item.CollectionName);

        //                                    var bounds = SamplePartitioner.GetChunkBounds(item.MigrationChunks[i]);
        //                                    var gte = bounds.gte;
        //                                    var lt = bounds.lt;

        //                                    item.MigrationChunks[i].DocCountInTarget = MongoHelper.GetDocCount(targetCollection, gte, lt, item.MigrationChunks[i].DataType);

        //                                    if (item.MigrationChunks[i].DocCountInTarget == item.MigrationChunks[i].DumpResultDocCount)
        //                                    {
        //                                        Log.WriteLine($"{dbName}.{colName}-{i} No documents missing, count in Target: {item.MigrationChunks[i].DocCountInTarget}");
        //                                        Log.Save();
        //                                    }

        //                                    Jobs?.Save(); //persists state
        //                                }

        //                                restoreAttempts = 0;

        //                                restoredChunks++;
        //                                restoredDocs = restoredDocs + Math.Max(item.MigrationChunks[i].RestoredSucessDocCount, item.MigrationChunks[i].DocCountInTarget);
        //                                try
        //                                {
        //                                    System.IO.Directory.Delete($"{folder}\\{i}.bson", true);
        //                                }
        //                                catch { }
        //                            }
        //                            else
        //                            {
        //                                Log.WriteLine($"Attempt {restoreAttempts} {dbName}.{colName}-{i} of Restore Executor failed");
        //                                //System.Threading.Thread.Sleep(10000);
        //                            }                                    
        //                        }
        //                        catch (MongoExecutionTimeoutException ex)
        //                        {
        //                            Log.WriteLine($" Restore attempt {restoreAttempts} failed due to timeout: {ex.Message}", LogType.Error);

        //                            if (restoreAttempts >= maxRetries)
        //                            {
        //                                Log.WriteLine("Maximum retry attempts reached. Aborting operation.", LogType.Error);
        //                                Log.Save();

        //                                Job.CurrentlyActive = false;
        //                                Jobs?.Save();

        //                                ProcessRunning = false;
        //                            }

        //                            // Wait for the backoff duration before retrying
        //                            Log.WriteLine($"Retrying in {backoff.TotalSeconds} seconds...", LogType.Error);
        //                            Thread.Sleep(backoff);
        //                            Log.Save();

        //                            // Exponentially increase the backoff duration
        //                            backoff = TimeSpan.FromTicks(backoff.Ticks * 2);
        //                        }
        //                        catch (Exception ex)
        //                        {
        //                            Log.WriteLine(ex.ToString(), LogType.Error);
        //                            Log.Save();

        //                            Job.CurrentlyActive = false;
        //                            Jobs?.Save();
        //                            ProcessRunning = false;
        //                        }
        //                    }
        //                    if(restoreAttempts == maxRetries)
        //                    {
        //                        Log.WriteLine("Maximum restore attempts reached. Aborting operations.", LogType.Error);
        //                        Log.Save();

        //                        Job.CurrentlyActive = false;
        //                        Jobs?.Save();
        //                        ProcessRunning = false;
        //                    }
        //                }
        //                else if(item.MigrationChunks[i].IsUploaded == true)
        //                {
        //                    restoredChunks++;
        //                    restoredDocs = restoredDocs + item.MigrationChunks[i].RestoredSucessDocCount;
        //                }
        //            }

        //            if (restoredChunks == item.MigrationChunks.Count && !MigrationCancelled)
        //            {
        //                item.RestoreGap = Math.Max(item.ActualDocCount, item.EstimatedDocCount) - restoredDocs;
        //                item.RestorePercent = 100;
        //                item.RestoreComplete = true;
        //                Jobs?.Save(); //persists state
        //            }
        //            else
        //            {
        //                System.Threading.Thread.Sleep(10000);
        //            }
        //        }
        //    }
        //    if (item.RestoreComplete && item.DumpComplete)
        //    {
        //        try
        //        {
        //            System.IO.Directory.Delete(folder, true);
        //            // Process change streams
        //            if (Online && !MigrationCancelled)
        //            {
        //                if (targetClient == null)
        //                    targetClient = new MongoClient(targetConnectionString);

        //                Log.WriteLine($"{dbName}.{colName} ProcessCollectionChangeStream invoked");                            
        //                Task.Run(() => ProcessCollectionChangeStream(item));
                        
        //            }

        //            if (!Online && !MigrationCancelled)
        //            {

        //                var migrationJob = Jobs.MigrationJobs.Find(m => m.Id == jobId);
        //                if (IsOfflineJobCompleted(migrationJob)) 
        //                {
        //                    Log.WriteLine($"{migrationJob.Id} Terminated");

        //                    migrationJob.IsCompleted = true;
        //                    migrationJob.CurrentlyActive = false;
        //                    ProcessRunning = false;
        //                    Jobs?.Save();
        //                }
        //            }
        //        }
        //        catch
        //        {
        //            //do nothing
        //        }
        //    }
        //}

        //private bool IsOfflineJobCompleted(MigrationJob migrationJob)
        //{
        //    if (migrationJob == null) return true;

        //    foreach (var mu in migrationJob.MigrationUnits)
        //    {
        //        if (!mu.RestoreComplete || !mu.DumpComplete)
        //            return false;
        //    }
        //    return true;
        //}
    

        // void ProcessCollectionChangeStream(MigrationUnit item)
        //{
        //    string databaseName = item.DatabaseName;
        //    string collectionName = item.CollectionName;

        //    var sourceDb = sourceClient.GetDatabase(databaseName);
        //    var sourceCollection = sourceDb.GetCollection<BsonDocument>(collectionName);

        //    var targetDb = targetClient.GetDatabase(databaseName);
        //    var targetCollection = targetDb.GetCollection<BsonDocument>(collectionName);

        //    Log.WriteLine($"Replaying change stream for {databaseName}.{collectionName}");

        //    while (!MigrationCancelled)
        //    {

        //        ChangeStreamOptions options;
        //        if (item.resumeToken != null)
        //        {
        //            options = new ChangeStreamOptions { FullDocument = ChangeStreamFullDocumentOption.UpdateLookup, ResumeAfter = MongoDB.Bson.BsonDocument.Parse(item.resumeToken) };
        //        }
        //        else
        //        {
        //            var bsonTimStamp = ConvertToBsonTimestamp(MigrationJobStartTime);
        //            options = new ChangeStreamOptions { FullDocument = ChangeStreamFullDocumentOption.UpdateLookup, StartAtOperationTime = bsonTimStamp };
        //        }
        //        // Open a Change Stream
        //        using (var cursor = sourceCollection.Watch(options))
        //        {
        //            // Continuously monitor the change stream
        //            foreach (var change in cursor.ToEnumerable())
        //            {
        //                // Access the ClusterTime (timestamp) from the ChangeStreamDocument
        //                var timestamp = change.ClusterTime; // Convert BsonTimestamp to DateTime

        //                // Output change details to the console
        //                Log.AddVerboseMessage($"Change detected at {timestamp}");
        //                ProcessChange(change, targetCollection);

        //                item.resumeToken = cursor.Current.FirstOrDefault().ResumeToken.ToJson();
        //                item.cursorUtcTimestamp =  BsonTimestampToUtcDateTime(timestamp);
        //                Jobs?.Save(); //persists state

        //                if (MigrationCancelled) break;
        //            }   
        //            Log.Save();
        //        }

        //        System.Threading.Thread.Sleep(100);
        //    }
        //}

        
        //void ProcessChange(ChangeStreamDocument<BsonDocument> change, IMongoCollection<BsonDocument> targetCollection)
        //{
        //    switch (change.OperationType)
        //    {
        //        case ChangeStreamOperationType.Insert:
        //            targetCollection.InsertOne(change.FullDocument);
        //            break;
        //        case ChangeStreamOperationType.Update:
        //        case ChangeStreamOperationType.Replace:
        //            var filter = Builders<BsonDocument>.Filter.Eq("_id", change.DocumentKey["_id"]);
        //            targetCollection.ReplaceOne(filter, change.FullDocument, new ReplaceOptions { IsUpsert = true });
        //            break;

        //        case ChangeStreamOperationType.Delete:
        //            var deleteFilter = Builders<BsonDocument>.Filter.Eq("_id", change.DocumentKey["_id"]);
        //            targetCollection.DeleteOne(deleteFilter);
        //            break;

        //        default:
        //            Log.WriteLine($"Unhandled operation type: {change.OperationType}");
        //            break;
        //    }
        //}
    }
}
