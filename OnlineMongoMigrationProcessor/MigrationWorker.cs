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

        

       
    }
}
