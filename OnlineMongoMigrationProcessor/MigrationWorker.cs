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

        bool MigrationCancelled = false;

        private Joblist? Jobs;

        public MigrationSettings? Config;

        private MongoClient? sourceClient;


        MigrationJob? Job;

        IMigrationProcessor migrationProcessor;
        //DumpRestoreProcessor? DRProcessor;
        //CopyProcessor? CProcessor;

        public string? CurrentJobId { get; set; }

        public bool IsProcessRunning()
        {
            if (Config == null)
            {
                Config = new MigrationSettings();
                Config.Load();
            }


            if (migrationProcessor == null)
                return false;

            return migrationProcessor.ProcessRunning;

        }

        public MigrationWorker(Joblist jobs)
        {
            this.Jobs= jobs;            
        }

        public void StopMigration()
        {
            MigrationCancelled = true;
 
            migrationProcessor.StopProcessing();
            migrationProcessor.ProcessRunning = false;

            migrationProcessor = null;

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

                if (migrationProcessor != null)
                {
                    migrationProcessor.StopProcessing();
                    migrationProcessor = null;
                }               
            }
            catch { }   

            

            Job = _job;


            MigrationCancelled = false;
            CurrentJobId = Job.Id;

            Log.init(Job.Id);

            Log.WriteLine($"{Job.Id} Started on  {Job.StartedOn.ToString()} (UTC)");
            Log.Save();

            string[] collectionsInput = namespacesToMigrate
                .Split(',')
                .Select(item => item.Trim())
                .ToArray();

            // checking if change stream is enabled on source for online job.
            if (Job.IsOnline)
            {
                Log.WriteLine("Checking if Change Stream is enabled on source");
                Log.Save();

                var retValue = await MongoHelper.IsChangeStreamEnabledAsync(Job.SourceConnectionString);
                if (!retValue)
                {
                    Job.CurrentlyActive = false;
                    Job.IsCompleted = true;
                    Jobs?.Save();

                    if (migrationProcessor != null)
                    {
                        migrationProcessor.ProcessRunning = false;
                    }
                    return;
                }
            }

            if (Job.UseMongoDump)
            {
                // Ensure MongoDB tools are available        
                toolsLaunchFolder = await Helper.EnsureMongoToolsAvailableAsync(toolsDestinationFolder, Config);
            }



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

                    if(!Job.UseMongoDump)
                    {
                        if (migrationProcessor != null)
                        {
                            migrationProcessor.ProcessRunning = false;
                            migrationProcessor.StopProcessing();
                            migrationProcessor = null;
                        }

                        migrationProcessor = new CopyProcessor(Jobs, Job, sourceClient, Config);

                        migrationProcessor.ProcessRunning = true;
                    }
                    else
                    {
                        if (migrationProcessor == null)
                            migrationProcessor = new DumpRestoreProcessor(Jobs, Job, sourceClient, Config, toolsLaunchFolder);

                        migrationProcessor.ProcessRunning = true;
                    }
                     

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
                            unit.ChangeStreamStartedOn= System.DateTime.Now;

                            //mongo restore copies indexes and data, so no need to copy indexes manually.
                            if (!Job.UseMongoDump)
                            {
                                var database = sourceClient.GetDatabase(unit.DatabaseName);
                                var collection = database.GetCollection<BsonDocument>(unit.CollectionName);
                                var retValue = await MongoHelper.DeleteAndCopyIndexesAsync(targetConnectionString, collection);
                            }
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
                            
                            migrationProcessor.Download(migrationUnit, sourceConnectionString, targetConnectionString);

                        }
                    }
                    //else
                    //    Log.WriteLine("Skipping Bulk Copy");

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

                        if (migrationProcessor != null)
                        {
                            migrationProcessor.ProcessRunning = false;
                        }                        
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

                    if (migrationProcessor != null)
                    {
                        migrationProcessor.ProcessRunning = false;
                    }                   
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

            //if using MongoDump/restore, create single epmty chunk, else we may need to segment the chunk also.
            if (totalChunks > 1 || !Job.UseMongoDump)
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
                    long docCountByType;
                    ChunkBoundaries chunkBoundaries = partitioner.CreatePartitions(idField, totalChunks, dataType, documentCount / totalChunks, out docCountByType);

                    if (docCountByType == 0)
                    {
                        continue;
                    }

                    if (chunkBoundaries == null)
                    {
                        if (Job.UseMongoDump)
                        {
                            continue;
                        }
                        else
                        {
                            //single chunk and segment in case of data set being small.

                            var min = BsonNull.Value;
                            var max = BsonNull.Value;

                            var chunkBoundary = new Boundary
                            {
                                StartId = min,
                                EndId = max,
                                SegmentBoundaries = new List<Boundary>() // Initialize SegmentBoundaries here
                            };

                            chunkBoundaries.Boundaries ??= new List<Boundary>(); // Use null-coalescing assignment
                            chunkBoundaries.Boundaries.Add(chunkBoundary);
                            var segmentBoundary = new Boundary
                            {
                                StartId = min,
                                EndId = max
                            };
                            chunkBoundary.SegmentBoundaries.Add(segmentBoundary);
                        }

                    }

                    MigrationChunk chunk = null;
                    // Add the partitions to migrationChunks
                    for (int i = 0; i < chunkBoundaries.Boundaries.Count; i++)
                    {
                        var (startId, endId) = GetStartEnd(true,chunkBoundaries.Boundaries[i], chunkBoundaries.Boundaries.Count, i);
                        chunk = new MigrationChunk(startId, endId, dataType, false, false);
                        migrationChunks.Add(chunk);


                        if (!Job.UseMongoDump && (chunkBoundaries.Boundaries[i].SegmentBoundaries == null || chunkBoundaries.Boundaries[i].SegmentBoundaries.Count==0))
                        {
                            //ensure single segment in  each chunk
                            if(chunk.Segments == null)
                                chunk.Segments = new List<Segment>();

                            //use parent chunk boundaries
                            chunk.Segments.Add(new Segment { Gte = startId, Lt = endId, IsProcessed = false });
                        }
                        // Add the segments   
                        if (!Job.UseMongoDump && chunkBoundaries.Boundaries[i].SegmentBoundaries.Count>0)
                        { 
                            for(int j = 0; j < chunkBoundaries.Boundaries[i].SegmentBoundaries.Count; j++)
                            {
                                var segment = chunkBoundaries.Boundaries[i].SegmentBoundaries[j];
                                var (segemntStartId, segmentEndId) = GetStartEnd(false,segment, chunkBoundaries.Boundaries[i].SegmentBoundaries.Count, j,chunk.Lt, chunk.Gte);

                                if (chunk.Segments == null)
                                    chunk.Segments = new List<Segment>();

                                chunk.Segments.Add(new Segment { Gte = segemntStartId, Lt = segmentEndId, IsProcessed = false });
                            }                        
                            
                        }
                    }
                }

            }
            else
            {
                //single chunk in case of data set being small.
                var chunk = new MigrationChunk(string.Empty, string.Empty, DataType.String, false, false);
                migrationChunks.Add(chunk);                
            }

            return migrationChunks;
        }

        //need chunkLt only IsChunk=false 
        private Tuple<string, string> GetStartEnd(bool IsChunk, Boundary boundary, int totalBoundaries, int currentIndex, string chunkLt="",string chunkGte="")
        {
            string startId;
            string endId;

            if (currentIndex == 0) // First partition, no gte if its a chunk, else inherit Gte from parent chunk
            {
                if (!IsChunk)
                    startId = chunkGte;
                else
                    startId = "";
                endId = boundary.EndId?.ToString() ?? "";
            }
            else if (currentIndex == totalBoundaries - 1) // Last partition, no lte if its a chunk, else inherit lt from parent chunk
            {
                startId = boundary.StartId?.ToString() ?? "";
                if(!IsChunk)
                    endId = chunkLt;
                else
                    endId = "";
                
            }
            else // Middle partitions
            {
                startId = boundary.StartId?.ToString() ?? "";
                endId = boundary.EndId?.ToString() ?? "";
            }

            return Tuple.Create(startId, endId);
        }




    }
}
