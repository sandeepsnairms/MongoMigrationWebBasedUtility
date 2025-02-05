﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Driver;

#pragma warning disable CS8618 // Non-nullable field must contain a non-null value when exiting constructor. Consider adding the 'required' modifier or declaring as nullable.

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
        private string _toolsDestinationFolder = $"{Helper.GetWorkingFolder()}mongo-tools";
        private string _toolsLaunchFolder = string.Empty;
        private bool _migrationCancelled = false;
        private JobList? _jobs;
        private MigrationJob? _job;
        private MongoClient? _sourceClient;
        private IMigrationProcessor _migrationProcessor;

        public MigrationSettings? Config { get; set; }
        public string? CurrentJobId { get; set; }

        public MigrationWorker(JobList jobs)
        {
            _jobs = jobs;
        }

        public bool IsProcessRunning()
        {
            if (Config == null)
            {
                Config = new MigrationSettings();
                Config.Load();
            }

            return _migrationProcessor?.ProcessRunning ?? false;
        }

        public void StopMigration()
        {
            _migrationCancelled = true;
            _migrationProcessor?.StopProcessing();
            _migrationProcessor.ProcessRunning = false;
            _migrationProcessor = null;
        }

        public async Task StartMigrationAsync(MigrationJob job, string sourceConnectionString, string targetConnectionString, string namespacesToMigrate, bool doBulkCopy, bool trackChangeStreams)
        {
            int maxRetries = 10;
            int attempts = 0;

            TimeSpan backoff = TimeSpan.FromSeconds(2);

            //encoding speacial characters
            sourceConnectionString = Helper.EncodeMongoPasswordInConnectionString(sourceConnectionString);
            targetConnectionString = Helper.EncodeMongoPasswordInConnectionString(targetConnectionString);

            if (Config == null)
            {
                Config = new MigrationSettings();
                Config.Load();
            }

            try
            {
                _migrationProcessor?.StopProcessing();
                _migrationProcessor = null;
            }
            catch { }

            _job = job;
            _migrationCancelled = false;
            CurrentJobId = _job.Id;

            Log.Init(_job.Id);
            Log.WriteLine($"{_job.Id} Started on {_job.StartedOn} (UTC)");
            Log.Save();

            string[] collectionsInput = namespacesToMigrate
                .Split(',')
                .Select(item => item.Trim())
                .ToArray();



            if (_job.UseMongoDump)
            {
                _toolsLaunchFolder = await Helper.EnsureMongoToolsAvailableAsync(_toolsDestinationFolder, Config);
                if (string.IsNullOrEmpty(_toolsLaunchFolder))
                {
                    _job.CurrentlyActive = false;
                    _job.IsCompleted = false;
                    _jobs?.Save();

                    _migrationProcessor?.StopProcessing();
                    return;
                }
            }

            bool continueProcessing = true;

            if (_job.MigrationUnits == null)
            {
                _job.MigrationUnits = new List<MigrationUnit>();
            }

            if (_job.MigrationUnits.Count == 0)
            {
                foreach (var fullName in collectionsInput)
                {
                    if (_migrationCancelled) return;

                    string[] parts = fullName.Split('.');
                    if (parts.Length != 2) continue;

                    string dbName = parts[0].Trim();
                    string colName = parts[1].Trim();

                    var migrationUnit = new MigrationUnit(dbName, colName, null);
                    _job.MigrationUnits.Add(migrationUnit);
                    _jobs?.Save();
                }
            }




            while (attempts < maxRetries && !_migrationCancelled && continueProcessing)
            {
                attempts++;
                try
                {
                    _sourceClient = new MongoClient(sourceConnectionString);
                    Log.WriteLine("Source Client Created");
                    Log.Save();


                    if (_job.IsOnline)
                    {
                        Log.WriteLine("Checking if Change Stream is enabled on source");
                        Log.Save();


                        var retValue = await MongoHelper.IsChangeStreamEnabledAsync(_job.SourceConnectionString);
                        _job.SourceServerVersion = retValue.Version;
                        _jobs?.Save();

                        if (!retValue.IsCSEnabled)
                        {
                            _job.CurrentlyActive = false;
                            _job.IsCompleted = true;
                            _jobs?.Save();
                            continueProcessing = false;

                            _migrationProcessor?.StopProcessing();
                            return;
                        }
                    }

                    _migrationProcessor?.StopProcessing();
                    _migrationProcessor = null;
                    if (!_job.UseMongoDump)
                    {
                        _migrationProcessor = new CopyProcessor(_jobs, _job, _sourceClient, Config);
                    }
                    else
                    {
                        _migrationProcessor = new DumpRestoreProcessor(_jobs, _job, _sourceClient, Config, _toolsLaunchFolder);
                    }
                    _migrationProcessor.ProcessRunning = true;

                    foreach (var unit in _job.MigrationUnits)
                    {
                        if (_migrationCancelled) return;
                        
                        if (await MongoHelper.CheckCollectionExists(_sourceClient, unit.DatabaseName, unit.CollectionName))
                        {
                            unit.SourceStatus = CollectionStatus.OK;

                            if (_job.IsOnline)
                            {
                                Task.Run(async () =>
                                {
                                    await MongoHelper.SetChangeStreamResumeTokenAsync(_sourceClient, unit);
                                });
                            }
                            if (unit.MigrationChunks == null || unit.MigrationChunks.Count == 0)
                            {

                                var chunks = await PartitionCollection(unit.DatabaseName, unit.CollectionName);

                                Log.WriteLine($"{unit.DatabaseName}.{unit.CollectionName} has {chunks.Count} Chunks");
                                Log.Save();

                                unit.MigrationChunks = chunks;
                                unit.ChangeStreamStartedOn = DateTime.Now;



                                if (!_job.UseMongoDump)
                                {
                                    var database = _sourceClient.GetDatabase(unit.DatabaseName);
                                    var collection = database.GetCollection<BsonDocument>(unit.CollectionName);
                                    await MongoHelper.DeleteAndCopyIndexesAsync(targetConnectionString, collection);
                                }
                            }
                           
                        }
                        else
                        {
                            unit.SourceStatus = CollectionStatus.NotFound;
                            Log.WriteLine($"{unit.DatabaseName}.{unit.CollectionName} does not exist on source", LogType.Error);
                            Log.Save();
                        }
                    }

                    _jobs?.Save();
                    Log.Save();

                    foreach (var migrationUnit in _job.MigrationUnits)
                    {
                        if (_migrationCancelled) break;

                        if (migrationUnit.SourceStatus == CollectionStatus.OK)
                        {
                            if (await MongoHelper.CheckCollectionExists(_sourceClient, migrationUnit.DatabaseName, migrationUnit.CollectionName))
                            {
                                _migrationProcessor.Migrate(migrationUnit, sourceConnectionString, targetConnectionString);
                            }
                            else
                            {
                                migrationUnit.SourceStatus = CollectionStatus.NotFound;
                                Log.WriteLine($"{migrationUnit.DatabaseName}.{migrationUnit.CollectionName} does not exist on source", LogType.Error);
                                Log.Save();
                            }
                        }
                    }

                    continueProcessing = false;
                }
                catch (MongoExecutionTimeoutException ex)
                {
                    Log.WriteLine($"Attempt {attempts} failed due to timeout: {ex.Message}", LogType.Error);

                    if (attempts >= maxRetries)
                    {
                        Log.WriteLine("Maximum retry attempts reached. Aborting operation.", LogType.Error);
                        Log.Save();

                        _job.CurrentlyActive = false;
                        _jobs?.Save();

                        _migrationProcessor?.StopProcessing();
                    }

                    Log.WriteLine($"Retrying in {backoff.TotalSeconds} seconds...", LogType.Error);
                    Thread.Sleep(backoff);
                    Log.Save();

                    continueProcessing = true;
                    backoff = TimeSpan.FromTicks(backoff.Ticks * 2);
                }
                catch (Exception ex)
                {
                    Log.WriteLine(ex.ToString(), LogType.Error);
                    Log.Save();

                    _job.CurrentlyActive = false;
                    _jobs?.Save();
                    continueProcessing = false;

                    _migrationProcessor?.StopProcessing();
                }
            }
        }

        private async Task<List<MigrationChunk>> PartitionCollection(string databaseName, string collectionName, string idField = "_id")
        {
            var database = _sourceClient.GetDatabase(databaseName);
            var collection = database.GetCollection<BsonDocument>(collectionName);


            var statsCommand = new BsonDocument { { "collStats", collectionName } };
            var stats = await database.RunCommandAsync<BsonDocument>(statsCommand);
            long totalCollectionSizeBytes = stats.Contains("storageSize") ? stats["storageSize"].ToInt64() : stats["size"].ToInt64();


            long documentCount;
            if (stats["count"].IsInt32)
            {
                documentCount = stats["count"].ToInt32();
            }
            else if (stats["count"].IsInt64)
            {
                documentCount = stats["count"].ToInt64();
            }
            else
            {
                throw new InvalidOperationException("Unexpected data type for document count.");
            }

            int totalChunks = 0;
            long minDocsInChunk = 0;

            long targetChunkSizeBytes = Config.ChunkSizeInMb * 1024 * 1024;
            var totalChunksBySize = (int)Math.Ceiling((double)totalCollectionSizeBytes / targetChunkSizeBytes);


            if (_job.UseMongoDump)
            {
                totalChunks = totalChunksBySize;
                minDocsInChunk = documentCount / totalChunks;
                Log.WriteLine($"{databaseName}.{collectionName} Storage Size: {totalCollectionSizeBytes}");
            }
            else
            {
                Log.WriteLine($"{databaseName}.{collectionName} Estimated Document Count: {documentCount}");
                totalChunks = (int)Math.Min(SamplePartitioner.MaxSamples / SamplePartitioner.MaxSegments, documentCount / SamplePartitioner.MaxSamples);
                totalChunks = Math.Max(1, totalChunks); // At least one chunk
                totalChunks = Math.Max(totalChunks, totalChunksBySize);
                minDocsInChunk = documentCount / totalChunks;
            }

            List<MigrationChunk> migrationChunks = new List<MigrationChunk>();

            if (totalChunks > 1 || !_job.UseMongoDump)
            {
                Log.WriteLine($"Chunking {databaseName}.{collectionName}");
                Log.Save();


                List<DataType> dataTypes = new List<DataType> { DataType.Int, DataType.Int64, DataType.String, DataType.Object, DataType.Decimal128, DataType.Date, DataType.ObjectId };

                if (Config.HasUuid)
                    dataTypes.Add(DataType.UUID);

                foreach (var dataType in dataTypes)
                {
                    long docCountByType;
                    ChunkBoundaries chunkBoundaries = SamplePartitioner.CreatePartitions(_job.UseMongoDump, collection, idField, totalChunks, dataType, minDocsInChunk, out docCountByType);

                    if (docCountByType == 0)
                    {
                        continue;
                    }

                    if (chunkBoundaries == null)
                    {
                        if (_job.UseMongoDump)
                        {
                            continue;
                        }
                        else
                        {
                            var min = BsonNull.Value;
                            var max = BsonNull.Value;

                            var chunkBoundary = new Boundary
                            {
                                StartId = min,
                                EndId = max,
                                SegmentBoundaries = new List<Boundary>()
                            };

                            chunkBoundaries.Boundaries ??= new List<Boundary>();
                            chunkBoundaries.Boundaries.Add(chunkBoundary);
                            var segmentBoundary = new Boundary
                            {
                                StartId = min,
                                EndId = max
                            };
                            chunkBoundary.SegmentBoundaries.Add(segmentBoundary);
                        }
                    }

                    for (int i = 0; i < chunkBoundaries.Boundaries.Count; i++)
                    {
                        var (startId, endId) = GetStartEnd(true, chunkBoundaries.Boundaries[i], chunkBoundaries.Boundaries.Count, i);
                        var chunk = new MigrationChunk(startId, endId, dataType, false, false);
                        migrationChunks.Add(chunk);

                        if (!_job.UseMongoDump && (chunkBoundaries.Boundaries[i].SegmentBoundaries == null || chunkBoundaries.Boundaries[i].SegmentBoundaries.Count == 0))
                        {
                            chunk.Segments ??= new List<Segment>();
                            chunk.Segments.Add(new Segment { Gte = startId, Lt = endId, IsProcessed = false });
                        }

                        if (!_job.UseMongoDump && chunkBoundaries.Boundaries[i].SegmentBoundaries.Count > 0)
                        {
                            for (int j = 0; j < chunkBoundaries.Boundaries[i].SegmentBoundaries.Count; j++)
                            {
                                var segment = chunkBoundaries.Boundaries[i].SegmentBoundaries[j];
                                var (segmentStartId, segmentEndId) = GetStartEnd(false, segment, chunkBoundaries.Boundaries[i].SegmentBoundaries.Count, j, chunk.Lt, chunk.Gte);

                                chunk.Segments ??= new List<Segment>();
                                chunk.Segments.Add(new Segment { Gte = segmentStartId, Lt = segmentEndId, IsProcessed = false });
                            }
                        }
                    }
                }
            }
            else
            {
                var chunk = new MigrationChunk(string.Empty, string.Empty, DataType.String, false, false);
                migrationChunks.Add(chunk);
            }

            return migrationChunks;
        }

        private Tuple<string, string> GetStartEnd(bool isChunk, Boundary boundary, int totalBoundaries, int currentIndex, string chunkLt = "", string chunkGte = "")
        {
            string startId;
            string endId;

            if (currentIndex == 0)
            {
                startId = isChunk ? "" : chunkGte;
                endId = boundary.EndId?.ToString() ?? "";
            }
            else if (currentIndex == totalBoundaries - 1)
            {
                startId = boundary.StartId?.ToString() ?? "";
                endId = isChunk ? "" : chunkLt;
            }
            else
            {
                startId = boundary.StartId?.ToString() ?? "";
                endId = boundary.EndId?.ToString() ?? "";
            }

            return Tuple.Create(startId, endId);
        }
    }
}
