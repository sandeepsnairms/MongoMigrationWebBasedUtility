using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Xml.Linq;

namespace OnlineMongoMigrationProcessor
{
    public class MongoDocumentCopier
    {
        private MongoClient _sourceClient;
        private MongoClient _targetClient;
        private IMongoCollection<BsonDocument> _sourceCollection;
        private IMongoCollection<BsonDocument> _targetCollection;
        private const int BatchSize = 500;

        private int successCount = 0;
        private int failureCount = 0;
        private int skippedCount = 0;


        public void Initialize(
            MongoClient sourceClient,
            MongoClient targetClient,
            IMongoCollection<BsonDocument> sourceCollection,
            string targetDatabase,
            string targetCollectionName)
        {
            _sourceClient = sourceClient;
            _targetClient = targetClient;
            _sourceCollection = sourceCollection;
            _targetCollection = _targetClient.GetDatabase(targetDatabase).GetCollection<BsonDocument>(targetCollectionName);
        }

        private void UpdateProgress(
            int setIndex,
            Joblist joblist,
            MigrationUnit item,
            int migrationChunkIndex,
            double basePercent,
            double contribFactor,
            long targetCount,
            int successCount,
            int failureCount)
        {


            MigrationChunk migrationChunk = item.MigrationChunks[migrationChunkIndex];
            var percent = Math.Round((double)(successCount + skippedCount) / targetCount * 100, 3);
            if(percent>100)
            {
                Debug.WriteLine("Percent is greater than 100");
            }

            if (percent > 0)
            {
                Log.AddVerboseMessage($"DocumentCopy Chunk [{migrationChunkIndex}.{setIndex}] Progress: {successCount} documents copied, {skippedCount} documents skipped(duplicate),  {failureCount} documents failed. Chunk percentage: {percent}");
                Log.Save();
                item.DumpPercent = basePercent + (percent * contribFactor);
                item.RestorePercent = item.DumpPercent;
                item.DumpComplete = item.DumpPercent == 100;
                item.RestoreComplete = item.DumpComplete;
            }

            migrationChunk.skippedAsDuplicateCount = skippedCount;
            migrationChunk.DumpResultDocCount = successCount + skippedCount;
            migrationChunk.RestoredSucessDocCount = successCount+skippedCount;
            migrationChunk.RestoredFailedDocCount = failureCount;

            joblist.Save();
        }

        public async Task<bool> CopyDocumentsAsync(
            Joblist joblist,
            MigrationUnit item,
            int migrationChunkIndex,
            double basePercent,
            double contribFactor,
            long targetCount,
            FilterDefinition<BsonDocument> filter,
            CancellationToken cancellationToken)
        {
            ConcurrentBag<Exception> errors=new ConcurrentBag<Exception>();
            try
            {
                Log.WriteLine($"DocumentCopy for Chunk [{migrationChunkIndex}] with {item.MigrationChunks[migrationChunkIndex].Segments.Count} segments started ");
                Log.Save();

                List<Task> tasks = new List<Task>();
                int segmentIndex = 0;
                errors=new ConcurrentBag<Exception>();

                foreach (var segment in item.MigrationChunks[migrationChunkIndex].Segments)
                {
                    if(segment.IsProcessed == true)
                    {
                        segmentIndex++;
                        continue;
                    }
                    FilterDefinition<BsonDocument> combinedFilter = Builders<BsonDocument>.Filter.Empty;
                    if (item.MigrationChunks[migrationChunkIndex].Segments.Count == 1)
                    {
                        combinedFilter = filter;
                    }
                    else
                    {
                        var bounds = SamplePartitioner.GetChunkBounds(segment.Gte, segment.Lt, item.MigrationChunks[migrationChunkIndex].DataType);
                        var gte = bounds.gte;
                        var lt = bounds.lt;

                        //filter by id bounds
                        FilterDefinition<BsonDocument> idFilter = MongoHelper.GenerateQueryFilter(gte, lt, item.MigrationChunks[migrationChunkIndex].DataType);

                        //filter by datatype
                        BsonDocument matchCondition = SamplePartitioner.DataTypeConditionBuilder(item.MigrationChunks[migrationChunkIndex].DataType,"_id");

                        // Combine the filters using $and
                        combinedFilter = Builders<BsonDocument>.Filter.And(idFilter, matchCondition);
                    }

                    //var set = await _sourceCollection.Aggregate()
                    //    .Match(combinedFilter)
                    //    .ToListAsync(cancellationToken);

                    tasks.Add(ProcessSegmentAsync(segment, combinedFilter, segmentIndex, joblist, item, migrationChunkIndex, basePercent, contribFactor, targetCount, errors, cancellationToken));
                    segmentIndex++;
                }         

                await Task.WhenAll(tasks);

            }
            catch (OperationCanceledException)
            {
                Log.WriteLine("DocumentCopy process was canceled.");
                Log.Save();
                return false;
            }

            if (!errors.IsEmpty)
            {
                Log.WriteLine($"DocumentCopy in Chunk [{migrationChunkIndex}] encountered {errors.Count} errors, skipped {skippedCount} during the process.");
                Log.Save();
            }


            if (item.MigrationChunks[migrationChunkIndex].RestoredFailedDocCount > 0)
            {
                var bounds = SamplePartitioner.GetChunkBounds(item.MigrationChunks[migrationChunkIndex].Gte, item.MigrationChunks[migrationChunkIndex].Lt, item.MigrationChunks[migrationChunkIndex].DataType);
                var gte = bounds.gte;
                var lt = bounds.lt;

                item.MigrationChunks[migrationChunkIndex].DocCountInTarget = MongoHelper.GetDocCount(_targetCollection, gte, lt, item.MigrationChunks[migrationChunkIndex].DataType);

                if (item.MigrationChunks[migrationChunkIndex].DocCountInTarget == item.MigrationChunks[migrationChunkIndex].DumpResultDocCount)
                {
                    Log.WriteLine($"DocumentCopy Chunk [{migrationChunkIndex}] no documents missing, count in Target: {item.MigrationChunks[migrationChunkIndex].DocCountInTarget}");
                    Log.Save();
                }

                joblist?.Save(); //persists state
            }
            return true;
        }

        private async Task ProcessSegmentAsync(
            Segment segment,
            
            FilterDefinition<BsonDocument> combinedFilter,
            int segmentIndex,
            Joblist joblist,
            MigrationUnit item,
            int migrationChunkIndex,
            double basePercent,
            double contribFactor,
            long targetCount,
            ConcurrentBag<Exception> errors,
            CancellationToken cancellationToken)
        {

            Log.WriteLine($"DocumentCopy segment started for Chunk [{migrationChunkIndex}.{segmentIndex}]");
            Log.Save();

            if (segment.IsProcessed == true)
            {
                return;
            }   

            segment.QueryDocCount = MongoHelper.GetDocCount(_sourceCollection, combinedFilter);
            joblist.Save();

            MigrationChunk migrationChunk = item.MigrationChunks[migrationChunkIndex];
            try
            {
                //var documentBatches = set
                //    .Select((doc, index) => new { doc, index })
                //    .GroupBy(x => x.index / BatchSize)
                //    .Select(group => group.Select(x => x.doc).ToList())
                //    .ToList();

                //foreach (var batch in documentBatches)
                //{
                //    try
                //    {
                //        if (cancellationToken.IsCancellationRequested)
                //            cancellationToken.ThrowIfCancellationRequested();

                //        await _targetCollection.InsertManyAsync(batch, new InsertManyOptions { IsOrdered = false }, cancellationToken);
                //        Interlocked.Add(ref successCount, batch.Count);
                //    }
                //    catch (MongoException mex) when (mex.Message.Contains("DuplicateKey"))
                //    {
                //        Interlocked.Add(ref skippedCount, ((MongoDB.Driver.MongoBulkWriteException)mex).WriteErrors.Count);
                //    }
                //    catch (MongoBulkWriteException<BsonDocument> ex)
                //    {
                //        int successfulInserts = batch.Count - ex.WriteErrors.Count;
                //        Interlocked.Add(ref successCount, successfulInserts);
                //        Interlocked.Add(ref failureCount, ex.WriteErrors.Count);
                //        LogErrors(ex);
                //    }
                //    catch (Exception ex)
                //    {
                //        errors.Add(ex);
                //        Interlocked.Add(ref failureCount, batch.Count);
                //        Log.WriteLine($"DocumentCopy Chunk [{migrationChunkIndex}.{segmentIndex}] Batch processing error: {ex.Message}");
                //        Log.Save();
                //    }
                //    finally
                //    {
                //        UpdateProgress(segmentIndex,joblist, item, migrationChunkIndex, basePercent, contribFactor, targetCount, successCount, failureCount);
                //    }
                //}
                
                int pageIndex = 0; // Current page index
                List<BsonDocument> set=new List<BsonDocument>();
                while (!cancellationToken.IsCancellationRequested)
                {
                    try
                    {
                        // Get the next page of results
                        set = await _sourceCollection.Aggregate()
                            .Match(combinedFilter)  // Apply the filter
                            .Skip(pageIndex * BatchSize)  // Skip documents based on the current page
                            .Limit(BatchSize)  // Limit to the page size
                            .ToListAsync(cancellationToken);

                       
                        if (set.Count == 0)
                        {
                            // No more results, break the loop
                            break;
                        }

                        // Insert the current batch into the target collection
                        await _targetCollection.InsertManyAsync(set, cancellationToken: cancellationToken);

                        Interlocked.Add(ref successCount, set.Count);

                        // Increment the page index to get the next batch
                        pageIndex++;
                    }
                    catch (MongoException mex) when (mex.Message.Contains("DuplicateKey"))
                    {
                        Interlocked.Add(ref skippedCount, ((MongoDB.Driver.MongoBulkWriteException)mex).WriteErrors.Count);
                    }
                    catch (MongoBulkWriteException<BsonDocument> ex)
                    {
                        int successfulInserts = set.Count - ex.WriteErrors.Count;
                        Interlocked.Add(ref successCount, successfulInserts);
                        Interlocked.Add(ref failureCount, ex.WriteErrors.Count);
                        LogErrors(ex);
                    }
                    catch(Exception ex) when(ex.Message.Contains("canceled."))
                    {
                        Log.WriteLine($"DocumentCopy Chunk [{migrationChunkIndex}.{segmentIndex}] Operation Canceled");
                        Log.Save();
                    }
                    catch (Exception ex)
                    {
                        errors.Add(ex);
                        Interlocked.Add(ref failureCount, set.Count);
                        Log.WriteLine($"DocumentCopy Chunk [{migrationChunkIndex}.{segmentIndex}] Batch processing error: {ex.Message}", LogType.Error);
                        Log.Save();
                    }
                    finally
                    {
                        UpdateProgress(segmentIndex, joblist, item, migrationChunkIndex, basePercent, contribFactor, targetCount, successCount, failureCount);
                    }
                }

                Log.WriteLine($"DocumentCopy segment completed for Chunk [{migrationChunkIndex}.{segmentIndex}]");
                Log.Save();

                segment.IsProcessed = true;
                joblist.Save();
            }
            catch (Exception ex)
            {
                errors.Add(ex);
                Log.WriteLine($"DocumentCopy Error processing Chunk [{migrationChunkIndex}.{segmentIndex}],  Details: {ex.Message}", LogType.Error);
                Log.Save();
            }
        }

        private void LogErrors(MongoBulkWriteException<BsonDocument> ex)
        {
            foreach (var error in ex.WriteErrors)
            {
                Log.WriteLine($"DocumentCopy encountered WriteErrors, Details: {ex.Message}");
                Log.Save();
            }
        }


    }
}
