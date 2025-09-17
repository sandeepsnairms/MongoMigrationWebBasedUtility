using MongoDB.Bson;
using MongoDB.Driver;
using OnlineMongoMigrationProcessor.Models;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Reflection.Metadata;
using System.Threading;
using System.Threading.Tasks;
using System.Linq;

#pragma warning disable CS8618 // Non-nullable field must contain a non-null value when exiting constructor. Consider adding the 'required' modifier or declaring as nullable.
namespace OnlineMongoMigrationProcessor.Workers
{
    public class DocumentCopyWorker
    {
        private Log _log;
        private MongoClient _targetClient;
        private IMongoCollection<BsonDocument> _sourceCollection;
        private IMongoCollection<BsonDocument> _targetCollection;
        private int _pageSize = 500;
        private long _successCount = 0;
        private long _failureCount = 0;
        private long _skippedCount = 0;
              

        private void UpdateProgress(
            string segmentId,
            JobList jobList,
            MigrationUnit mu,
            int migrationChunkIndex,
            double basePercent,
            double contribFactor,
            long targetCount,
            long successCount,
            long failureCount,
            long skippedCount)
        {
            MigrationChunk migrationChunk = mu.MigrationChunks[migrationChunkIndex];
            var percent = Math.Round((double)(successCount + skippedCount) / targetCount * 100, 3);
            if (percent > 100)
            {
                Debug.WriteLine("Percent is greater than 100");
            }

            if (percent > 0)
            {
               _log.AddVerboseMessage($"Document copy for segment [{migrationChunkIndex}.{segmentId}] Progress: {successCount} documents copied, {skippedCount} documents skipped(duplicate), {failureCount} documents failed. Chunk completion percentage: {percent}");

                mu.DumpPercent = basePercent + percent * contribFactor;
                mu.RestorePercent = mu.DumpPercent;
                mu.DumpComplete = mu.DumpPercent == 100;
                mu.RestoreComplete = mu.DumpComplete;

                if(mu.DumpComplete && mu.RestoreComplete)
                {
                    mu.BulkCopyEndedOn = DateTime.UtcNow;
                }
            }

            migrationChunk.SkippedAsDuplicateCount = skippedCount;
            migrationChunk.DumpResultDocCount = successCount + skippedCount;
            migrationChunk.RestoredSuccessDocCount = successCount + skippedCount;
            migrationChunk.RestoredFailedDocCount = failureCount;

            jobList.Save();
        }

        public void Initialize(
          Log log,
          MongoClient targetClient,
          IMongoCollection<BsonDocument> sourceCollection,
          string targetDatabase,
          string targetCollectionName,
          int pageSize)
        {
            _log = log;
            _targetClient = targetClient;
            _sourceCollection = sourceCollection;
            if (_targetClient != null)
                _targetCollection = _targetClient.GetDatabase(targetDatabase).GetCollection<BsonDocument>(targetCollectionName);
            _pageSize=pageSize;
        }

        public async Task<TaskResult> CopyDocumentsAsync(
            JobList jobList,
            MigrationUnit mu,
            int migrationChunkIndex,
            double basePercent,
            double contribFactor,
            long targetCount,
            FilterDefinition<BsonDocument> filter,
            CancellationToken cancellationToken,
            bool isWriteSimulated)
        {
            ConcurrentBag<Exception> errors = new ConcurrentBag<Exception>();
            try
            {
               _log.WriteLine($"Document copy for {mu.DatabaseName}.{mu.CollectionName}[{migrationChunkIndex}] with {mu.MigrationChunks[migrationChunkIndex].Segments.Count} segments started");
                
                if(!mu.BulkCopyStartedOn.HasValue || mu.BulkCopyStartedOn == DateTime.MinValue)
                    mu.BulkCopyStartedOn = DateTime.UtcNow;

				List<Task<TaskResult>> tasks = new List<Task<TaskResult>>();
				int segmentIndex = 0;
                errors = new ConcurrentBag<Exception>();

                SemaphoreSlim semaphore = new SemaphoreSlim(20);

                foreach (var segment in mu.MigrationChunks[migrationChunkIndex].Segments)
                {

                    //back ward compatibility, Id was introduced later
                    segmentIndex++;
                    if (string.IsNullOrEmpty(segment.Id))
                        segment.Id = segmentIndex.ToString();

                    FilterDefinition<BsonDocument> combinedFilter = Builders<BsonDocument>.Filter.Empty;
                    if (mu.MigrationChunks[migrationChunkIndex].Segments.Count == 1 || segment.IsProcessed == true)
                    {
                        combinedFilter = filter;
                    }
                    else
                    {
#pragma warning disable CS8604 // Possible null reference argument.
                        var bounds = SamplePartitioner.GetChunkBounds(segment.Gte, segment.Lt, mu.MigrationChunks[migrationChunkIndex].DataType);
#pragma warning restore CS8604 // Possible null reference argument.
                        var gte = bounds.gte;
                        var lt = bounds.lt;

                        // Filter by id bounds
                        FilterDefinition<BsonDocument> idFilter = MongoHelper.GenerateQueryFilter(gte, lt, mu.MigrationChunks[migrationChunkIndex].DataType, MongoHelper.ConvertUserFilterToBSONDocument(mu.UserFilter!), mu.DataTypeFor_Id.HasValue);

                        // Filter by datatype (skip if DataTypeFor_Id is specified)
                        BsonDocument? userFilter = BsonDocument.Parse(mu.UserFilter ?? "{}");
                        BsonDocument matchCondition = SamplePartitioner.BuildDataTypeCondition(mu.MigrationChunks[migrationChunkIndex].DataType, userFilter, mu.DataTypeFor_Id.HasValue);

                        // Combine the filters using $and
                        combinedFilter = Builders<BsonDocument>.Filter.And(idFilter, matchCondition);
                    }

                    await semaphore.WaitAsync();
					tasks.Add(Task.Run(async () =>
					{
						try
						{
							// Directly return the awaited enum value
							return await ProcessSegmentAsync(
								segment, combinedFilter, jobList, mu,
								migrationChunkIndex, basePercent, contribFactor,
								targetCount, errors, cancellationToken, isWriteSimulated
							);
						}
						finally
						{
							semaphore.Release();
						}
					}));

				}

				TaskResult[] results=await Task.WhenAll(tasks);

				if (results.Any(r => r == TaskResult.Canceled))
                {
					return TaskResult.Canceled;
				}

                // Check for segments with missing documents after all segments are processed
                //typically caused because of online changes, we saw in some cases some documents are missed.
                await CheckForMissingDocumentsAsync(jobList, mu, migrationChunkIndex, filter, cancellationToken, isWriteSimulated);
            }
            catch (OperationCanceledException)
            {
				return TaskResult.Canceled;
            }

            if (!errors.IsEmpty)
            {
               _log.WriteLine($"Document copy for {mu.DatabaseName}.{mu.CollectionName}[{migrationChunkIndex}] encountered {errors.Count} errors, skipped {_skippedCount} during the process");                
            }

            if (mu.MigrationChunks[migrationChunkIndex].RestoredFailedDocCount > 0 || errors.Count>0)
            {
#pragma warning disable CS8604 // Possible null reference argument.
                var bounds = SamplePartitioner.GetChunkBounds(mu.MigrationChunks[migrationChunkIndex].Gte, mu.MigrationChunks[migrationChunkIndex].Lt, mu.MigrationChunks[migrationChunkIndex].DataType);
                var gte = bounds.gte;
                var lt = bounds.lt;

               _log.WriteLine($"Counting documents on target {mu.DatabaseName}.{mu.CollectionName}[{ migrationChunkIndex}].");
                

                try
                {
                    mu.MigrationChunks[migrationChunkIndex].DocCountInTarget = MongoHelper.GetDocumentCount(_targetCollection, gte, lt, mu.MigrationChunks[migrationChunkIndex].DataType, MongoHelper.ConvertUserFilterToBSONDocument(mu.UserFilter!), mu.DataTypeFor_Id.HasValue);
                    mu.MigrationChunks[migrationChunkIndex].DumpQueryDocCount = MongoHelper.GetDocumentCount(_sourceCollection, gte, lt, mu.MigrationChunks[migrationChunkIndex].DataType, MongoHelper.ConvertUserFilterToBSONDocument(mu.UserFilter!), mu.DataTypeFor_Id.HasValue);
                }
                catch (Exception ex)
                {
                   _log.WriteLine($"Encountered error while counting documents on target for {mu.DatabaseName}.{mu.CollectionName}[{migrationChunkIndex}]. Chunk will be reprocessed. Details: {ex}", LogType.Error);
                    
                    ResetSegmentsInChunk(mu.MigrationChunks[migrationChunkIndex]);
                    return TaskResult.Retry;
                }

                if (mu.MigrationChunks[migrationChunkIndex].DocCountInTarget == mu.MigrationChunks[migrationChunkIndex].DumpQueryDocCount)
                {
                   _log.WriteLine($"No documents missing in {mu.DatabaseName}.{mu.CollectionName}[{migrationChunkIndex}]. Target count: {mu.MigrationChunks[migrationChunkIndex].DocCountInTarget}");
                }
                else
                {
                   _log.WriteLine($"Count mismatch in {mu.DatabaseName}.{mu.CollectionName}[{migrationChunkIndex}]. Chunk will be reprocessed.", LogType.Error);
					return TaskResult.Retry;
				}
                jobList?.Save(); //persists state
            }
            return TaskResult.Success;
        }

        private void ResetSegmentsInChunk(MigrationChunk migrationChunk)
        {
            foreach (var segment in migrationChunk.Segments)
            {
                segment.IsProcessed = false;
            }
        }

        private async Task<TaskResult> ProcessSegmentAsync(
            Segment segment,
            FilterDefinition<BsonDocument> combinedFilter,
            JobList jobList,
            MigrationUnit mu,
            int migrationChunkIndex,
            double basePercent,
            double contribFactor,
            long targetCount,
            ConcurrentBag<Exception> errors,
            CancellationToken cancellationToken,
            bool IsWriteSimulated)
        {
              
            string segmentId = segment.Id;
            TimeSpan backoff = TimeSpan.FromSeconds(2);

           _log.WriteLine($"Document copy started for segment {mu.DatabaseName}.{mu.CollectionName}[{migrationChunkIndex}.{segmentId}]");
            

            if (segment.IsProcessed == true)
            {
               _log.WriteLine($"Skipping processed segment {mu.DatabaseName}.{mu.CollectionName}[{migrationChunkIndex}.{segmentId}]");
                

                Interlocked.Add(ref _successCount, segment.QueryDocCount);
                return TaskResult.Success;
            }
           
            segment.QueryDocCount = MongoHelper.GetDocumentCount(_sourceCollection, combinedFilter,new BsonDocument());
            jobList.Save();


            try
            {
                bool failed = false;
                int pageIndex = 0; // Current page index
                List<BsonDocument> set = new List<BsonDocument>();
                while (!cancellationToken.IsCancellationRequested)
                {
                    try
                    {
                        // 1. Fetch documents from source
                        set = await _sourceCollection.Aggregate()
                            .Match(combinedFilter)
                            .Skip(pageIndex * _pageSize)
                            .Limit(_pageSize)
                            .ToListAsync(cancellationToken);

                        segment.ResultDocCount+= set.Count;

                        if (set.Count == 0)
                            break;

                        // 2. Write to target (unless simulated)
                        if (!IsWriteSimulated)
                        {
                            var insertModels = set.Select(doc =>
                            {
                                // _id handling only if really needed
                                if (doc.Contains("_id"))
                                {
                                    var id = doc["_id"];
                                    if (id.IsObjectId)
                                        doc["_id"] = id.AsObjectId;
                                }
                                return new InsertOneModel<BsonDocument>(doc);
                            }).ToList();

                            var result = await _targetCollection.BulkWriteAsync(
                                insertModels,
                                new BulkWriteOptions { IsOrdered = false },
                                cancellationToken);

                            long insertCount = result.InsertedCount;
                            Interlocked.Add(ref _successCount, insertCount);
                        }
                        else
                        {
                            Interlocked.Add(ref _successCount, set.Count);
                        }
                    }
                    catch (MongoBulkWriteException<BsonDocument> ex)
                    {
                        // Partial success
                        long bulkInserted = ex.Result?.InsertedCount ?? 0;
                        Interlocked.Add(ref _successCount, bulkInserted);

                        // Analyze errors
                        int dupeCount = ex.WriteErrors.Count(e => e.Code == 11000);
                        if (dupeCount > 0)
                            Interlocked.Add(ref _skippedCount, dupeCount);

                        int otherErrors = ex.WriteErrors.Count(e => e.Code != 11000);
                        if (otherErrors > 0)
                        {
                            Interlocked.Add(ref _failureCount, otherErrors);
                            _log.WriteLine(
                                $"Document copyencountered errors for segment {mu.DatabaseName}.{mu.CollectionName}[{migrationChunkIndex}.{segmentId}]",
                                LogType.Error);
                            LogErrors(
                                ex.WriteErrors.Where(e => e.Code != 11000).ToList(),
                                $"segment {mu.DatabaseName}.{mu.CollectionName}[{migrationChunkIndex}.{segmentId}]"
                            );
                        }
                    }
                    catch (OutOfMemoryException ex)
                    {
                        _log.WriteLine(
                            $"Encountered Out Of Memory exception for segment {mu.DatabaseName}.{mu.CollectionName}[{migrationChunkIndex}.{segmentId}]. " +
                            $"Try reducing _pageSize. Details: {ex}", LogType.Error);
						return TaskResult.Retry;
					}
                    catch (Exception ex) when (ex.ToString().Contains("canceled."))
                    {
                        _log.WriteLine($"Document copy operation for segment {mu.DatabaseName}.{mu.CollectionName}[{migrationChunkIndex}.{segmentId}] was canceled.");
						//throw new OperationCanceledException("Document copy operation was cancelled.");
						return TaskResult.Canceled;
					}
                    catch (Exception ex)
                    {
                        errors.Add(ex);
                        _log.WriteLine(
                            $"Batch processing error encountered during document copy of segment {mu.DatabaseName}.{mu.CollectionName}[{migrationChunkIndex}.{segmentId}]. Details: {ex}",
                            LogType.Error);
                        failed = true;
                    }
                    finally
                    {
                        UpdateProgress(segmentId, jobList, mu, migrationChunkIndex, basePercent, contribFactor, targetCount, _successCount, _failureCount, _skippedCount);
                        pageIndex++;
                    }

                    if (failed)
                        break;                    
                }

                if(!cancellationToken.IsCancellationRequested)
                {
                    if(failed)
                    {
                        _log.WriteLine($"Document copy failed for segment {mu.DatabaseName}.{mu.CollectionName}[{migrationChunkIndex}.{segmentId}].");
                    }
                    else if(_failureCount > 0 || _skippedCount > 0)
					{
                        _log.WriteLine($"Document copy completed for segment {mu.DatabaseName}.{mu.CollectionName}[{migrationChunkIndex}.{segmentId}] with {_successCount} documents copied, {_skippedCount} documents skipped(duplicate), {_failureCount} documents failed.");
					}
                    else
                    {
						_log.WriteLine($"Document copy completed for segment {mu.DatabaseName}.{mu.CollectionName}[{migrationChunkIndex}.{segmentId}] with {_successCount} documents copied.");
					}
                    segment.IsProcessed = !failed;
                    jobList.Save();
					return TaskResult.Success;
				}
                else
					return TaskResult.Canceled;
			}
            catch (Exception ex) when (!(ex is OperationCanceledException))
            {
                errors.Add(ex);
                _log.WriteLine($"Document copy encountered error while processing segment {mu.DatabaseName}.{mu.CollectionName}[{migrationChunkIndex}.{segmentId}], Details: {ex}", LogType.Error);
				return TaskResult.Retry;
			}
        }


        private void LogErrors(List<BulkWriteError> exceptions, string location)
        {
            foreach (var error in exceptions)
            {
               _log.WriteLine($"Document copy encountered write errors for {location}. Details: {error.ToString()}");

            }
        }

        private async Task CheckForMissingDocumentsAsync(
            JobList jobList,
            MigrationUnit mu,
            int migrationChunkIndex,
            FilterDefinition<BsonDocument> baseFilter,
            CancellationToken cancellationToken,
            bool isWriteSimulated)
        {
            var migrationChunk = mu.MigrationChunks[migrationChunkIndex];
            
            // Check if any segment has QueryDocCount > ResultDocCount
            var segmentsWithMissingDocs = migrationChunk.Segments
                .Where(s => s.QueryDocCount > s.ResultDocCount)
                .ToList();

            if (!segmentsWithMissingDocs.Any())
            {
                _log.WriteLine($"No missing documents detected during chunk-level comparison of {mu.DatabaseName}.{mu.CollectionName}[{migrationChunkIndex}]");
                return;
            }

            long missingDocCount = segmentsWithMissingDocs.Sum(s => s.QueryDocCount - s.ResultDocCount);
            _log.WriteLine($"Starting chunk-level comparison for {missingDocCount} missing documents in {mu.DatabaseName}.{mu.CollectionName}[{migrationChunkIndex}].");

            // Build chunk-level filter instead of segment-specific filters
            FilterDefinition<BsonDocument> chunkFilter = baseFilter;
            
            if (migrationChunk.Segments.Count > 1)
            {
#pragma warning disable CS8604 // Possible null reference argument.
                var bounds = SamplePartitioner.GetChunkBounds(migrationChunk.Gte, migrationChunk.Lt, migrationChunk.DataType);
#pragma warning restore CS8604 // Possible null reference argument.
                var gte = bounds.gte;
                var lt = bounds.lt;

                FilterDefinition<BsonDocument> idFilter = MongoHelper.GenerateQueryFilter(gte, lt, migrationChunk.DataType, MongoHelper.ConvertUserFilterToBSONDocument(mu.UserFilter!), mu.DataTypeFor_Id.HasValue);
                
                BsonDocument? userFilter = BsonDocument.Parse(mu.UserFilter ?? "{}");
                BsonDocument matchCondition = SamplePartitioner.BuildDataTypeCondition(migrationChunk.DataType, userFilter, mu.DataTypeFor_Id.HasValue);

                chunkFilter = Builders<BsonDocument>.Filter.And(idFilter, matchCondition);
            }

            try
            {
                await CompareChunkDocumentsAsync(migrationChunk, mu, migrationChunkIndex, chunkFilter, cancellationToken, isWriteSimulated);
            }
            catch (Exception ex)
            {
                _log.WriteLine($"Error while chunk-level comparison for {mu.DatabaseName}.{mu.CollectionName}[{migrationChunkIndex}]: {ex.Message}", LogType.Error);
            }

            jobList.Save();
        }

        private async Task CompareChunkDocumentsAsync(
            MigrationChunk migrationChunk,
            MigrationUnit mu,
            int migrationChunkIndex,
            FilterDefinition<BsonDocument> chunkFilter,
            CancellationToken cancellationToken,
            bool isWriteSimulated)
        {
            var missingDocuments = new List<BsonDocument>();
            int pageIndex = 0;
            const int comparisonPageSize = 100; // Smaller page size for comparison
            long processedCount = 0;
            long foundMissingCount = 0;
            
            _log.WriteLine($"Starting chunk-level document comparison for {mu.DatabaseName}.{mu.CollectionName}[{migrationChunkIndex}]");

            while (!cancellationToken.IsCancellationRequested)
            {
                // Get documents from source using chunk filter
                var sourceDocuments = await _sourceCollection.Aggregate()
                    .Match(chunkFilter)
                    .Skip(pageIndex * comparisonPageSize)
                    .Limit(comparisonPageSize)
                    .ToListAsync(cancellationToken);

                if (sourceDocuments.Count == 0)
                    break;

                processedCount += sourceDocuments.Count;

                // Batch check documents in target using $in operator for better performance
                var documentIds = sourceDocuments
                    .Where(doc => doc.Contains("_id"))
                    .Select(doc => doc["_id"])
                    .ToList();

                if (documentIds.Count > 0)
                {
                    var targetFilter = Builders<BsonDocument>.Filter.In("_id", documentIds);
                    var existingTargetDocs = await _targetCollection.Find(targetFilter)
                        .Project(Builders<BsonDocument>.Projection.Include("_id"))
                        .ToListAsync(cancellationToken);

                    var existingIds = new HashSet<BsonValue>(existingTargetDocs.Select(doc => doc["_id"]));

                    // Find missing documents
                    foreach (var sourceDoc in sourceDocuments)
                    {
                        if (cancellationToken.IsCancellationRequested)
                            return;

                        if (!sourceDoc.Contains("_id"))
                            continue;

                        var idValue = sourceDoc["_id"];

                        if (!existingIds.Contains(idValue))
                        {
                            missingDocuments.Add(sourceDoc);
                            foundMissingCount++;
                            
                            // Attempt to insert the missing document if not simulated
                            if (!isWriteSimulated)
                            {
                                try
                                {
                                    await _targetCollection.InsertOneAsync(sourceDoc, cancellationToken: cancellationToken);
                                    Interlocked.Increment(ref _successCount);
                                    
                                    // Update the segment that should contain this document
                                    UpdateSegmentResultCount(migrationChunk);
                                }
                                catch (MongoDuplicateKeyException)
                                {
                                    // Document was inserted by another process, skip                                   
                                    Interlocked.Increment(ref _skippedCount);
                                    
                                    // Update the segment that should contain this document
                                    UpdateSegmentResultCount(migrationChunk);
                                }
                                catch (Exception ex)
                                {
                                    _log.WriteLine($"Failed to insert missing document with _id: {idValue}. Error: {ex.Message}", LogType.Error);
                                    Interlocked.Increment(ref _failureCount);
                                }
                            }                            
                        }
                    }
                }

                pageIndex++;

                // Log progress every 10 pages
                if (pageIndex % 10 == 0)
                {
                    _log.AddVerboseMessage($"Processed {processedCount} documents, found {foundMissingCount} missing documents in chunk {mu.DatabaseName}.{mu.CollectionName}[{migrationChunkIndex}]");
                }

                // Limit the number of pages to prevent infinite loops
                if (pageIndex > 10000)
                {
                    _log.WriteLine($"Reached maximum page limit while comparing chunk {mu.DatabaseName}.{mu.CollectionName}[{migrationChunkIndex}]", LogType.Error);
                    break;
                }
            }

            if (foundMissingCount > 0)
            {
                _log.WriteLine($"Found and processed {foundMissingCount} missing documents in chunk {mu.DatabaseName}.{mu.CollectionName}[{migrationChunkIndex}] out of {processedCount} total documents checked");
                migrationChunk.SkippedAsDuplicateCount = _skippedCount;
                migrationChunk.DumpResultDocCount = _successCount + _skippedCount;
                migrationChunk.RestoredSuccessDocCount = _successCount + _skippedCount;
                migrationChunk.RestoredFailedDocCount = _failureCount;
            }
            else
            {
                _log.WriteLine($"No missing documents found during chunk-level comparison for {mu.DatabaseName}.{mu.CollectionName}[{migrationChunkIndex}]");
            }
        }

        private void UpdateSegmentResultCount(MigrationChunk migrationChunk)
        {
            // Simple approach: increment the first segment that might contain this document
            // This is an approximation since we're not using segment-specific logic
            var segmentToUpdate = migrationChunk.Segments.FirstOrDefault(s => s.QueryDocCount > s.ResultDocCount);
            if (segmentToUpdate != null)
            {
                segmentToUpdate.ResultDocCount++;
            }
        }
    }
}

