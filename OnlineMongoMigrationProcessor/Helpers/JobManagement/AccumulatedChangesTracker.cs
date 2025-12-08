using MongoDB.Bson;
using MongoDB.Driver;
using OnlineMongoMigrationProcessor.Helpers.Mongo;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OnlineMongoMigrationProcessor.Helpers.JobManagement
{
    public class AccumulatedChangesTracker
    {
        public List<ChangeStreamDocument<BsonDocument>> DocsToBeInserted { get; private set; } = new();
        public List<ChangeStreamDocument<BsonDocument>> DocsToBeUpdated { get; private set; } = new();
        public List<ChangeStreamDocument<BsonDocument>> DocsToBeDeleted { get; private set; } = new();


        // Track the latest resume token for checkpoint on success
        public string CollectionKey { get; private set; } = string.Empty;
        public string LatestResumeToken { get; private set; } = string.Empty;
        public DateTime LatestTimestamp { get; private set; } = DateTime.MinValue;
        public ChangeStreamOperationType LatestOperationType { get; private set; }
        public string LatestDocumentKey { get; private set; } = string.Empty;

        // Track batch latencies
        public long CSTotalReadDurationInMS { get; set; } = 0;
        public long CSTotaWriteDurationInMS { get; set; } = 0;

        private string _collectionKey = string.Empty;
        public AccumulatedChangesTracker(string collectionKey)
        {
            _collectionKey = collectionKey;
            CollectionKey = _collectionKey;
        }


        public void AddInsert(ChangeStreamDocument<BsonDocument> change)
        {
            var changeCollectionKey = $"{change.DatabaseNamespace?.DatabaseName}.{change.CollectionNamespace?.CollectionName}";
            if (changeCollectionKey != _collectionKey)
                return;

            var id = change.DocumentKey.ToJson();

            if(string.IsNullOrEmpty(id))
                return;

            // Remove from other lists
            DocsToBeUpdated.RemoveAll(c => c.DocumentKey != null && c.DocumentKey.ToJson() == id);
            DocsToBeDeleted.RemoveAll(c => c.DocumentKey != null && c.DocumentKey.ToJson() == id);

            // Replace if already exists
            DocsToBeInserted.RemoveAll(c => c.DocumentKey != null && c.DocumentKey.ToJson() == id);
            DocsToBeInserted.Add(change);

            // Track earliest and latest change metadata for checkpoint updates
            UpdateMetadata(change);
        }

        public void AddUpdate(ChangeStreamDocument<BsonDocument> change)
        {
            var changeCollectionKey = $"{change.DatabaseNamespace?.DatabaseName}.{change.CollectionNamespace?.CollectionName}";
            if (changeCollectionKey != _collectionKey)
                return;

            var id = change.DocumentKey.ToJson();

            if (string.IsNullOrEmpty(id))
                return;

            // Remove from delete list
            DocsToBeDeleted.RemoveAll(c => c.DocumentKey != null && c.DocumentKey != null && c.DocumentKey.ToJson() == id);

            // Replace in update list
            DocsToBeUpdated.RemoveAll(c => c.DocumentKey != null && c.DocumentKey.ToJson() == id);
            DocsToBeUpdated.Add(change);

            // Don't remove from insert — updates after insert are valid

            // Track earliest and latest change metadata for checkpoint updates
            UpdateMetadata(change);
        }

        public void AddDelete(ChangeStreamDocument<BsonDocument> change)
        {
            var changeCollectionKey = $"{change.DatabaseNamespace?.DatabaseName}.{change.CollectionNamespace?.CollectionName}";
            if (changeCollectionKey != _collectionKey)
                return;

            var id = change.DocumentKey.ToJson();

            if (string.IsNullOrEmpty(id))
                return;

            // Remove from insert and update
            DocsToBeInserted.RemoveAll(c => c.DocumentKey != null && c.DocumentKey != null && c.DocumentKey.ToJson() == id);
            DocsToBeUpdated.RemoveAll(c => c.DocumentKey != null && c.DocumentKey != null && c.DocumentKey.ToJson() == id);

            // Replace in delete list
            DocsToBeDeleted.RemoveAll(c => c.DocumentKey != null && c.DocumentKey != null && c.DocumentKey.ToJson() == id);
            DocsToBeDeleted.Add(change);

            // Track earliest and latest change metadata for checkpoint updates
            UpdateMetadata(change);
        }

        /// <summary>
        /// Track both earliest and latest change metadata for safe checkpoint updates.
        /// - Earliest token: Used for rollback if batch fails
        /// - Latest token: Used for checkpoint if batch succeeds
        /// This ensures resume tokens are only updated AFTER successful batch processing.
        /// </summary>
        private void UpdateMetadata(ChangeStreamDocument<BsonDocument> change)
        {
            DateTime changeTimestamp = DateTime.MinValue;
            
            // Extract timestamp from ClusterTime or WallTime
            if (change.ClusterTime != null)
            {
                changeTimestamp = MongoHelper.BsonTimestampToUtcDateTime(change.ClusterTime);
            }
            else if (change.WallTime != null)
            {
                changeTimestamp = change.WallTime.Value;
            }
           
            // Always track latest (last change in batch) - for checkpoint on success
            if (change.ResumeToken != null && change.ResumeToken != BsonNull.Value)
            {
                if (changeTimestamp >= LatestTimestamp && !string.IsNullOrEmpty(change.ResumeToken.ToJson()))
                {
                    LatestResumeToken = change.ResumeToken.ToJson();
                    LatestOperationType = change.OperationType;
                    LatestDocumentKey = change.DocumentKey.ToJson();
                    LatestTimestamp = changeTimestamp;
                }
            }
        }

        /// <summary>
        /// Clear all metadata after successful checkpoint update or on failure rollback.
        /// </summary>
        public void ClearMetadata()
        {
           
            // Clear latest
            LatestResumeToken = string.Empty;
            LatestTimestamp = DateTime.MinValue;
            LatestDocumentKey = string.Empty;
        }       
        
    }
}
