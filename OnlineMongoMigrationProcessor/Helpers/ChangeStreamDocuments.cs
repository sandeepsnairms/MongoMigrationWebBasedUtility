using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OnlineMongoMigrationProcessor.Helpers
{
    public class ChangeStreamDocuments
    {
        public List<ChangeStreamDocument<BsonDocument>> DocsToBeInserted { get; private set; } = new();
        public List<ChangeStreamDocument<BsonDocument>> DocsToBeUpdated { get; private set; } = new();
        public List<ChangeStreamDocument<BsonDocument>> DocsToBeDeleted { get; private set; } = new();

        // Track the earliest resume token for rollback on failure
        public string EarliestResumeToken { get; private set; } = string.Empty;
        public DateTime EarliestTimestamp { get; private set; } = DateTime.MinValue;
        public ChangeStreamOperationType EarliestOperationType { get; private set; }
        public string EarliestDocumentKey { get; private set; } = string.Empty;

        // Track the latest resume token for checkpoint on success
        public string LatestResumeToken { get; private set; } = string.Empty;
        public DateTime LatestTimestamp { get; private set; } = DateTime.MinValue;
        public ChangeStreamOperationType LatestOperationType { get; private set; }
        public string LatestDocumentKey { get; private set; } = string.Empty;

        public void AddInsert(ChangeStreamDocument<BsonDocument> change)
        {
            var id = change.DocumentKey.ToJson();

            // Remove from other lists
            DocsToBeUpdated.RemoveAll(c => c.DocumentKey.ToJson() == id);
            DocsToBeDeleted.RemoveAll(c => c.DocumentKey.ToJson() == id);

            // Replace if already exists
            DocsToBeInserted.RemoveAll(c => c.DocumentKey.ToJson() == id);
            DocsToBeInserted.Add(change);

            // Track earliest and latest change metadata for checkpoint updates
            UpdateMetadata(change);
        }

        public void AddUpdate(ChangeStreamDocument<BsonDocument> change)
        {
            var id = change.DocumentKey.ToJson();

            // Remove from delete list
            DocsToBeDeleted.RemoveAll(c => c.DocumentKey.ToJson() == id);

            // Replace in update list
            DocsToBeUpdated.RemoveAll(c => c.DocumentKey.ToJson() == id);
            DocsToBeUpdated.Add(change);

            // Don't remove from insert — updates after insert are valid

            // Track earliest and latest change metadata for checkpoint updates
            UpdateMetadata(change);
        }

        public void AddDelete(ChangeStreamDocument<BsonDocument> change)
        {
            var id = change.DocumentKey.ToJson();

            // Remove from insert and update
            DocsToBeInserted.RemoveAll(c => c.DocumentKey.ToJson() == id);
            DocsToBeUpdated.RemoveAll(c => c.DocumentKey.ToJson() == id);

            // Replace in delete list
            DocsToBeDeleted.RemoveAll(c => c.DocumentKey.ToJson() == id);
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

            // Track earliest (first change in batch) - for rollback on failure
            if (string.IsNullOrEmpty(EarliestResumeToken) && change.ResumeToken != null && change.ResumeToken != BsonNull.Value)
            {
                EarliestResumeToken = change.ResumeToken.ToJson();
                EarliestOperationType = change.OperationType;
                EarliestDocumentKey = change.DocumentKey.ToJson();
                EarliestTimestamp = changeTimestamp;
            }

            // Always track latest (last change in batch) - for checkpoint on success
            if (change.ResumeToken != null && change.ResumeToken != BsonNull.Value)
            {
                LatestResumeToken = change.ResumeToken.ToJson();
                LatestOperationType = change.OperationType;
                LatestDocumentKey = change.DocumentKey.ToJson();
                LatestTimestamp = changeTimestamp;
            }
        }

        /// <summary>
        /// Clear all metadata after successful checkpoint update or on failure rollback.
        /// </summary>
        public void ClearMetadata()
        {
            // Clear earliest
            EarliestResumeToken = string.Empty;
            EarliestTimestamp = DateTime.MinValue;
            EarliestDocumentKey = string.Empty;
            
            // Clear latest
            LatestResumeToken = string.Empty;
            LatestTimestamp = DateTime.MinValue;
            LatestDocumentKey = string.Empty;
        }
        
        /// <summary>
        /// DEPRECATED: Use ClearMetadata() instead.
        /// </summary>
        [Obsolete("Use ClearMetadata() instead to clear both earliest and latest metadata")]
        public void ClearLatestMetadata()
        {
            ClearMetadata();
        }
    }
}
