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

        public void AddInsert(ChangeStreamDocument<BsonDocument> change)
        {
            var id = change.DocumentKey.GetValue("_id");

            // Remove from other lists
            DocsToBeUpdated.RemoveAll(c => c.DocumentKey.GetValue("_id") == id);
            DocsToBeDeleted.RemoveAll(c => c.DocumentKey.GetValue("_id") == id);

            // Replace if already exists
            DocsToBeInserted.RemoveAll(c => c.DocumentKey.GetValue("_id") == id);
            DocsToBeInserted.Add(change);
        }

        public void AddUpdate(ChangeStreamDocument<BsonDocument> change)
        {
            var id = change.DocumentKey.GetValue("_id");

            // Remove from delete list
            DocsToBeDeleted.RemoveAll(c => c.DocumentKey.GetValue("_id") == id);

            // Replace in update list
            DocsToBeUpdated.RemoveAll(c => c.DocumentKey.GetValue("_id") == id);
            DocsToBeUpdated.Add(change);

            // Don't remove from insert — updates after insert are valid
        }

        public void AddDelete(ChangeStreamDocument<BsonDocument> change)
        {
            var id = change.DocumentKey.GetValue("_id");

            // Remove from insert and update
            DocsToBeInserted.RemoveAll(c => c.DocumentKey.GetValue("_id") == id);
            DocsToBeUpdated.RemoveAll(c => c.DocumentKey.GetValue("_id") == id);

            // Replace in delete list
            DocsToBeDeleted.RemoveAll(c => c.DocumentKey.GetValue("_id") == id);
            DocsToBeDeleted.Add(change);
        }
    }
}
