using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace OnlineMongoMigrationProcessor.Persistence
{
    /// <summary>
    /// Abstract base class for persistence storage implementations.
    /// Provides a common interface for storing and retrieving documents and log entries.
    /// </summary>
    public abstract class PersistenceStorage
    {
        /// <summary>
        /// Initializes the persistence storage with the provided connection string or path.
        /// This method should be thread-safe and idempotent.
        /// </summary>
        /// <param name="connectionStringOrPath">Connection string or file path for the storage</param>
        public abstract void Initialize(string connectionStringOrPath);

        /// <summary>
        /// Upserts a document with the specified id.
        /// Creates a new document if it doesn't exist, updates if it does.
        /// </summary>
        /// <param name="id">Unique identifier for the document</param>
        /// <param name="jsonContent">JSON content to store</param>
        /// <returns>True if successful, false otherwise</returns>
        public abstract Task<bool> UpsertDocumentAsync(string id, string jsonContent);

        /// <summary>
        /// Synchronous version of UpsertDocumentAsync
        /// </summary>
        /// <param name="id">Unique identifier for the document</param>
        /// <param name="jsonContent">JSON content to store</param>
        /// <returns>True if successful, false otherwise</returns>
        public virtual bool UpsertDocument(string id, string jsonContent)
        {
            return UpsertDocumentAsync(id, jsonContent).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Reads a document by its id
        /// </summary>
        /// <param name="id">Unique identifier of the document</param>
        /// <returns>JSON content if found, null otherwise</returns>
        public abstract Task<string?> ReadDocumentAsync(string id);

        /// <summary>
        /// Synchronous version of ReadDocumentAsync
        /// </summary>
        /// <param name="id">Unique identifier of the document</param>
        /// <returns>JSON content if found, null otherwise</returns>
        public virtual string? ReadDocument(string id)
        {
            return ReadDocumentAsync(id).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Checks if a document exists by its id
        /// </summary>
        /// <param name="id">Unique identifier of the document</param>
        /// <returns>True if document exists, false otherwise</returns>
        public abstract Task<bool> DocumentExistsAsync(string id);

        /// <summary>
        /// Synchronous version of DocumentExistsAsync
        /// </summary>
        public virtual bool DocumentExists(string id)
        {
            return DocumentExistsAsync(id).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Deletes a document by its id
        /// </summary>
        /// <param name="id">Unique identifier of the document to delete</param>
        /// <returns>True if deleted, false otherwise</returns>
        public abstract Task<bool> DeleteDocumentAsync(string id);

        /// <summary>
        /// Synchronous version of DeleteDocumentAsync
        /// </summary>
        public virtual bool DeleteDocument(string id)
        {
            return DeleteDocumentAsync(id).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Lists all document IDs in the storage
        /// </summary>
        /// <returns>List of document IDs</returns>
        public abstract Task<List<string>> ListDocumentIdsAsync();

        /// <summary>
        /// Synchronous version of ListDocumentIdsAsync
        /// </summary>
        public virtual List<string> ListDocumentIds()
        {
            return ListDocumentIdsAsync().GetAwaiter().GetResult();
        }

        /// <summary>
        /// Pushes a LogObject to the LogEntries array in the document.
        /// If the document doesn't exist, it creates a new one with the LogEntries array.
        /// If it exists, appends the LogObject to the existing array.
        /// </summary>
        /// <param name="id">Unique identifier of the document</param>
        /// <param name="logObject">LogObject to push to the array</param>
        /// <returns>True if successful, false otherwise</returns>
        public abstract Task<bool> PushLogEntryAsync(string id, LogObject logObject);

        /// <summary>
        /// Synchronous version of PushLogEntryAsync
        /// </summary>
        /// <param name="id">Unique identifier of the document</param>
        /// <param name="logObject">LogObject to push to the array</param>
        /// <returns>True if successful, false otherwise</returns>
        public virtual bool PushLogEntry(string id, LogObject logObject)
        {
            return PushLogEntryAsync(id, logObject).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Tests the connection to the storage
        /// </summary>
        /// <returns>True if connection is successful, false otherwise</returns>
        public abstract Task<bool> TestConnectionAsync();

        /// <summary>
        /// Checks if the storage is initialized
        /// </summary>
        public abstract bool IsInitialized { get; }
    }
}
