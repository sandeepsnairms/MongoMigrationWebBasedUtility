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
        public abstract void Initialize(string connectionStringOrPath, string AppId);

        /// <summary>
        /// Synchronous version of UpsertDocumentAsync
        /// </summary>
        /// <param name="id">Unique identifier for the document</param>
        /// <param name="jsonContent">JSON content to store</param>
        /// <returns>True if successful, false otherwise</returns>
        public abstract bool UpsertDocument(string id, string jsonContent);

        /// <summary>
        /// Synchronous version of ReadDocumentAsync
        /// </summary>
        /// <param name="id">Unique identifier of the document</param>
        /// <returns>JSON content if found, null otherwise</returns>
        public abstract string? ReadDocument(string id);

        /// <summary>
        /// Synchronous version of DocumentExistsAsync
        /// </summary>
        public abstract bool DocumentExists(string id);

        /// <summary>
        /// Synchronous version of DeleteDocumentAsync
        /// </summary>
        public abstract bool DeleteDocument(string id);

        /// <summary>
        /// Lists all document IDs in the storage
        /// </summary>
        /// <returns>List of document IDs</returns>
        public abstract List<string> ListDocumentIds();


        /// <summary>
        /// Tests the connection to the storage
        /// </summary>
        /// <returns>True if connection is successful, false otherwise</returns>
        public abstract bool TestConnection();

        /// <summary>
        /// Checks if the storage is initialized
        /// </summary>
        public abstract bool IsInitialized { get; }

        public abstract LogBucket ReadLogs(string id, out string fileName);

        public abstract byte[] DownloadLogsAsJsonBytes(string id, int topEntries = 20, int bottomEntries = 230);

        public abstract void PushLogEntry(string JobId,LogObject logObj);
    }
}
