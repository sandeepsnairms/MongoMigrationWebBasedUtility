using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace OnlineMongoMigrationProcessor.Persistence
{
    /// <summary>
    /// Disk-based implementation of PersistenceStorage.
    /// Stores documents as JSON files on the local file system.
    /// </summary>
    public class DiskPersistence : PersistenceStorage
    {
        private static string? _storagePath;
        private static bool _isInitialized = false;
        private static readonly object _initLock = new object();

        private const string FILE_EXTENSION = ".json";

        /// <summary>
        /// Initializes the disk persistence layer with the provided storage path.
        /// This method is thread-safe and idempotent.
        /// </summary>
        /// <param name="connectionStringOrPath">Directory path where files will be stored</param>
        /// <exception cref="ArgumentException">Thrown when path is null or empty</exception>
        /// <exception cref="InvalidOperationException">Thrown when initialization fails</exception>
        public override void Initialize(string connectionStringOrPath)
        {
            if (_isInitialized)
                return;

            lock (_initLock)
            {
                if (_isInitialized)
                    return;

                if (string.IsNullOrWhiteSpace(connectionStringOrPath))
                    throw new ArgumentException("Storage path cannot be null or empty", nameof(connectionStringOrPath));

                try
                {
                    _storagePath = connectionStringOrPath;
                    
                    // Create directory if it doesn't exist
                    if (!Directory.Exists(_storagePath))
                    {
                        Directory.CreateDirectory(_storagePath);
                    }

                    _isInitialized = true;
                }
                catch (Exception ex)
                {
                    throw new InvalidOperationException($"Failed to initialize DiskPersistence: {ex.Message}", ex);
                }
            }
        }

        /// <summary>
        /// Ensures the storage is initialized before operations
        /// </summary>
        private static void EnsureInitialized()
        {
            if (!_isInitialized || string.IsNullOrEmpty(_storagePath))
                throw new InvalidOperationException("DiskPersistence is not initialized. Call Initialize() first with a valid path.");
        }

        /// <summary>
        /// Gets the file path for a document id.
        /// Handles hierarchical IDs like "job1\mu1.json" by creating folder structure.
        /// The ID should include the .json extension for files.
        /// </summary>
        private static string GetFilePath(string id)
        {
            // Split by backslash or forward slash to handle hierarchical structure
            var parts = id.Split('\\', '/');
            
            if (parts.Length == 1)
            {
                // Simple ID, just use as filename (already has .json extension)
                var sanitizedId = SanitizeFileName(parts[0]);
                return Path.Combine(_storagePath!, sanitizedId);
            }
            else
            {
                // Hierarchical ID like "job1\mu1.json"
                // Create folder structure: storagePath/job1/mu1.json
                var pathParts = new List<string> { _storagePath! };
                
                // Add all parts except the last as directories
                for (int i = 0; i < parts.Length - 1; i++)
                {
                    pathParts.Add(SanitizeFileName(parts[i]));
                }
                
                // Create the directory structure if it doesn't exist
                var directoryPath = Path.Combine(pathParts.ToArray());
                if (!Directory.Exists(directoryPath))
                {
                    Directory.CreateDirectory(directoryPath);
                }
                
                // Add the last part as the filename (already has .json extension)
                var fileName = SanitizeFileName(parts[^1]);
                pathParts.Add(fileName);
                
                return Path.Combine(pathParts.ToArray());
            }
        }

        /// <summary>
        /// Gets the directory path for a folder id (id without .json extension).
        /// </summary>
        private static string GetDirectoryPath(string id)
        {
            // Split by backslash or forward slash
            var parts = id.Split('\\', '/');
            
            var pathParts = new List<string> { _storagePath! };
            
            // Add all parts as directories
            foreach (var part in parts)
            {
                pathParts.Add(SanitizeFileName(part));
            }
            
            return Path.Combine(pathParts.ToArray());
        }

        /// <summary>
        /// Sanitizes a string to be used as a filename or folder name
        /// </summary>
        private static string SanitizeFileName(string fileName)
        {
            var invalidChars = Path.GetInvalidFileNameChars();
            return string.Join("_", fileName.Split(invalidChars, StringSplitOptions.RemoveEmptyEntries));
        }

        /// <summary>
        /// Upserts a document with the specified id.
        /// Creates a new document if it doesn't exist, updates if it does.
        /// </summary>
        /// <param name="id">Unique identifier for the document (must include .json extension, e.g., "job1\mu1.json")</param>
        /// <param name="jsonContent">JSON content to store</param>
        /// <returns>True if successful, false otherwise</returns>
        public override async Task<bool> UpsertDocumentAsync(string id, string jsonContent)
        {
            EnsureInitialized();

            if (string.IsNullOrWhiteSpace(id))
                throw new ArgumentException("ID cannot be null or empty", nameof(id));

            if (string.IsNullOrWhiteSpace(jsonContent))
                throw new ArgumentException("JSON content cannot be null or empty", nameof(jsonContent));

            if (!id.EndsWith(FILE_EXTENSION))
                throw new ArgumentException($"ID must end with {FILE_EXTENSION} extension", nameof(id));

            try
            {
                var filePath = GetFilePath(id);
                return Helper.WriteAtomicFile(filePath, jsonContent);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[DiskPersistence] Error upserting document {id}: {ex.Message}");
                return false;
            }
        }

        /// <summary>
        /// Reads a document by its id
        /// </summary>
        /// <param name="id">Unique identifier of the document (must include .json extension, e.g., "job1\mu1.json")</param>
        /// <returns>JSON content if found, null otherwise</returns>
        public override async Task<string?> ReadDocumentAsync(string id)
        {
            EnsureInitialized();

            if (string.IsNullOrWhiteSpace(id))
                throw new ArgumentException("ID cannot be null or empty", nameof(id));

            if (!id.EndsWith(FILE_EXTENSION))
                throw new ArgumentException($"ID must end with {FILE_EXTENSION} extension", nameof(id));

            try
            {
                var filePath = GetFilePath(id);

                using var fs = new FileStream(
                    filePath,
                    FileMode.Open,
                    FileAccess.Read,
                    FileShare.ReadWrite | FileShare.Delete);
                using var sr = new StreamReader(fs);
                return sr.ReadToEnd();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[DiskPersistence] Error reading document {id}: {ex.Message}");
                return null;
            }
        }

        /// <summary>
        /// Checks if a document exists by its id
        /// </summary>
        /// <param name="id">Unique identifier of the document (must include .json extension, e.g., "job1\mu1.json")</param>
        /// <returns>True if document exists, false otherwise</returns>
        public override Task<bool> DocumentExistsAsync(string id)
        {
            EnsureInitialized();

            if (string.IsNullOrWhiteSpace(id))
                return Task.FromResult(false);

            if (!id.EndsWith(FILE_EXTENSION))
                throw new ArgumentException($"ID must end with {FILE_EXTENSION} extension", nameof(id));

            try
            {
                var filePath = GetFilePath(id);
                return Task.FromResult(File.Exists(filePath));
            }
            catch
            {
                return Task.FromResult(false);
            }
        }

        /// <summary>
        /// Deletes a document or folder by its id.
        /// If id ends with .json, deletes the file.
        /// If id doesn't end with .json, deletes the entire folder.
        /// </summary>
        /// <param name="id">Unique identifier of the document/folder (e.g., "job1\mu1.json" for file, "job1" for folder)</param>
        /// <returns>True if deleted, false otherwise</returns>
        public override Task<bool> DeleteDocumentAsync(string id)
        {
            EnsureInitialized();

            if (string.IsNullOrWhiteSpace(id))
                throw new ArgumentException("ID cannot be null or empty", nameof(id));

            try
            {
                if (id.EndsWith(FILE_EXTENSION))
                {
                    // Delete file
                    var filePath = GetFilePath(id);
                    
                    if (!File.Exists(filePath))
                        return Task.FromResult(false);

                    File.Delete(filePath);
                    return Task.FromResult(true);
                }
                else
                {
                    // Delete folder
                    var directoryPath = GetDirectoryPath(id);
                    
                    if (!Directory.Exists(directoryPath))
                        return Task.FromResult(false);

                    Directory.Delete(directoryPath, recursive: true);
                    return Task.FromResult(true);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[DiskPersistence] Error deleting document/folder {id}: {ex.Message}");
                return Task.FromResult(false);
            }
        }

        /// <summary>
        /// Lists all document IDs in the storage.
        /// Returns IDs with .json extension included.
        /// </summary>
        /// <returns>List of document IDs (e.g., "job1\mu1.json", "settings.json")</returns>
        public override Task<List<string>> ListDocumentIdsAsync()
        {
            EnsureInitialized();

            try
            {
                var ids = new List<string>();
                
                // Recursively find all .json files
                var files = Directory.GetFiles(_storagePath!, "*" + FILE_EXTENSION, SearchOption.AllDirectories);
                
                foreach (var file in files)
                {
                    // Get relative path from storage root
                    var relativePath = Path.GetRelativePath(_storagePath!, file);
                    
                    // Convert path separators to backslash for consistency (keep .json extension)
                    var id = relativePath.Replace(Path.DirectorySeparatorChar, '\\');
                    
                    ids.Add(id);
                }
                
                return Task.FromResult(ids);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[DiskPersistence] Error listing document IDs: {ex.Message}");
                return Task.FromResult(new List<string>());
            }
        }

        /// <summary>
        /// Pushes a LogObject to the LogEntries array in the document.
        /// If the document doesn't exist, it creates a new one with the LogEntries array.
        /// If it exists, appends the LogObject to the existing array.
        /// </summary>
        /// <param name="id">Unique identifier of the document (must include .json extension, e.g., "job1\mu1.json")</param>
        /// <param name="logObject">LogObject to push to the array</param>
        /// <returns>True if successful, false otherwise</returns>
        public override async Task<bool> PushLogEntryAsync(string id, LogObject logObject)
        {
            EnsureInitialized();

            if (string.IsNullOrWhiteSpace(id))
                throw new ArgumentException("ID cannot be null or empty", nameof(id));

            if (logObject == null)
                throw new ArgumentNullException(nameof(logObject));

            if (!id.EndsWith(FILE_EXTENSION))
                throw new ArgumentException($"ID must end with {FILE_EXTENSION} extension", nameof(id));

            try
            {
                var filePath = GetFilePath(id);
                LogDocument? document;

                // Read existing document or create new one
                if (File.Exists(filePath))
                {
                    var jsonContent = await File.ReadAllTextAsync(filePath);
                    document = JsonConvert.DeserializeObject<LogDocument>(jsonContent);
                    
                    if (document == null)
                    {
                        document = new LogDocument { LogEntries = new List<LogObject>() };
                    }
                }
                else
                {
                    document = new LogDocument { LogEntries = new List<LogObject>() };
                }

                // Add the new log entry
                document.LogEntries.Add(logObject);

                // Save back to file
                var updatedJson = JsonConvert.SerializeObject(document, Formatting.Indented);
                await File.WriteAllTextAsync(filePath, updatedJson);

                return true;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[DiskPersistence] Error pushing log entry to document {id}: {ex.Message}");
                return false;
            }
        }

        /// <summary>
        /// Tests the connection to the storage
        /// </summary>
        /// <returns>True if connection is successful, false otherwise</returns>
        public override Task<bool> TestConnectionAsync()
        {
            if (!_isInitialized || string.IsNullOrEmpty(_storagePath))
                return Task.FromResult(false);

            try
            {
                return Task.FromResult(Directory.Exists(_storagePath));
            }
            catch
            {
                return Task.FromResult(false);
            }
        }

        /// <summary>
        /// Checks if the storage is initialized
        /// </summary>
        public override bool IsInitialized => _isInitialized;

        /// <summary>
        /// Helper class for storing log documents with LogEntries array
        /// </summary>
        private class LogDocument
        {
            public List<LogObject> LogEntries { get; set; } = new List<LogObject>();
        }
    }
}
