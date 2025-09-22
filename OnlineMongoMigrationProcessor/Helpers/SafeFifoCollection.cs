using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;

namespace OnlineMongoMigrationProcessor.Helpers
{
    /// <summary>
    /// Thread-safe collection that provides both key-based access and FIFO ordering for retrieval
    /// </summary>
    internal class SafeFifoCollection<TKey, TValue> where TKey : notnull
    {
        private readonly ConcurrentDictionary<TKey, TValue> _dictionary = new();
        private readonly ConcurrentQueue<TKey> _insertionOrder = new();
        private readonly object _lock = new object();

        public int Count => _dictionary.Count;

        /// <summary>
        /// Adds or updates an item. If key already exists, the value is updated but insertion order is preserved.
        /// </summary>
        public void AddOrUpdate(TKey key, TValue value)
        {
            lock (_lock)
            {
                bool isNewKey = !_dictionary.ContainsKey(key);
                _dictionary[key] = value;
                
                // Only add to queue if it's a new key
                if (isNewKey)
                {
                    _insertionOrder.Enqueue(key);
                }
            }
        }

        /// <summary>
        /// Tries to get a value by key
        /// </summary>
        public bool TryGet(TKey key, [MaybeNullWhen(false)] out TValue value)
        {
            return _dictionary.TryGetValue(key, out value);
        }

        /// <summary>
        /// Removes an item by key
        /// </summary>
        public bool Remove(TKey key)
        {
            lock (_lock)
            {
                bool removed = _dictionary.TryRemove(key, out _);
                
                if (removed)
                {
                    // Clean up the queue by removing stale keys
                    CleanupQueue();
                }
                
                return removed;
            }
        }

        /// <summary>
        /// Checks if a key exists
        /// </summary>
        public bool ContainsKey(TKey key)
        {
            return _dictionary.ContainsKey(key);
        }

        /// <summary>
        /// Clears all items
        /// </summary>
        public void Clear()
        {
            lock (_lock)
            {
                _dictionary.Clear();
                
                // Clear the queue
                while (_insertionOrder.TryDequeue(out _)) { }
            }
        }

        /// <summary>
        /// Gets all items as a snapshot
        /// </summary>
        public IEnumerable<KeyValuePair<TKey, TValue>> GetAll()
        {
            return _dictionary.ToArray();
        }

        /// <summary>
        /// Gets the first item in FIFO order (oldest insertion)
        /// </summary>
        public bool TryGetFirst(out KeyValuePair<TKey, TValue> item)
        {
            lock (_lock)
            {
                // Find the first valid key in the queue
                while (_insertionOrder.TryPeek(out TKey? key))
                {
                    if (_dictionary.TryGetValue(key, out TValue? value))
                    {
                        item = new KeyValuePair<TKey, TValue>(key, value);
                        return true;
                    }
                    else
                    {
                        // Key was removed, dequeue it and continue
                        _insertionOrder.TryDequeue(out _);
                    }
                }

                item = default!;
                return false;
            }
        }

        /// <summary>
        /// Removes stale keys from the queue that no longer exist in the dictionary
        /// </summary>
        private void CleanupQueue()
        {
            // Create a new queue with only valid keys
            var validKeys = new List<TKey>();
            
            while (_insertionOrder.TryDequeue(out TKey? key))
            {
                if (_dictionary.ContainsKey(key))
                {
                    validKeys.Add(key);
                }
            }

            // Re-enqueue valid keys in the same order
            foreach (var key in validKeys)
            {
                _insertionOrder.Enqueue(key);
            }
        }
    }
}