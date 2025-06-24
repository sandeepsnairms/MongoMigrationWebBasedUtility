using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;


namespace OnlineMongoMigrationProcessor.Helpers
{
    internal  class SafeDictionary<TKey, TValue>
    {
        private  readonly ConcurrentDictionary<TKey, TValue> _dictionary = new();

        public int Count => _dictionary.Count;

        public  void AddOrUpdate(TKey key, TValue value)
        {
            _dictionary[key] = value; // Adds or updates safely
        }

        public  bool TryGet(TKey key, out TValue value)
        {
            return _dictionary.TryGetValue(key, out value);
        }

        public  bool Remove(TKey key)
        {
            return _dictionary.TryRemove(key, out _);
        }

        public  bool ContainsKey(TKey key)
        {
            return _dictionary.ContainsKey(key);
        }

        public  void Clear()
        {
            _dictionary.Clear(); // Not atomic per se, but thread-safe for general use
        }

        public  IEnumerable<KeyValuePair<TKey, TValue>> GetAll()
        {
            return _dictionary.ToList(); // Snapshot of the current state
        }

        public bool TryGetFirst(out KeyValuePair<TKey, TValue> item)
        {
            foreach (var kvp in _dictionary)
            {
                item = kvp;
                return true;
            }

            item = default;
            return false;
        }
    }
}
