using System.Collections.Generic;

namespace OSM_pbf_convert
{
    public class CachedAccessor<TKey, TValue> : IAccessor<TKey, TValue>
    {
        private readonly IAccessor<TKey, TValue> accessor;
        private readonly Dictionary<TKey, CacheItem<TValue>> cache = new Dictionary<TKey, CacheItem<TValue>>();
        private readonly int limit = 2000;

        public CachedAccessor(IAccessor<TKey, TValue> accessor)
        {
            this.accessor = accessor;
        }

        public long TotalCount { get; private set; }
        public long HitsCount { get; private set; }

        public TValue Read(TKey id)
        {
            TotalCount++;
            if (cache.TryGetValue(id, out var item))
            {
                item.Time = TotalCount;
                return item.Value;
            }

            HitsCount++;
            var data = accessor.Read(id);
            AddItemToCache(id, data);
            return data;
        }

        public void Write(TKey key, TValue value)
        {
            TotalCount++;
            AddItemToCache(key, value);
        }

        public void Flush()
        {
            foreach (var item in cache)
            {
                accessor.Write(item.Key, item.Value.Value);
            }
        }

        private void AddItemToCache(TKey key, TValue value)
        {
            if (cache.Count >= limit)
            {
                var minTime = long.MaxValue;
                TKey minId = default;
                TValue minItem = default;
                foreach (var pair in cache)
                    if (pair.Value.Time < minTime)
                    {
                        minTime = pair.Value.Time;
                        minItem = pair.Value.Value;
                        minId = pair.Key;
                    }

                accessor.Write(minId, minItem);

                cache.Remove(minId);
            }

            cache[key] = new CacheItem<TValue> {Value = value, Time = TotalCount};
        }

        private struct CacheItem<T>
        {
            public long Time;
            public T Value;
        }
    }
}