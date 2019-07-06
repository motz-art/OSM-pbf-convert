using System.Collections.Generic;

namespace OSM_pbf_convert
{
    public class CachedReader
    {
        public long TotalCount { get; private set; } = 0;
        public long HitsCount { get; private set; } = 0;
        private readonly int limit = 20;
        private readonly Dictionary<long, long> cache = new Dictionary<long, long>();

        public void Get(long id)
        {
            TotalCount++;
            if (!cache.ContainsKey(id))
            {
                HitsCount++;
                if (cache.Count >= limit)
                {
                    long minTime = long.MaxValue;
                    long minId = -1;
                    foreach (var pair in cache)
                    {
                        if (pair.Value < minTime)
                        {
                            minTime = pair.Value;
                            minId = pair.Key;
                        }
                    }

                    cache.Remove(minId);
                }
            }

            cache[id] = TotalCount;
        }
    }
}