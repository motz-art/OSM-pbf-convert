using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using OsmReader;
using OsmReader.PbfDataObjects;

namespace OSM_pbf_convert
{
    public class NodesIndexBlobProcessor :
        IBlobProcessor<string>,
        IDisposable
    {
        private const int BufferLimit = 10_000_000;
        private readonly List<MapNode> buffer = new List<MapNode>(BufferLimit);
        private readonly List<List<int>> fileIds = new List<List<int>>();
        private readonly List<Task> mergeTasks = new List<Task>();
        private readonly string fileName;

        private int indexId;

        private long totalNodesCount;

        public NodesIndexBlobProcessor(string fileName)
        {
            this.fileName = fileName;
        }

        public string BlobRead(Blob blob)
        {
            return null;
        }

        public void Finish()
        {
            WriteIndexItems();
            // ToDo: Merge all;

            List<int> ids = new List<int>();
            int level;
            int resultId;
            for (level = 0; level < fileIds.Count; level++)
            {
                if (mergeTasks.Count > level && mergeTasks[level] != null)
                {
                    mergeTasks[level].Wait();
                }

                var files = fileIds[level];
                lock (files)
                {
                    ids.AddRange(files);
                    files.Clear();
                }

                if (ids.Count >= 4)
                {
                    resultId = Interlocked.Increment(ref indexId);
                    Merge(level, ids.ToArray(), resultId);
                    ids.Clear();
                }
            }
            resultId = Interlocked.Increment(ref indexId);
            Merge(level, ids.ToArray(), resultId);
        }

        public void ProcessPrimitives(PrimitiveAccessor accessor, string data)
        {
            Console.Write($"Nodes: {totalNodesCount}.\r");

            foreach (var node in accessor.Nodes)
            {
                totalNodesCount++;

                var lat = Helpers.CoordAsInt(node.Lat);
                var lon = Helpers.CoordAsInt(node.Lon);

                var mNode = new MapNode { Id = node.Id, Lat = lat, Lon = lon };

                AddToSort(mNode);
            }
        }

        public void Dispose()
        {
        }

        private void AddToSort(MapNode mNode)
        {
            buffer.Add(mNode);

            if (buffer.Count >= BufferLimit)
            {
                WriteIndexItems();
            }
        }

        private void WriteIndexItems()
        {
            var buf = buffer.Select(x => x.BlockIndex).ToArray();

            buffer.Clear();

            var id = Interlocked.Increment(ref indexId);

            Task.Run(() => WriteBuf(buf, id));
        }

        private string GetName(int id)
        {
            return $"{fileName}.sort{id}.tmp";
        }

        private void WriteBuf(ulong[] buf, int id)
        {
            Array.Sort(buf);

            var name = GetName(id);

            SpatialData.WriteSequence(buf, name);

            FileWritten(id, 0);
        }

        private void FileWritten(int id, int i)
        {
            int[] ids = null;

            List<int> level;
            lock (fileIds)
            {
                while (i >= fileIds.Count)
                {
                    fileIds.Add(new List<int>());
                }
                level = fileIds[i];
            }

            lock (level)
            {
                level.Add(id);
                if (level.Count >= 4)
                {
                    ids = level.ToArray();
                    level.Clear();
                }
            }

            if (ids != null)
            {
                var resultId = Interlocked.Increment(ref indexId);
                while (mergeTasks.Count <= i)
                {
                    mergeTasks.Add(null);
                }

                if (mergeTasks[i] != null)
                {
                    mergeTasks[i].Wait();
                }
                mergeTasks[i] = Task.Run(() => Merge(i + 1, ids, resultId));
            }
        }

        private void Merge(int level, int[] fileIds, int resultFileId)
        {
            var source = CreateMergedSource(fileIds);

            SpatialData.WriteSequence(source, GetName(resultFileId));

            FileWritten(resultFileId, level);
            foreach (var fileId in fileIds)
            {
                File.Delete(GetName(fileId));
            }
        }

        private IEnumerable<ulong> CreateMergedSource(int[] fileIds)
        {
            var sources = fileIds.Select(id => SpatialData.ReadSequence(GetName(id))).ToList();

            while (sources.Count > 1)
            {
                var newSources = new List<IEnumerable<ulong>>();
                for (var i = 0; i < sources.Count; i += 2)
                {
                    if (i + 1 < sources.Count)
                    {
                        newSources.Add(MergeSorted(sources[i], sources[i + 1]));
                    }
                    else
                    {
                        newSources.Add(sources[i]);
                    }
                }

                sources = newSources;
            }

            return sources[0];
        }

        private static IEnumerable<ulong> MergeSorted(IEnumerable<ulong> sourceA, IEnumerable<ulong> sourceB)
        {
            using (var a = sourceA.GetEnumerator())
            {
                using (var b = sourceB.GetEnumerator())
                {
                    var hasA = a.MoveNext();
                    var hasB = b.MoveNext();

                    while (hasA && hasB)
                    {
                        if (a.Current < b.Current)
                        {
                            yield return a.Current;
                            hasA = a.MoveNext();
                        }
                        else
                        {
                            yield return b.Current;
                            hasB = b.MoveNext();
                        }
                    }

                    while (hasA)
                    {
                        yield return a.Current;
                        hasA = a.MoveNext();
                    }

                    while (hasB)
                    {
                        yield return b.Current;
                        hasB = b.MoveNext();
                    }
                }
            }
        }
    }

    internal class MapIndexComparer : IComparer<MapNode>
    {
        public int Compare(MapNode x, MapNode y)
        {
            var a = x.BlockIndex;
            var b = y.BlockIndex;
            if (a < b)
            {
                return -1;
            }

            if (a > b)
            {
                return 1;
            }

            return 0;
        }
    }

    public class MapNode
    {
        public long Id { get; set; }
        public int Lat { get; set; }
        public int Lon { get; set; }

        public ulong BlockIndex => CalcBlockIndex(Lat, Lon);

        public static ulong CalcBlockIndex(double latitude, double longitude)
        {
            var lat = Helpers.CoordAsInt(latitude);
            var lon = Helpers.CoordAsInt(longitude);
            return CalcBlockIndex(lat, lon);
        }

        public static ulong CalcBlockIndex(int latitude, int longitude)
        {
            ulong res = 0;
            ulong mask = 1;
            var lat = (ulong)latitude << 1;
            var lon = (ulong)longitude;
            for (var i = 0; i < 32; i++)
            {
                res |= lon & mask;
                lon <<= 1;
                mask <<= 1;


                res |= lat & mask;
                lat <<= 1;
                mask <<= 1;
            }

            return res;

        }
    }
}
