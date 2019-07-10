using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using HuffmanCoding;
using ProtocolBuffers;

namespace OSM_pbf_convert
{
    public class NodesIndexBlobProcessor :
        IBlobProcessor<string>,
        IDisposable
    {
        private const int BufferLimit = 1_000_000;
        private readonly List<MapNode> buffer = new List<MapNode>(BufferLimit);
        private readonly List<int> fileIds = new List<int>();
        private readonly string fileName;

        private readonly Stream indexStream;
        private readonly BinaryWriter indexWriter;
        private readonly object locker = new object();


        private readonly Stream stream;
        private readonly BinaryWriter writer;
        private int indexId;

        private long lastId;
        private int lastLat;
        private int lastLon;
        private long lastPosition;
        private long totalNodesCount;

        public NodesIndexBlobProcessor(string fileName)
        {
            this.fileName = fileName;

            stream = File.Open(fileName + ".nodes.dat", FileMode.Create);
            writer = new BinaryWriter(stream, Encoding.UTF8, true);
            indexStream = File.Open(fileName + ".idx", FileMode.Create);
            indexWriter = new BinaryWriter(indexStream, Encoding.UTF8, true);
        }

        public string BlobRead(Blob blob)
        {
            return null;
        }

        public void Finish()
        {
            WriteIndexItems();

            lock (fileIds)
            {
                indexId++;
                Merge(fileIds.ToArray(), indexId);
            }
        }

        public void ProcessPrimitives(PrimitiveAccessor accessor, string data)
        {
            Console.Write($"Nodes: {totalNodesCount}.\r");

            foreach (var node in accessor.Nodes)
            {
                totalNodesCount++;

                var lat = CoordAsInt(node.Lat);
                var lon = CoordAsInt(node.Lon);

                var mNode = new MapNode {Id = node.Id, Lat = lat, Lon = lon};

                WriteNode(mNode);

                AddToSort(mNode);
            }
        }

        public void Dispose()
        {
            writer?.Dispose();
            indexWriter?.Dispose();
            stream?.Dispose();
            indexStream?.Dispose();
        }

        private void AddToSort(MapNode mNode)
        {
            buffer.Add(mNode);

            if (buffer.Count >= BufferLimit) WriteIndexItems();
        }

        private void WriteIndexItems()
        {
            var buf = buffer.Select(x => x.BlockIndex).ToArray();

            buffer.Clear();

            int id;
            lock (fileIds)
            {
                id = ++indexId;
                fileIds.Add(id);
            }

            var name = GetName(id);
            Task.Run(() => WriteBuf(buf, name));
        }

        private string GetName(int id)
        {
            return $"{fileName}.sort{id}.tmp";
        }

        private void WriteBuf(ulong[] buf, string name)
        {
            Array.Sort(buf);

            WriteSequence(buf, name);

            Task.Run(() => MergeFilesIfNeeded());
        }

        private void MergeFilesIfNeeded()
        {
            int[] ids = null;
            var resultId = 0;
            lock (fileIds)
            {
                if (fileIds.Count >= 4)
                {
                    resultId = ++indexId;
                    ids = fileIds.ToArray();
                    fileIds.Clear();
                    fileIds.Add(resultId);
                }
            }

            if (ids != null) Merge(ids, resultId);
        }

        private static void WriteSequence(IEnumerable<ulong> buf, string name)
        {
            var watch = Stopwatch.StartNew();
            ulong cnt = 0;
            using (var stream = File.Create(name))
            {
                using (var writer = new BufByteWriter(stream))
                {
                    ulong lastIndex = 0;

                    foreach (var index in buf)
                    {
                        cnt++;

                        writer.Write7BitEncodedInt(index - lastIndex);

                        lastIndex = index;
                    }
                }
            }

            watch.Stop();
            Console.WriteLine(
                $"Written: {cnt}, in {watch.Elapsed}, speed: {cnt / watch.Elapsed.TotalSeconds / 1000} k/s.");
        }

        private void Merge(int[] fileIds, int resultFileId)
        {
            lock (locker)
            {
                var sources = fileIds.Select(id => ReadSequence(GetName(id))).ToList();

                while (sources.Count > 1)
                {
                    var newSources = new List<IEnumerable<ulong>>();
                    for (var i = 0; i < sources.Count; i += 2)
                        if (i + 1 < sources.Count)
                            newSources.Add(MergeSorted(sources[i], sources[i + 1]));
                        else
                            newSources.Add(sources[i]);

                    sources = newSources;
                }

                WriteSequence(sources[0], GetName(resultFileId));

                foreach (var fileId in fileIds) File.Delete(GetName(fileId));
            }
        }

        private IEnumerable<ulong> MergeSorted(IEnumerable<ulong> sourceA, IEnumerable<ulong> sourceB)
        {
            using (var a = sourceA.GetEnumerator())
            {
                using (var b = sourceB.GetEnumerator())
                {
                    var hasA = a.MoveNext();
                    var hasB = b.MoveNext();

                    while (hasA && hasB)
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

        private IEnumerable<ulong> ReadSequence(string name)
        {
            using (var stream = File.OpenRead(name))
            {
                ulong last = 0;
                var reader = new BufByteReader(stream);
                while (reader.CanRead())
                {
                    last += reader.Read7BitEncodedInt();
                    yield return last;
                }
            }
        }

        private void WriteNode(MapNode node)
        {
            var cLat = node.Lat;
            writer.Write7BitEncodedInt(EncodeHelpers.EncodeZigZag(cLat - lastLat));
            lastLat = cLat;
            var cLon = node.Lon;
            writer.Write7BitEncodedInt(EncodeHelpers.EncodeZigZag(cLon - lastLon));
            lastLon = cLon;

            var cid = (ulong) (node.Id - lastId);
            indexWriter.Write7BitEncodedInt(cid);
            lastId = node.Id;

            indexWriter.Write7BitEncodedInt((ulong) (stream.Position - lastPosition));
            lastPosition = stream.Position;
        }

        private int CoordAsInt(double value)
        {
            return (int) (value / 180 * int.MaxValue);
        }
    }

    internal class MapIndexComparer : IComparer<MapNode>
    {
        public int Compare(MapNode x, MapNode y)
        {
            var a = x.BlockIndex;
            var b = y.BlockIndex;
            if (a < b) return -1;
            if (a > b) return 1;
            return 0;
        }
    }

    internal class MapNode
    {
        public long Id { get; set; }
        public int Lat { get; set; }
        public int Lon { get; set; }

        public ulong BlockIndex
        {
            get
            {
                ulong res = 0;
                ulong mask = 1;
                var lat = (ulong) Lat << 1;
                var lon = (ulong) Lon;
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
}