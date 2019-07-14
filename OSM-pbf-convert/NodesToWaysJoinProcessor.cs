using HuffmanCoding;
using ProtocolBuffers;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OSM_pbf_convert
{
    public class NodesToWaysJoinProcessor :
        IBlobProcessor<string>,
        IDisposable
    {
        public const long BlockSize = 4096;

        struct NodesOffset
        {
            public long Id { get; set; }
            public byte Offset { get; set; }

            public override string ToString()
            {
                return $"{nameof(Id)}: {Id}, {nameof(Offset)}: {Offset}";
            }
        }

        readonly List<NodesOffset> index = new List<NodesOffset>(1_000_000);

        private readonly Stream indexStream;
        private readonly BinaryWriter indexWriter;

        private readonly Stream stream;
        private readonly BinaryWriter writer;

        private readonly Stream waysStream;
        private readonly BinaryWriter waysWriter;

        private readonly Stopwatch watch = new Stopwatch();

        private List<OsmWay> waysBuf = new List<OsmWay>();
        private HashSet<long> wayBufNodeIds = new HashSet<long>();

        private int totalWayNodes;

        private long lastId;
        private long lastIndexId;
        private int lastLat;
        private int lastLon;

        private long lastReset;

        private long totalNodesCount;

        private bool nodesIndexLoad = false;

        public NodesToWaysJoinProcessor(string fileName, bool canLoad)
        {
            waysStream = File.Open(fileName + ".ways.dat", FileMode.Create);
            waysWriter = new BinaryWriter(waysStream, Encoding.UTF8, true);

            var indexFileName = fileName + ".idx";
            if (canLoad && File.Exists(indexFileName))
            {
                nodesIndexLoad = true;
                stream = File.Open(fileName + ".nodes.dat", FileMode.Open);
                writer = new BinaryWriter(stream, Encoding.UTF8, true);
                indexStream = File.Open(indexFileName, FileMode.Open);
                indexWriter = new BinaryWriter(indexStream, Encoding.UTF8, true);

                LoadIndex();
            }
            else
            {
                stream = File.Open(fileName + ".nodes.dat", FileMode.Create);
                writer = new BinaryWriter(stream, Encoding.UTF8, true);
                indexStream = File.Open(indexFileName, FileMode.Create);
                indexWriter = new BinaryWriter(indexStream, Encoding.UTF8, true);
            }
        }

        public string BlobRead(Blob blob)
        {
            Console.Write($"Offset: {blob.Header.StartPosition}. ");
            
            if (nodesIndexLoad && blob.Header.StartPosition < 1700_000_000)
                return null;

            return string.Empty;
        }

        public void Finish()
        {
        }

        public void ProcessPrimitives(PrimitiveAccessor accessor, string data)
        {
            if (data == null) 
                return;
            watch.Start();
            Console.Write($"Decode: {watch.Elapsed}. Nodes: {totalNodesCount}.\r");

            if (!nodesIndexLoad)
            {
                foreach (var node in accessor.Nodes)
                {
                    watch.Stop();
                    totalNodesCount++;

                    var lat = CoordAsInt(node.Lat);
                    var lon = CoordAsInt(node.Lon);

                    var mNode = new MapNode {Id = node.Id, Lat = lat, Lon = lon};

                    WriteNode(mNode);
                }
            }

            var ways = accessor.Ways.ToList();
            watch.Stop();

            if (ways.Any())
            {
                Console.WriteLine("Way!.");
                writer.Flush();
                stream.Flush();
                indexWriter.Flush();
                indexStream.Flush();

                waysBuf.AddRange(ways);

                foreach (var id in ways.SelectMany(x => x.NodeIds))
                {
                    totalWayNodes++;
                    wayBufNodeIds.Add(id);
                }

                if (totalWayNodes >= 10_000_000)
                {
                    var watch = Stopwatch.StartNew();
                    var nodes = ReadAllNodesById(wayBufNodeIds.OrderBy(x => x)).ToDictionary(x => x.Id);
                    
                    wayBufNodeIds.Clear();
                    totalWayNodes = 0;

                    watch.Stop();
                    Console.WriteLine($"Found: {nodes.Count} withing: {watch.Elapsed}. Speed: {nodes.Count / watch.Elapsed.TotalSeconds}/s");

                    foreach (var osmWay in waysBuf)
                    {
                        var wayNodes = osmWay.NodeIds.Select(x => nodes[x]);
                        WriteWay(osmWay, wayNodes);
                    }

                    waysBuf.Clear();
                }
            }
        }

        private IEnumerable<MapNode> ReadAllNodesById(IEnumerable<long> ids)
        {
            var buf = new byte[BlockSize];
            var lastBlockOffset = -1L;

            var lId = 0L;
            var lLat = 0;
            var lLon = 0;
            var offset = 0;
            ulong v = 0;

            var searchWatch = new Stopwatch();
            var readerWatch = new Stopwatch();
            var reads = 0;
            var seek = 0;

            bool continuation = false;

            foreach (var nodeId in ids)
            {
                searchWatch.Start();
                var blockIndex = FindBlockIndex(nodeId);
                searchWatch.Stop();

                var state = 0;

                do
                {
                    var blockOffset = (blockIndex + 1) * BlockSize;

                    if (lastBlockOffset != blockOffset)
                    {
                        readerWatch.Start();
                        reads++;
                        if (lastBlockOffset + BlockSize != blockOffset)
                        {
                            stream.Position = blockOffset;
                            seek++;
                        }

                        stream.Read(buf, 0, buf.Length);
                        readerWatch.Stop();
                        
                        lastBlockOffset = blockOffset;

                        if (!continuation)
                        {
                            offset = blockIndex >= 0 ? index[blockIndex].Offset : 0;
                            state = 0;
                            lId = 0;
                            lLat = 0;
                            lLon = 0;
                        }
                    }
                    else if (nodeId < lId)
                    {
                        offset = blockIndex >= 0 ? index[blockIndex].Offset : 0;
                        lId = 0;
                        lLat = 0;
                        lLon = 0;
                    }

                    while (state >= 0 && offset < buf.Length)
                    {
                        var b = buf[offset];
                        offset++;
                        v = (v << 7) | (b & 0b0111_1111ul);

                        if ((b & 0b1000_0000) == 0)
                        {
                            switch (state)
                            {
                                case 0:
                                    lId += (long) v;
                                    state = 1;
                                    break;
                                case 1:
                                    lLat += (int) EncodeHelpers.DecodeZigZag(v);
                                    state = 2;
                                    break;
                                case 2:
                                    lLon += (int) EncodeHelpers.DecodeZigZag(v);
                                    state = 0;

                                    if (lId > nodeId)
                                    {
                                        state = -1;
                                    }
                                    if (lId == nodeId)
                                    {
                                        state = -1;
                                        yield return new MapNode
                                        {
                                            Id = lId,
                                            Lat = lLat,
                                            Lon = lLon
                                        };
                                    }

                                    if (continuation)
                                    {
                                        continuation = false;
                                        lId = 0;
                                        lLat = 0;
                                        lLon = 0;
                                    }
                                    break;
                            }

                            v = 0;
                        }

                    }

                    if (state >= 0)
                    {
                        blockIndex++;
                        offset = 0;
                        continuation = true;
                    }
                } while (state >= 0);
            }

            Console.WriteLine($"Search time: ${searchWatch.Elapsed}, Read time: ${readerWatch.Elapsed}. ${reads}/${seek}.");
        }

        private int FindBlockIndex(long nodeId)
        {
            var l = 0;
            var lId = index[l].Id;
            if (nodeId < lId)
            {
                return l - 1;
            }

            var h = index.Count - 1;
            var hId = index[h].Id;
            if (nodeId >= hId)
            {
                return h;
            }

            var cnt = 0;

            while (h - l > 1)
            {
                cnt++;
                var i = (h + l) >> 1;

                var cId = index[i].Id;

                if (nodeId == cId)
                {
                    return i;
                }

                if (nodeId > cId)
                {
                    l = i;
                }
                else if (nodeId < cId)
                {
                    h = i;
                }
            }

            return l;
        }

        public void Dispose()
        {
            writer?.Dispose();
            indexWriter?.Dispose();
            stream?.Dispose();
            indexStream?.Dispose();
            waysWriter?.Dispose();
            waysStream?.Dispose();
        }

        private void LoadIndex()
        {
            index.Clear();
            lastIndexId = 0;
            using (var reader = new BinaryReader(indexStream, Encoding.UTF8, true))
            {
                while (stream.Position < stream.Length)
                {
                    var offset = reader.ReadByte();
                    if (offset == 255)
                    {
                        var position = reader.ReadInt64();
                        //stream.SetLength(position);
                        break;
                    }

                    lastIndexId += (long) reader.Read7BitEncodedInt();

                    index.Add(new NodesOffset
                    {
                        Id = lastIndexId,
                        Offset = offset
                    });
                }
            }
        }

        private void WriteNode(MapNode node)
        {
            if (stream.Position >= lastReset + BlockSize)
            {
                lastReset += BlockSize;
                var offset = (byte)(stream.Position - lastReset);

                index.Add(new NodesOffset
                {
                    Id = node.Id,
                    Offset = offset
                });

                indexWriter.Write(offset);

                var ciid = (ulong)(node.Id - lastIndexId);
                indexWriter.Write7BitEncodedInt(ciid);
                lastIndexId = node.Id;

                lastLat = 0;
                lastLon = 0;
                lastId = 0;
            }

            var cid = (ulong)(node.Id - lastId);
            writer.Write7BitEncodedInt(cid);
            lastId = node.Id;

            var cLat = node.Lat;
            writer.Write7BitEncodedInt(EncodeHelpers.EncodeZigZag(cLat - lastLat));
            lastLat = cLat;

            var cLon = node.Lon;
            writer.Write7BitEncodedInt(EncodeHelpers.EncodeZigZag(cLon - lastLon));
            lastLon = cLon;
        }

        private void WriteWay(OsmWay way, IEnumerable<MapNode> nodes)
        {
            var cid = (ulong) (way.Id - lastId);
            waysWriter.Write7BitEncodedInt(cid);
            lastId = way.Id;

            foreach (var node in nodes)
            {
                var cLat = node.Lat;
                waysWriter.Write7BitEncodedInt(EncodeHelpers.EncodeZigZag(cLat - lastLat));
                lastLat = cLat;

                var cLon = node.Lon;
                waysWriter.Write7BitEncodedInt(EncodeHelpers.EncodeZigZag(cLon - lastLon));
                lastLon = cLon;    
            }
        }

        private int CoordAsInt(double value)
        {
            return (int)(value / 180 * int.MaxValue);
        }
    }
}