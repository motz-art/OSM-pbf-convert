using System;
using System.IO;
using System.Net;
using System.Text;
using HuffmanCoding;
using ProtocolBuffers;

namespace OSM_pbf_convert
{
    class BlockIndexAccessor : IAccessor<long, MapBlock>
    {
        private readonly Stream stream;

        public BlockIndexAccessor(Stream stream)
        {
            this.stream = stream ?? throw new ArgumentNullException(nameof(stream));
        }

        public MapBlock Read(long key)
        {
            var block = MapBlock.Read(stream, key);
            return block;
        }

        public void Write(long key, MapBlock value)
        {
            if (stream.Length < key)
            {
                stream.SetLength(key);
            }
            value.Write(stream, key);
        }
    }

    public class NodesIndexBlobProcessor:
        IBlobProcessor<string>,
        IDisposable
    {
        private readonly IndexNode root = new IndexNode();

        private readonly SplitBlock mapRoot = new SplitBlock();

        private readonly Stream stream;
        private readonly Stream blockStream;
        private readonly Stream indexStream;
        private readonly BinaryWriter writer;
        private readonly BinaryWriter indexWriter;
        private readonly CachedAccessor<long, MapBlock> mapAccessor;
        
        private int lastLat;
        private int lastLon;
        private long lastPosition;
        private long lastId;
        private long lastOffset;
        private long freeOffset = -1;
        private long totalNodesCount = 0;
        private int splitCount;

        public NodesIndexBlobProcessor(string fileName)
        {
            stream = File.Open(fileName + ".nodes.dat", FileMode.Create);
            writer = new BinaryWriter(stream, Encoding.UTF8, true);
            indexStream = File.Open(fileName + ".idx", FileMode.Create);
            indexWriter = new BinaryWriter(indexStream, Encoding.UTF8, true);
            blockStream = File.Open(fileName + ".mblocks.dat", FileMode.Create, FileAccess.ReadWrite);
            mapAccessor = new CachedAccessor<long, MapBlock>(new BlockIndexAccessor(blockStream));
        }

        public string BlobRead(Blob blob)
        {
            return null;
        }

        public void Finish()
        {
            mapAccessor.Flush();
        }

        public void ProcessPrimitives(PrimitiveAccessor accessor, string data)
        {
            Console.WriteLine($"Nodes: {totalNodesCount}, Total: {mapAccessor.TotalCount}, SplitCount: {splitCount}, HitCount: {mapAccessor.HitsCount}.\r");
            foreach (var node in accessor.Nodes)
            {
                //WriteNode(node);
                totalNodesCount++;

                var lat = CoordAsInt(node.Lat);
                var lon = CoordAsInt(node.Lon);

                AddNode(mapRoot, new MapNode{ Lat = lat, Lon = lon });
                
                AddToIdsIndex(node);
            }
        }
        
        private void AddNode(SplitBlock block, MapNode node, int depth = 0)
        {
            var i = CalcBlockIndex(node, depth);
            if (block.Blocks[i] == null)
            {
                long offset;
                if (freeOffset > 0)
                {
                    offset = freeOffset;
                    freeOffset = -1;
                }
                else
                {
                    lastOffset += MapBlock.BlockSize;
                    offset = lastOffset;
                }

                var mapBlock = new MapBlock { Offset = offset };
                block.Blocks[i] = AddNode(mapBlock, node, depth);
                mapAccessor.Write(mapBlock.Offset, mapBlock);
            } else if (block.Blocks[i] is long blockOffset)
            {
                var b = mapAccessor.Read(blockOffset);
                var obj = AddNode(b, node, depth);
                block.Blocks[i] = obj;
            }
            else if (block.Blocks[i] is SplitBlock s)
            {
                AddNode(s, node, depth + 1);
            }
        }

        private Object AddNode(MapBlock mapBlock, MapNode node, int depth)
        {
            if (mapBlock.Count < mapBlock.Nodes.Length)
            {
                mapBlock.Nodes[mapBlock.Count] = node;
                mapBlock.Count++;
                return mapBlock.Offset;
            }

            // mapBlock.Count = 0;
            freeOffset = mapBlock.Offset;
            mapBlock.Offset = -2; //Removed!
            splitCount++;
            var block = new SplitBlock();
            foreach (var mapNode in mapBlock.Nodes) // ToDo: remove old block from index!
            {
                AddNode(block, mapNode, depth + 1);
            }
            
            AddNode(block, node, depth + 1);

            return block;
        }

        private static int CalcBlockIndex(MapNode node, int depth)
        {
            depth++;
            var lat = node.Lat >> (32 - depth * 4);
            var lon = node.Lon >> (32 - depth * 4);
            var i = ((lat & 0x0F) << 4) | (lon & 0x0F);
            return i;
        }

        private int Split<T>(T[] nodes, Func<T, double> fn)
        {
            var start = 0;
            var end = nodes.Length - 1;

            var mid = nodes.Length / 2;

            while(true)
            {
                var l = start;
                var r = end;

                var val = fn(nodes[(r - l) / 2]);
                while (l < r)
                {
                    while (fn(nodes[l]) < val && l < r) l++;
                    while (fn(nodes[r]) >= val && l < r) r--;
                    if (l < r)
                    {
                        var tmp = nodes[l];
                        nodes[l] = nodes[r];
                        nodes[r] = tmp;
                    }
                }

                if (Math.Abs(l - mid) < 10) return l;
                if (l < mid)
                {
                    start = l + 1;
                }
                else
                {
                    end = l - 1;
                }
            };
        }

        private bool IsLatSplit(MapBlock mapBlock)
        {
            var minLat = mapBlock.Nodes[0].Lat;
            var maxLat = mapBlock.Nodes[0].Lat;
            var minLon = mapBlock.Nodes[0].Lon;
            var maxLon = mapBlock.Nodes[0].Lon;

            foreach (var node in mapBlock.Nodes)
            {
                minLat = Math.Min(minLat, node.Lat);
                maxLat = Math.Max(maxLat, node.Lat);
                minLon = Math.Min(minLon, node.Lon);
                maxLon = Math.Max(maxLon, node.Lon);
            }

            return maxLat - minLat > maxLon - minLon;
        }

        private void AddToIdsIndex(OsmNode node)
        {
            var bytes = BitConverter.GetBytes(IPAddress.HostToNetworkOrder(node.Id));
            var indexNode = root;
            for (int i = 3; i < bytes.Length - 2; i++)
            {
                indexNode.Count++;
                var b = bytes[i];
                if (indexNode.SubNodes[b] == null)
                {
                    indexNode.SubNodes[b] = new IndexNode();
                }

                indexNode = indexNode.SubNodes[b];
            }
        }

        private void WriteNode(OsmNode node)
        {
            var cLat = CoordAsInt(node.Lat);
            StorageHelpers.Write7BitEncodedInt(writer, EncodeHelpers.EncodeZigZag(cLat - lastLat));
            lastLat = cLat;
            var cLon = CoordAsInt(node.Lon);
            StorageHelpers.Write7BitEncodedInt(writer, EncodeHelpers.EncodeZigZag(cLon - lastLon));
            lastLon = cLon;
            
            var cid = (ulong) (node.Id - lastId);
            StorageHelpers.Write7BitEncodedInt(indexWriter, cid);
            lastId = node.Id;

            StorageHelpers.Write7BitEncodedInt(indexWriter, (ulong)(stream.Position - lastPosition));
            lastPosition = stream.Position;
        }

        private int CoordAsInt(double value)
        {
            return (int)(value / 180 * int.MaxValue);
        }

        public void Dispose()
        {
            writer?.Dispose();
            indexWriter?.Dispose();
            stream?.Dispose();
            indexStream?.Dispose();
            blockStream?.Dispose();
        }
    }

    internal class IndexNode
    {
        public long Count { get; set; }
        public IndexNode[] SubNodes { get; } = new IndexNode[256];
    }

    class SplitBlock
    {
        public Object[] Blocks { get; } = new Object[256];
    }

    class MapBlock
    {
        public long Offset { get; set; }
        public int Count { get; set; }
        public MapNode[] Nodes { get; set; } = new MapNode[1000];
        public const long BlockSize = 8 * 1024;

        public void Write(Stream stream, long offset)
        {
            stream.Position = offset;
            using (var writer = new BinaryWriter(stream, Encoding.UTF8, true))
            {
                writer.Write7BitEncodedInt((ulong) Count);
                var lastLat = 0;
                var lastLon = 0;
                for (var i = 0; i < Count; i++)
                {
                    var node = Nodes[i];
                    writer.Write7BitEncodedInt(EncodeHelpers.EncodeZigZag(node.Lat - lastLat));
                    lastLat = node.Lat;

                    writer.Write7BitEncodedInt(EncodeHelpers.EncodeZigZag(node.Lon - lastLon));
                    lastLon = node.Lon;
                }
            }
        }

        public static MapBlock Read(Stream stream, long offset)
        {
            stream.Position = offset;
            using (var reader = new BinaryReader(stream, Encoding.UTF8, true))
            {
                var cnt = (int)reader.Read7BitEncodedInt();

                var res = new MapBlock
                {
                    Offset = offset,
                    Count = cnt
                };

                var lat = 0;
                var lon = 0;
                for (int i = 0; i < cnt; i++)
                {
                    lat += (int)EncodeHelpers.DecodeZigZag(reader.Read7BitEncodedInt());
                    lon += (int)EncodeHelpers.DecodeZigZag(reader.Read7BitEncodedInt());
                    
                    var node = new MapNode
                    {
                        Lat = lat,
                        Lon = lon
                    };

                    res.Nodes[i] = node;
                }

                return res;
            }
        }
    }

    internal class MapNode
    {
        public int Lat { get; set; }
        public int Lon { get; set; }
    }
}