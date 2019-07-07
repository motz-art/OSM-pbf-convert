using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Text;
using HuffmanCoding;
using ProtocolBuffers;

namespace OSM_pbf_convert
{
    public class NodesIndexBlobProcessor:
        IBlobProcessor<string>,
        IDisposable
    {
        private readonly IndexNode root = new IndexNode();

        private SplitBlock mapRoot = new SplitBlock();

        private Stream stream;
        private Stream istream;
        private BinaryWriter writer;
        private BinaryWriter iwriter;
        
        private int lat = 0;
        private int lon = 0;
        private long lastPosition = 0;
        private long id = 0;
        private long totalCnt = 0;

        public NodesIndexBlobProcessor(string fileName)
        {
            this.stream = File.Open(fileName, FileMode.Create);
            this.writer = new BinaryWriter(stream, Encoding.UTF8, true);
            istream = File.Open(fileName + ".idx", FileMode.Create);
            iwriter = new BinaryWriter(istream, Encoding.UTF8, true);
        }

        public string BlobRead(Blob blob)
        {
            return null;
        }

        public void Finish()
        {
        }

        public void ProcessPrimitives(PrimitiveAccessor accessor, string data)
        {
            foreach (var node in accessor.Nodes)
            {
                //WriteNode(node);

                var lat = CoordAsInt(node.Lat);
                var lon = CoordAsInt(node.Lon);

                AddNode(mapRoot, new MapNode{ Lat = lat, Lon = lon });
                
                AddToIdsIndex(node);
            }
        }
        
        private void AddNode(SplitBlock block, MapNode node, int depth = 0)
        {
            var lat = (int)BitConverter.GetBytes(IPAddress.HostToNetworkOrder(node.Lat))[depth];
            var lon = (int)BitConverter.GetBytes(IPAddress.HostToNetworkOrder(node.Lon))[depth];
            var i = (lat << 8) | lon;
            if (block.Blocks[i] == null)
            {
                block.Blocks[i] = AddNode(new MapBlock(), node, depth);
            } else if (block.Blocks[i] is MapBlock b)
            {
                block.Blocks[i] = AddNode(b, node, depth);
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
                return mapBlock;
            }

            var block = new SplitBlock();
            foreach (var mapNode in mapBlock.Nodes)
            {
                AddNode(block, node, depth + 1);
            }

            return block;
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
            totalCnt++;

            var cLat = CoordAsInt(node.Lat);
            StorageHelpers.Write7BitEncodedInt(writer, EncodeHelpers.EncodeZigZag(cLat - lat));
            lat = cLat;
            var cLon = CoordAsInt(node.Lon);
            StorageHelpers.Write7BitEncodedInt(writer, EncodeHelpers.EncodeZigZag(cLon - lon));
            lon = cLon;
            
            var cid = (ulong) (node.Id - id);
            StorageHelpers.Write7BitEncodedInt(iwriter, cid);
            id = node.Id;

            StorageHelpers.Write7BitEncodedInt(iwriter, (ulong)(stream.Position - lastPosition));
            lastPosition = stream.Position;
        }

        private int CoordAsInt(double value)
        {
            return (int)(value / 180 * int.MaxValue);
        }

        public void Dispose()
        {
            writer?.Dispose();
            iwriter?.Dispose();
            stream?.Dispose();
            istream?.Dispose();
        }
    }

    internal class IndexNode
    {
        public long Count { get; set; }
        public IndexNode[] SubNodes { get; } = new IndexNode[256];
    }

    class SplitBlock
    {
        public Object[] Blocks { get; } = new object[65536];
    }

    class MapBlock
    {
        public int Count { get; set; }
        public MapNode[] Nodes { get; set; } = new MapNode[1000];

        public void Write(Stream stream, long offset)
        {
            stream.Position = offset;
            using (var writer = new BinaryWriter(stream, Encoding.UTF8, true))
            {
                writer.Write7BitEncodedInt((ulong) Count);
                foreach (var node in Nodes)
                {
                    writer.Write7BitEncodedInt((ulong) node.Lat);
                    writer.Write7BitEncodedInt((ulong) node.Lon);
                }
            }
        }

        public static MapBlock Read(Stream stream, long offset)
        {
            stream.Position = offset;
            using (var reader = new BinaryReader(stream, Encoding.UTF8, true))
            {
                var cnt = (int)reader.Read7BitEncodedInt();

                return new MapBlock
                {
                    Count = cnt
                };
            }
        }
    }

    internal class MapNode
    {
        public int Lat { get; set; }
        public int Lon { get; set; }
    }
}