using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Text;
using ProtocolBuffers;

namespace OSM_pbf_convert
{
    public class NodesIndexBlobProcessor:
        IBlobProcessor<string>,
        IDisposable
    {
        private readonly IndexNode root = new IndexNode();
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

                var bytes = BitConverter.GetBytes(IPAddress.HostToNetworkOrder(node.Id));
                var indexNode = root;
                for (int i = 3; i < bytes.Length-2; i++)
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
        }

        private void WriteNode(OsmNode node)
        {
            totalCnt++;

            var cLat = CoordAsInt(node.Lat);
            Write7BitEncodedInt(writer, EncodeHelpers.EncodeZigZag(cLat - lat));
            lat = cLat;
            var cLon = CoordAsInt(node.Lon);
            Write7BitEncodedInt(writer, EncodeHelpers.EncodeZigZag(cLon - lon));
            lon = cLon;
            
            var cid = (ulong) (node.Id - id);
            Write7BitEncodedInt(iwriter, cid);
            id = node.Id;

            Write7BitEncodedInt(iwriter, (ulong)(stream.Position - lastPosition));
            lastPosition = stream.Position;
        }

        private int CoordAsInt(double value)
        {
            return (int)(value / 180 * int.MaxValue);
        }

        private void Write7BitEncodedInt(BinaryWriter writer, ulong val)
        {
            var buf = new byte[10];
            var i = buf.Length-1;

            buf[i] = (byte)(val & 0b0111_1111);
            val >>= 7;

            while (val != 0)
            {
                i--;
                buf[i] = (byte) (0b1000_0000 | (val & 0b0111_1111));
                val >>= 7;
            }

            writer.Write(buf, i, buf.Length - i);
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
        public bool IsLatitude { get;set; }
        public int Value { get; set; }
        public Object LeftBlock { get; set; }
        public Object RightBlock { get; set; }
    }

    class MapBlock
    {
        public List<OsmNode> Nodes { get;set; }
    }
}