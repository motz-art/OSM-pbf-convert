using System.IO;
using System.Text;
using HuffmanCoding;
using ProtocolBuffers;

namespace OSM_pbf_convert
{
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
}