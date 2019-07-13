using System.Collections.Generic;
using System.IO;
using System.Text;

namespace OSM_pbf_convert
{
    public class BlobIdsInfo
    {
        public long StartPosition { get;set; }
        public long MinNodeId { get; set; }
        public long MaxNodeId { get; set; }
        public int NodesCount { get; set; }
        public int WaysCount {get; set; }
        public int RelationsCount { get; set; }

        public override string ToString()
        {
            return $"{nameof(StartPosition)}: {StartPosition}";
        }

        public static BlobIdsInfo ReadBlobIdsInfo(BinaryReader reader)
        {
            var info = new BlobIdsInfo
            {
                StartPosition = reader.ReadInt64(),
                NodesCount = reader.ReadInt32(),
                MinNodeId = reader.ReadInt64(),
                MaxNodeId = reader.ReadInt64(),
                WaysCount = reader.ReadInt32(),
                RelationsCount = reader.ReadInt32()
            };
            return info;
        }

        public static void WriteBlobInfo(BinaryWriter writer, BlobIdsInfo info)
        {
            writer.Write(info.StartPosition);
            writer.Write(info.NodesCount);
            writer.Write(info.MinNodeId);
            writer.Write(info.MaxNodeId);
            writer.Write(info.WaysCount);
            writer.Write(info.RelationsCount);
        }

        public static void WriteIdsIndex(string fileName, IEnumerable<BlobIdsInfo> infos)
        {
            using (var nodesStream = File.Open(fileName, FileMode.Create))
            {
                var writer = new BinaryWriter(nodesStream, Encoding.Unicode, true);
                foreach (var info in infos) BlobIdsInfo.WriteBlobInfo(writer, info);
            }
        }

        public static IEnumerable<BlobIdsInfo> ReadIdsIndex(string fileName)
        {
            using (var nodesStream = File.Open(fileName, FileMode.Open, FileAccess.Read))
            {
                var reader = new BinaryReader(nodesStream, Encoding.Unicode, true);
                while (nodesStream.Position < nodesStream.Length)
                {
                    yield return ReadBlobIdsInfo(reader);
                }
            }
        }
    }
}