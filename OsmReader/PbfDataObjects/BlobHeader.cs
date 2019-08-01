using System.IO;

namespace OsmReader.PbfDataObjects
{
    public class BlobHeader
    {
        public string Type { get; set; }
        public Stream IndexData { get; set; }
        public ulong DataSize { get; set; }
        public long StartPosition { get; set; }
    }
}