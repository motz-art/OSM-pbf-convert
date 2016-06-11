using System.IO;

namespace OSM_pbf_convert
{
    public class BlobHeader
    {
        public string Type { get; set; }
        public Stream IndexData { get; set; }
        public ulong DataSize { get; set; }
    }
}