using System.IO;

namespace OSM_pbf_convert
{
    public class Blob
    {
        public BlobTypes Type { get; set; }
        public long RawSize { get; set; }
        public Stream Data { get; set; }
    }
}