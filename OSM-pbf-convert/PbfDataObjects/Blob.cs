using System.IO;

namespace OSM_pbf_convert
{
    public class Blob
    {
        public Stream RawData { get; set; }
        public long RawSize { get; set; }
        public Stream DeflateData { get; set; }
        public Stream LZMAData { get; set; }
        public Stream BZipData { get; set; }
    }
}