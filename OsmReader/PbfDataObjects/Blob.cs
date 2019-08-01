using System.IO;

namespace OsmReader.PbfDataObjects
{
    public class Blob
    {
        public BlobHeader Header { get; set; }
        public Stream RawData { get; set; }
        public long RawSize { get; set; }
        public Stream DeflateData { get; set; }
        public Stream LZMAData { get; set; }
        public Stream BZipData { get; set; }
    }
}