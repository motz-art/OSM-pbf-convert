using System.Collections.Generic;

namespace OsmReader.PbfDataObjects
{
    public class Node
    {
        public long Id { get; set; }
        public List<uint> Keys { get; set; }
        public List<uint> Values { get; set; }
        public Info Info { get; set; }
        public long Lat { get; set; }
        public long Lon { get; set; }
    }
}