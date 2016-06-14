using System.Collections.Generic;

namespace OSM_pbf_convert
{
    public class Node
    {
        public long Id { get; set; }
        public List<int> Keys { get; set; }
        public List<int> Values { get; set; }
        public Info Info { get; set; }
        public long Lat { get; set; }
        public long Lon { get; set; }
    }
}