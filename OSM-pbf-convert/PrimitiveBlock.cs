using System.Collections.Generic;

namespace OSM_pbf_convert
{
    public class PrimitiveBlock
    {
        public string[] Strings { get; set; }
        public List<PrimitiveGroup> PrimitiveGroup { get; set; }
        public int Granularity { get; set; }
        public long LatOffset { get; set; }
        public long LonOffset { get; set; }
        public int DateGranularity { get; set; }
    }
}