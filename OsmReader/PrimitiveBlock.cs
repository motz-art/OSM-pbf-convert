using System.Collections.Generic;

namespace OsmReader
{
    public class PrimitiveBlock
    {
        public PrimitiveBlock()
        {
            Granularity = 100;
            LatOffset = 0;
            LonOffset = 0;
            DateGranularity = 1000;
        }

        public string[] Strings { get; set; }
        public List<PrimitiveGroup> PrimitiveGroup { get; set; }
        public int Granularity { get; set; }
        public long LatOffset { get; set; }
        public long LonOffset { get; set; }
        public int DateGranularity { get; set; }
    }
}