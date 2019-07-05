using System.Collections.Generic;

namespace OSM_pbf_convert
{
    public class Way
    {
        public long Id { get; set; }
        public long[] Keys { get; set; }
        public long[] Values { get; set; }
        public Info Info { get; set; }
        public long[] Refs { get; set; }
    }
}