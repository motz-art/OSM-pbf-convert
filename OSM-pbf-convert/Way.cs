using System.Collections.Generic;

namespace OSM_pbf_convert
{
    public class Way
    {
        public long Id { get; set; }
        public List<int> Keys { get; set; }
        public List<int> Values { get; set; }
        public Info Info { get; set; }
        public List<int> Refs { get; set; }
    }
}