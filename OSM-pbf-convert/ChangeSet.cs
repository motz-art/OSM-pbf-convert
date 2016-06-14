using System.Collections.Generic;

namespace OSM_pbf_convert
{
    public class ChangeSet
    {
        public long Id { get; set; }
        public List<int> Keys { get; set; }
        public List<int> Values { get; set; }
        public Info Info { get; set; }
        public long CreatedAt { get; set; }
        public long CloseTimeDelta { get; set; }
        public bool Open { get; set; }
        public BoundBox BBox { get;set; }
    }
}