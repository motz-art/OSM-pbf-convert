using System.Collections.Generic;

namespace OSM_pbf_convert
{
    public class OsmRelation
    {
        public long Id { get; set; }
        public IReadOnlyList<OsmTag> Tags { get; set; }
    }
}