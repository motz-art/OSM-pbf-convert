using System.Collections.Generic;

namespace OSM_pbf_convert
{
    public class OsmWay
    {
        public long Id { get; set; }
        public IReadOnlyList<long> NodeIds { get; set; }
        public IReadOnlyList<OsmTag> Tags { get; set; }
    }
}