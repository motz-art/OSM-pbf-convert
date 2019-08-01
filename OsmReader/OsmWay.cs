using System.Collections.Generic;

namespace OsmReader
{
    public class OsmWay
    {
        public long Id { get; set; }
        public IReadOnlyList<long> NodeIds { get; set; }
        public IReadOnlyList<OsmTag> Tags { get; set; }
    }
}