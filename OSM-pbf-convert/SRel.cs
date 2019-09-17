using System.Collections.Generic;
using OsmReader;

namespace OSM_pbf_convert
{
    public class SRel : IMapObject
    {
        public long Id { get; set; }

        public RelationMemberTypes ItemType { get; set; }
        public long ItemId { get; set; }

        public int RelType { get; set; }

        public RelationMemberTypes ObjectType => RelationMemberTypes.Relation;
        public int MidLat { get; set; }
        public int MidLon { get; set; }
    }
}