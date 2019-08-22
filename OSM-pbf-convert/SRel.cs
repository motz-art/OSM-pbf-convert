using System.Collections.Generic;
using OsmReader;

namespace OSM_pbf_convert
{
    public class SRel : IMapObject
    {
        public long Id { get; }
        public IList<STagInfo> Tags { get; set; }

        public RelationMemberTypes Type => RelationMemberTypes.Relation;
        public int MidLat { get; }
        public int MidLon { get; }
        public int Size { get; }
        public BoundingRect Rect { get; }
    }
}