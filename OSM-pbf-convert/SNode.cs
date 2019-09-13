using System.Collections.Generic;
using OsmReader;

namespace OSM_pbf_convert
{
    public class SNode : IMapObject
    {
        public long Id { get; set; }
        public RelationMemberTypes Type => RelationMemberTypes.Node;
        public int MidLat => Lat;
        public int MidLon => Lon;
        public int Lat { get; set; }
        public int Lon { get; set; }
        public List<STagInfo> Tags { get; set; }
    }
}