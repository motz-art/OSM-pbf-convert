using System.Collections.Generic;
using System.Linq;
using OsmReader;

namespace OSM_pbf_convert
{
    public class SWay : IMapObject
    {
        public long Id { get; set; }
        public int Type { get; set; }
        public IList<WayNode> Nodes { get; set; }
        public IList<STagInfo> Tags { get; set; }

        RelationMemberTypes IMapObject.ObjectType => RelationMemberTypes.Way;

        public int MidLat => (int) Nodes.Average(x => x.Lat);
        public int MidLon => (int) Nodes.Average(x => x.Lon);

        public BoundingRect Rect => Nodes.Aggregate(new BoundingRect(), (rect, node) =>
        {
            rect.Extend(node.Lat, node.Lon);
            return rect;
        });
    }
}