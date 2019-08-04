using System.Collections.Generic;
using System.Linq;
using OsmReader;

namespace OSM_pbf_convert
{
    public class SWay : IMapObject
    {
        public long Id { get; set; }
        public int WayType { get; set; }
        public IList<WayNode> Nodes { get; set; }
        RelationMemberTypes IMapObject.Type => RelationMemberTypes.Way;

        public int MidLat => (int) Nodes.Average(x => x.Lat);
        public int MidLon => (int) Nodes.Average(x => x.Lon);
        public int Size => 2 + Nodes.Count;

        public BoundingRect Rect => Nodes.Aggregate(new BoundingRect(), (rect, node) =>
        {
            rect.Extend(node.Lat, node.Lon);
            return rect;
        });
    }
}