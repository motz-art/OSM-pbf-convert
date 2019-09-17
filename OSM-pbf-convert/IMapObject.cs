using OsmReader;

namespace OSM_pbf_convert
{
    public interface IMapObject
    {
        long Id { get; }
        RelationMemberTypes ObjectType { get; }

        int MidLat { get; }
        int MidLon { get; }
    }
}