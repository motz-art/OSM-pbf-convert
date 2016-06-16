namespace OSM_pbf_convert
{
    public class OsmNode
    {
        public long Id { get; set; }
        public double Lat { get; set; }
        public double Lon { get; set; }

        public OsmNode(long id, double lon, double lat)
        {
            Id = id;
            Lon = lon;
            Lat = lat;
        }
    }
}