namespace OSM_pbf_convert
{
    public struct WayNode
    {
        public WayNode(ulong id, int lat, int lon)
        {
            Id = id;
            Lat = lat;
            Lon = lon;
        }

        public ulong Id { get; set; }
        public int Lat { get; set; }
        public int Lon { get; set; }
    }
}