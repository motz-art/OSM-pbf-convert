using System.Collections.Generic;

namespace OSM_pbf_convert
{
    public class DenseNodes
    {
        public DenseNodes()
        {
            Ids = new List<long>();
            Latitudes = new List<long>();
            Longitudes = new List<long>();
            KeysValues = new List<int>();
        }

        public List<long> Ids { get; set; }
        public DenseInfo DenseInfo { get; set; }
        public List<long> Latitudes { get; set; }
        public List<long> Longitudes { get; set; }
        public List<int> KeysValues { get; set; }
    }
}