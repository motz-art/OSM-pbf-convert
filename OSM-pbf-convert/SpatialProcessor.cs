using System;
using OsmReader;
using OsmReader.PbfDataObjects;

namespace OSM_pbf_convert
{
    public class SpatialSplitInfo : IDisposable
    {
        public int? SplitValue { get; set; }
        public bool SplitByLatitude { get; set; }
        public SpatialBlock Block { get; set; }
        public SpatialSplitInfo FirstChild { get; set; }
        public SpatialSplitInfo SecondChild { get; set; }

        public override string ToString()
        {
            if (Block != null) return "Block: " + Block;

            return $"{(SplitByLatitude ? "Latitude" : "Longitude")}: {SplitValue}.";
        }

        public void Dispose()
        {
            Block?.Dispose();
            FirstChild?.Dispose();
            SecondChild?.Dispose();
        }
    }

    public class SpatialProcessor : IBlobProcessor<string>
    {
        private readonly SpatialIndex index = new SpatialIndex();
        private long totalCnt;


        public string BlobRead(Blob blob)
        {
            return string.Empty;
        }

        public void ProcessPrimitives(PrimitiveAccessor accessor, string data)
        {
            Console.Write($"Nodes: {totalCnt:#,###}.           ");
            foreach (var node in accessor.Nodes)
            {
                totalCnt++;
                var sNode = new SNode
                {
                    Id = node.Id,
                    Lat = Helpers.CoordAsInt(node.Lat),
                    Lon = Helpers.CoordAsInt(node.Lon)
                };
                index.Add(sNode);
            }
        }

        public void Finish()
        {
            throw new NotImplementedException();
        }
    }
}