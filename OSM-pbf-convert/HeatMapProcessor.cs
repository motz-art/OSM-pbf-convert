﻿using System.IO;
using System.Text;
using HuffmanCoding;
using OsmReader;
using OsmReader.PbfDataObjects;

namespace OSM_pbf_convert
{
    public class HeatMapProcessor : IBlobProcessor<string>
    {
        private readonly string file;
        private int[,] map = new int[16384, 32768];

        public HeatMapProcessor(string file)
        {
            this.file = file;
        }

        public string BlobRead(Blob blob)
        {
            return null;
        }

        public void ProcessPrimitives(PrimitiveAccessor accessor, string data)
        {
            foreach (var node in accessor.Nodes)
            {
                var lat = (uint)(Helpers.CoordAsInt(node.Lat) + int.MaxValue) >> 18;
                var lon = (uint)(Helpers.CoordAsInt(node.Lon) + int.MaxValue) >> 17;

                map[lat, lon]++;
            }
        }

        public void Finish()
        {
            using (var stream = File.OpenWrite(file + ".heat.map"))
            {
                using (var writer = new BinaryWriter(stream, Encoding.UTF8, true))
                {
                    foreach (var i in map)
                    {
                        writer.Write7BitEncodedInt((ulong)i);
                    }
                }
            }
        }
    }
}