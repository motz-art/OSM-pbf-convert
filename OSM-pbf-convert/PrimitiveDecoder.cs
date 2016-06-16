using System;
using System.Collections.Generic;
using System.Linq;

namespace OSM_pbf_convert
{
    public class PrimitiveDecoder
    {
        public static IEnumerable<OsmNode> DecodeDenseNodes(PrimitiveBlock data)
        {
            if (data == null || data.PrimitiveGroup == null)
            {
                yield break;
            }
            foreach (var denseNodes in data.PrimitiveGroup.Select(group => group.DenseNodes).Where(denseNodes => denseNodes != null))
            {
                var ids = denseNodes.Ids;
                var latitudes = denseNodes.Latitudes;
                var longitudes = denseNodes.Longitudes;
                long prevId = 0;
                long prevLat = 0;
                long prevLon = 0;

                if (ids.Count != latitudes.Count || ids.Count != longitudes.Count)
                {
                    throw new InvalidOperationException("Dense node should have equal couont of Ids, Longitudes and Latitudes");
                }

                for (int i = 0; i < ids.Count; i++)
                {
                    prevId += ids[i];
                    prevLon += longitudes[i];
                    prevLat += latitudes[i];

                    var lon = 0.000000001 * (data.LonOffset + (data.Granularity * prevLon));
                    var lat = 0.000000001 * (data.LatOffset + (data.Granularity * prevLat));

                    var node = new OsmNode(prevId, lon, lat);

                    yield return node;
                }

            }
        }
    }
}