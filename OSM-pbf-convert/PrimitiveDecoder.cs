using System;
using System.Collections.Generic;
using System.Linq;

namespace OSM_pbf_convert
{
    public class PrimitiveDecoder
    {
        public static IEnumerable<OsmWay> DecodeWays(PrimitiveBlock data)
        {
            if (data == null || data.PrimitiveGroup == null) yield break;

            foreach (var way in data.PrimitiveGroup.Where(x => x.Ways != null).SelectMany(x => x.Ways))
            {
                var ids = DecodeDeltaItems(way.Refs);
                var tags = DecodeTags(way.Keys, way.Values, data.Strings);
                yield return new OsmWay
                {
                    Id = way.Id,
                    NodeIds = ids,
                    Tags = tags
                };
            }
        }

        private static IReadOnlyList<OsmTag> DecodeTags(long[] keys, long[] values, string[] strings)
        {
            if ((keys == null || keys.Length == 0 ) && (values == null || values.Length == 0)) return Array.Empty<OsmTag>();

            if (strings == null && (keys != null && keys.Any() || values != null && values.Any()))
                throw new InvalidOperationException("Can't decode tags! Strings are missing.");


            if (values == null) throw new InvalidOperationException("Can't decode tags. Values are missing.");

            if (keys == null) throw new InvalidOperationException("Can't decode tags. keys are missing.");

            var results = new OsmTag[keys.Length];
            for (var i = 0; i < results.Length; i++)
            {
                var key = strings[keys[i]];
                var value = strings[values[i]];

                results[i] = new OsmTag
                {
                    Key = key,
                    Value = value
                };
            }

            return results;
        }

        private static long[] DecodeDeltaItems(long[] items)
        {
            var result = new long[items.Length];
            long id = 0;
            for (var i = 0; i < items.Length; i++)
            {
                id += items[i];
                result[i] = id;
            }

            return result;
        }

        public static IEnumerable<OsmNode> DecodeDenseNodes(PrimitiveBlock data)
        {
            if (data == null || data.PrimitiveGroup == null) yield break;
            foreach (var denseNodes in data.PrimitiveGroup.Select(group => group.DenseNodes)
                .Where(denseNodes => denseNodes != null))
            {
                var ids = denseNodes.Ids;
                var latitudes = denseNodes.Latitudes;
                var longitudes = denseNodes.Longitudes;
                long prevId = 0;
                long prevLat = 0;
                long prevLon = 0;

                if (ids.Count != latitudes.Count || ids.Count != longitudes.Count)
                    throw new InvalidOperationException(
                        "Dense node should have equal couont of Ids, Longitudes and Latitudes");

                for (var i = 0; i < ids.Count; i++)
                {
                    prevId += ids[i];
                    prevLon += longitudes[i];
                    prevLat += latitudes[i];

                    var lon = 0.000000001 * (data.LonOffset + data.Granularity * prevLon);
                    var lat = 0.000000001 * (data.LatOffset + data.Granularity * prevLat);

                    var node = new OsmNode(prevId, lon, lat);

                    yield return node;
                }
            }
        }

        public static IEnumerable<OsmNode> DecodeAllNodes(PrimitiveBlock data)
        {
            if (data.PrimitiveGroup == null) return Enumerable.Empty<OsmNode>();

            if (data.PrimitiveGroup.Any(g => g.Nodes != null && g.Nodes.Any()))
                throw new NotImplementedException("Reading of plain nodes is not implemented. Only dense nodes are supported.");

            return DecodeDenseNodes(data);
        }

        public static IEnumerable<OsmRelation> DecodeRelations(PrimitiveBlock data)
        {
            return data.PrimitiveGroup.SelectMany(x => x.Relations).Select(x => new OsmRelation
            {
                Id = x.Id
            });
        }
    }
}