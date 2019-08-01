using System;
using System.Collections.Generic;
using System.Linq;
using OsmReader.PbfDataObjects;

namespace OsmReader
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

        private static IReadOnlyList<OsmTag> DecodeTags(IReadOnlyList<long> keys, IReadOnlyList<long> values, string[] strings)
        {
            if ((keys == null || keys.Count == 0 ) && (values == null || values.Count == 0)) return Array.Empty<OsmTag>();

            if (strings == null && (keys != null && Enumerable.Any<long>(keys) || values != null && Enumerable.Any<long>(values)))
                throw new InvalidOperationException("Can't decode tags! Strings are missing.");


            if (values == null) throw new InvalidOperationException("Can't decode tags. Values are missing.");

            if (keys == null) throw new InvalidOperationException("Can't decode tags. keys are missing.");

            var results = new OsmTag[keys.Count];
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
                long prevId = 0;
                
                var latitudes = denseNodes.Latitudes;
                long prevLat = 0;

                var longitudes = denseNodes.Longitudes;
                long prevLon = 0;


                var tagIds = denseNodes.KeysValues;
                var tagIndex = 0;

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

                    var tags = new List<OsmTag>();

                    while (tagIds[tagIndex] != 0)
                    {
                        var key = data.Strings[tagIds[tagIndex++]];
                        var value = data.Strings[tagIds[tagIndex++]];
                        tags.Add(new OsmTag(key, value));
                    }

                    tagIndex++;

                    var node = new OsmNode(prevId, lon, lat)
                    {
                        Tags = tags
                    };
                    

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
            if (data.PrimitiveGroup == null) return Enumerable.Empty<OsmRelation>();

            return data.PrimitiveGroup.Where(x => x.Relations != null).SelectMany(x => x.Relations).Select(x => new OsmRelation
            {
                Id = x.Id,
                Items = DecodeRelationItems(x, data),
                Tags = DecodeTags(x.Keys, x.Values, data.Strings)
            });
        }

        private static IReadOnlyList<RelationItem> DecodeRelationItems(Relation relation, PrimitiveBlock data)
        {
            var strings = data.Strings;

            var result = new List<RelationItem>(relation.MemberIds.Count);

            for (int i = 0; i < relation.MemberIds.Count; i++)
            {
                var item = new RelationItem{
                    Id = relation.MemberIds[i],
                    MemberType = relation.MemberType[i],
                    Role = strings[relation.Roles[i]]
                };

                result.Add(item);
            }

            return result;
        }
    }
}