using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using OsmReader;
using OsmReader.PbfDataObjects;

namespace OSM_pbf_convert
{
    public class NodesToWaysJoinProcessor :
        IBlobProcessor<string>,
        IDisposable
    {
        private readonly SpatialIndex spatialIndex = new SpatialIndex();
        private readonly NodesIndex nodesIndex;
        private readonly WaysDataFile waysDataFile;

        private readonly Stopwatch watch = new Stopwatch();

        private readonly List<OsmWay> waysBuf = new List<OsmWay>();
        private readonly HashSet<long> wayBufNodeIds = new HashSet<long>();

        private int totalWayNodes;


        private long totalNodesCount;
        private long maxDiff;
        private long maxDiffId;

        public NodesToWaysJoinProcessor(string fileName, bool canLoad)
        {
            waysDataFile = new WaysDataFile(fileName + ".ways.dat");
            nodesIndex = new NodesIndex(fileName, canLoad);
        }

        public string BlobRead(Blob blob)
        {
            return string.Empty;
        }

        public void Finish()
        {
            MergeAndFlushWays();

            waysDataFile.Flush();
            spatialIndex.Finish();
        }

        public void ProcessPrimitives(PrimitiveAccessor accessor, string data)
        {
            if (data == null) return;
            if (accessor == null) throw new ArgumentNullException(nameof(accessor));

            watch.Start();
            Console.Write($"Decode: {watch.Elapsed}. Nodes: {totalNodesCount}.\r");

            ProcessNodes(accessor);
            ProcessWays(accessor);
        }

        private void ProcessNodes(PrimitiveAccessor accessor)
        {
            foreach (var node in accessor.Nodes)
            {
                watch.Stop();
                totalNodesCount++;

                var lat = Helpers.CoordAsInt(node.Lat);
                var lon = Helpers.CoordAsInt(node.Lon);

                var mNode = new MapNode { Id = node.Id, Lat = lat, Lon = lon };

                nodesIndex.WriteNode(mNode);
                var sNode = new SNode
                {
                    Id = mNode.Id,
                    Lat = mNode.Lat,
                    Lon = mNode.Lon,
                    Tags = ConvertTags(node.Tags)
                };
                spatialIndex.Add(sNode);
            }
        }

        private List<STagInfo> ConvertTags(IReadOnlyList<OsmTag> nodeTags)
        {
            var result = new List<STagInfo>();

            return result;
        }

        private void ProcessWays(PrimitiveAccessor accessor)
        {
            var ways = accessor.Ways.ToList();
            watch.Stop();

            if (ways.Any())
            {
                Console.Write($"Way! {totalWayNodes:#,###}. ");
                nodesIndex.Flush();

                waysBuf.AddRange(ways);

                foreach (var id in ways.SelectMany(x => x.NodeIds))
                {
                    totalWayNodes++;
                    wayBufNodeIds.Add(id);
                }

                if (totalWayNodes >= 10_000_000)
                {
                    MergeAndFlushWays();
                }
            }
        }

        private void MergeAndFlushWays()
        {
            var mergeWatch = Stopwatch.StartNew();
            var nodes = nodesIndex.ReadAllNodesById(wayBufNodeIds.OrderBy(x => x)).ToDictionary(x => x.Id);

            wayBufNodeIds.Clear();
            totalWayNodes = 0;

            mergeWatch.Stop();
            Console.WriteLine(
                $"Found: {nodes.Count} withing: {mergeWatch.Elapsed}. Speed: {nodes.Count / mergeWatch.Elapsed.TotalSeconds}/s");

            foreach (var osmWay in waysBuf)
            {
                var wayNodes = osmWay.NodeIds.Select(x => nodes[x]).ToList();
                for (var i = 1; i < wayNodes.Count; i++)
                {
                    var diff = Math.Abs(wayNodes[i].Lat - wayNodes[i - 1].Lat);
                    if (diff > maxDiff)
                    {
                        maxDiff = diff;
                        maxDiffId = osmWay.Id;
                    }
                    diff = Math.Abs(wayNodes[i].Lat - wayNodes[i - 1].Lat);
                    if (diff > maxDiff)
                    {
                        maxDiff = diff;
                        maxDiffId = osmWay.Id;
                    }
                }
                WriteWayToIndex(osmWay, wayNodes);
            }

            Console.WriteLine($"Max Diffs: {maxDiff:0,000}, ID: {maxDiffId:0,000}");
            waysBuf.Clear();
        }
        public void Dispose()
        {
            nodesIndex?.Dispose();
            waysDataFile?.Dispose();
        }

        private void WriteWayToIndex(OsmWay way, IReadOnlyList<MapNode> nodes)
        {
            var sway = CreateSWay(way, nodes);

            waysDataFile.WriteWay(sway);

            spatialIndex.Add(sway);
        }

        private SWay CreateSWay(OsmWay way, IReadOnlyList<MapNode> nodes)
        {
            var sway = new SWay
            {
                Id = way.Id,
                WayType = GetWayType(way),
                Nodes = nodes.Select(x => new WayNode((ulong) x.Id, x.Lat, x.Lon)).ToList()
            };
            return sway;
        }

        private int GetWayType(OsmWay way)
        {
            // ToDo: extract to separate file with config file.
            if (way.Tags == null)
            {
                return 1;
            }
            var tag = way.Tags.FirstOrDefault(x => x.Key.Equals("highway", StringComparison.OrdinalIgnoreCase));
            if (tag != null)
            {
                switch (tag.Value.ToLowerInvariant())
                {
                    case "motorway": return 3;
                    case "motorway_link": return 3;
                    case "trunk": return 4;
                    case "trunk_link": return 4;
                    case "primary": return 5;
                    case "primary_link": return 5;
                    case "secondary": return 6;
                    case "secondary_link": return 6;
                    case "tertiary": return 7;
                    case "tertiary_link": return 7;
                    case "unclassified": return 8;
                    case "residential": return 9;
                    case "living_street": return 10;
                    case "service": return 11;
                    case "pedestrian": return 12;
                    case "track": return 13;
                    case "bus_guideway": return 14;
                    case "escape": return 15;
                    case "raceway": return 16;
                    case "road": return 17;
                    case "footway": return 18;
                    case "steps": return 19;
                    case "path": return 20;
                    case "cycleway": return 21;
                    case "bridleway": return 22;
                    default: return 2;
                }
            }

            tag = way.Tags.FirstOrDefault(x => x.Key.Equals("building", StringComparison.OrdinalIgnoreCase));
            if (tag != null)
            {
                return 100;
            }

            return 1;
        }

    }
}