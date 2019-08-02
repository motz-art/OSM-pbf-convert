using System;
using System.Collections.Generic;
using System.Linq;
using OsmReader;
using OsmReader.PbfDataObjects;

namespace OSM_pbf_convert
{
    public class IdsIndexerBlobProcessor :
        IBlobProcessor<BlobIdsInfo>
    {
        private readonly string fileName;
        private readonly List<BlobIdsInfo> infos = new List<BlobIdsInfo>();
        private bool waysStarted = false;
        private bool relationsStarted = false;

        private int maxMembersCount = 0;
        private ulong totalMemberCount = 0;
        private long totalRelsCount = 0;
        private int[] memberCounts = new int[1000];

        public IdsIndexerBlobProcessor(string fileName)
        {
            this.fileName = fileName ?? throw new ArgumentNullException(nameof(fileName));
        }

        public BlobIdsInfo BlobRead(Blob blob)
        {
            var info = new BlobIdsInfo
            {
                StartPosition = blob.Header.StartPosition
            };

            infos.Add(info);
            return info;
        }

        public void ProcessPrimitives(PrimitiveAccessor accessor, BlobIdsInfo info)
        {
            if (info != null)
            {
                var nodeCnt = 0;
                var minId = long.MaxValue;
                var maxId = long.MinValue;
                var waysCnt = 0;
                var relationCnt = 0;

                var nodes = accessor.Nodes;

                foreach (var node in nodes)
                {
                    minId = Math.Min(minId, node.Id);
                    maxId = Math.Max(maxId, node.Id);
                    nodeCnt++;

                    var lon = (uint) Math.Round((node.Lon - 180) / 180 * uint.MaxValue);
                    var lat = (uint) Math.Round((node.Lat - 90) / 90 * uint.MaxValue);
                    //nodesIndexWriter.Write(node.Id, lon, lat);
                }

                if (!waysStarted && accessor.Ways.Any())
                {
                    waysStarted = true;
                    Console.WriteLine($"\r\nWays offset: {info.StartPosition}.");
                }
                foreach (var way in accessor.Ways)
                    waysCnt++;


                if (!relationsStarted && accessor.Relations.Any())
                {
                    relationsStarted = true;
                    Console.WriteLine($"\r\nRelations offset: {info.StartPosition}.");
                }

                foreach (var relation in accessor.Relations)
                {
                    relationCnt++;
                    totalRelsCount++;
                    var itemsCount = relation.Items.Count;
                    maxMembersCount = Math.Max(maxMembersCount, itemsCount);
                    totalMemberCount += (ulong) itemsCount;

                    if (itemsCount > 400)
                    {
                        itemsCount = 400;
                    }
                    if (itemsCount < memberCounts.Length)
                    {
                        memberCounts[itemsCount]++;
                    }
                }

                Console.WriteLine($"\r\nMax Count: {maxMembersCount:#,###}. Avg: {1.0 * totalMemberCount / totalRelsCount}. Total: {totalRelsCount:#,###}.");

                for (int i = 1; i <= 400; i++)
                {
                    Console.Write($"{i:000}: {memberCounts[i]:00000},  ");
                    if (i % 10 == 0)
                    {
                        Console.WriteLine();
                    }
                }
                Console.SetCursorPosition(0,0);


                info.MinNodeId = minId;
                info.MaxNodeId = maxId;
                info.NodesCount = nodeCnt;
                info.WaysCount = waysCnt;
                info.RelationsCount = relationCnt;
            }
        }

        public void Finish()
        {
            BlobIdsInfo.WriteIdsIndex(fileName, infos);
        }
    }
}