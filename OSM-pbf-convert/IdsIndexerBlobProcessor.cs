using System;
using System.Collections.Generic;
using ProtocolBuffers;

namespace OSM_pbf_convert
{
    public class IdsIndexerBlobProcessor :
        IBlobProcessor<BlobIdsInfo>
    {
        private readonly string fileName;
        private readonly List<BlobIdsInfo> infos = new List<BlobIdsInfo>();

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

                foreach (var way in accessor.Ways)
                    waysCnt++;

                foreach (var relation in accessor.Relations) relationCnt++;

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