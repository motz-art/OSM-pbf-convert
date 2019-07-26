using System.Collections.Generic;
using System.Threading.Tasks;

namespace OSM_pbf_convert
{
    public class PrimitiveAccessor
    {
        private readonly PbfPrimitiveReader reader;
        private PrimitiveBlock data;
        private Task<PrimitiveBlock> readTask;

        public PrimitiveAccessor(PbfPrimitiveReader reader)
        {
            this.reader = reader;
        }

        public IEnumerable<OsmWay> Ways => PrimitiveDecoder.DecodeWays(Data);
        public IEnumerable<OsmNode> Nodes => PrimitiveDecoder.DecodeAllNodes(Data);
        public IEnumerable<OsmRelation> Relations => PrimitiveDecoder.DecodeRelations(Data);

        public PrimitiveBlock Data => data ?? ReadData();

        public void StartRead()
        {
            if (readTask == null)
            {
                lock (reader)
                {
                    if (readTask == null)
                    {
                        readTask = Task.Run(() =>
                            reader.ReadData()
                        );
                    }
                }
            }
        }

        private PrimitiveBlock ReadData()
        {
            StartRead();
            readTask.Wait();
            return readTask.Result;
        }
    }
}