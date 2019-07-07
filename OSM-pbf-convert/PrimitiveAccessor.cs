using System.Collections;
using System.Collections.Generic;

namespace OSM_pbf_convert
{
    public class PrimitiveAccessor
    {
        private PrimitiveBlock data;
        private readonly PbfPrimitiveReader reader;

        public PrimitiveAccessor(PbfPrimitiveReader reader)
        {
            this.reader = reader;
        }

        public IEnumerable<OsmWay> Ways => PrimitiveDecoder.DecodeWays(Data);
        public IEnumerable<OsmNode> Nodes => PrimitiveDecoder.DecodeAllNodes(Data);
        public IEnumerable<OsmRelation> Relations => PrimitiveDecoder.DecodeRelations(Data);
        
        public PrimitiveBlock Data => data ?? (data = ReadData());

        private PrimitiveBlock ReadData()
        {
            return reader.ReadData();
        }
    }
}