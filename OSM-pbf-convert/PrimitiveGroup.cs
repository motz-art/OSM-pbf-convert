using System.Collections.Generic;

namespace OSM_pbf_convert
{
    public class PrimitiveGroup
    {
        public List<Node> Nodes { get; set; }
        public DenseNodes DenseNodes { get; set; }
        public List<Way> Ways { get; set; }
        public List<Relation> Relations { get; set; }
        public List<ChangeSet> ChangeSets { get; set; }
    }
}