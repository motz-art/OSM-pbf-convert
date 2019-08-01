using System.Collections.Generic;

namespace OsmReader.PbfDataObjects
{
    public class DenseInfo
    {
        public List<int> Versions { get; set; }
        public List<long> TimeStamps { get; set; }
        public List<long> ChangeSets { get; set; }
        public List<int> UserIds { get; set; }
        public List<int> UserIdStrings { get; set; }
        public List<bool> Visibles { get; set; }
    }
}