using System.Collections.Generic;

namespace OSM_pbf_convert
{
    public class Relation
    {
        public long Id { get; set; }
        public List<int> Keys { get; set; }
        public List<int> Values { get; set; }
        public Info Info { get; set; }
        public List<int> Roles { get; set; }
        public List<long> MemberIds { get; set; }
        public List<RelationMemberTypes> MemberType { get; set; }
    }
}