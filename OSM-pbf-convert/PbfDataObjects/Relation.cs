using System.Collections.Generic;

namespace OSM_pbf_convert
{
    public class Relation
    {
        public long Id { get; set; }
        public List<long> Keys { get; } = new List<long>();
        public List<long> Values { get; } = new List<long>();
        public Info Info { get; set; }
        public List<long> Roles { get; } = new List<long>();
        public List<long> MemberIds { get; } = new List<long>();
        public List<RelationMemberTypes> MemberType { get; } = new List<RelationMemberTypes>();
    }
}