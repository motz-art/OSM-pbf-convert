using System.Collections.Generic;

namespace OsmReader
{
    public class OsmRelation
    {
        public long Id { get; set; }
        public IReadOnlyList<RelationItem> Items { get; set; }
        public IReadOnlyList<OsmTag> Tags { get; set; }
    }

    public class RelationItem
    {
        public RelationMemberTypes MemberType { get; set; }
        public long Id { get; set; }
        public string Role { get; set; }
    }
}