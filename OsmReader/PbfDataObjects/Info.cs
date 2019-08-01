namespace OsmReader.PbfDataObjects
{
    public class Info
    {
        public Info()
        {
            Version = -1;
        }

        public int Version { get; set; }
        public int? Timestamp { get; set; }
        public long? ChangeSet { get; set; }
        public int? UserId { get; set; }
        public int? UserStringId { get; set; }
        public bool? Visible { get; set; }
    }
}