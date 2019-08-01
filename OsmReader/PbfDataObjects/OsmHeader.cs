namespace OsmReader.PbfDataObjects
{
    public class OsmHeader
    {
        public BoundBox BoundBox { get; internal set; }
        public string WritingProgram { get; internal set; }
        public string OptionalFeatures { get; internal set; }
        public string RequiredFeatures { get; internal set; }
        public string Source { get; internal set; }
    }
}