namespace OsmReader
{
    public class OsmTag
    {
        public string Key { get; set; }
        public string Value { get; set; }

        public OsmTag()
        {

        }
        public OsmTag(string key, string value)
        {
            Key = key;
            Value = value;
        }

        public override string ToString()
        {
            return $"'{Key}' => '{Value}'";
        }
    }
}