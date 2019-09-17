using System;

namespace OsmReader
{
    public class OsmTag : IEquatable<OsmTag>
    {
        public bool Equals(OsmTag other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return string.Equals(Key, other.Key) && string.Equals(Value, other.Value);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != this.GetType()) return false;
            return Equals((OsmTag) obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                return ((Key != null ? Key.GetHashCode() : 0) * 397) ^ (Value != null ? Value.GetHashCode() : 0);
            }
        }

        public static bool operator ==(OsmTag left, OsmTag right)
        {
            return Equals(left, right);
        }

        public static bool operator !=(OsmTag left, OsmTag right)
        {
            return !Equals(left, right);
        }

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