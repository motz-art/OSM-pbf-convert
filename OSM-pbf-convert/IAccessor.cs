namespace OSM_pbf_convert
{
    public interface IAccessor<in TKey, TValue>
    {
        TValue Read(TKey key);
        void Write(TKey key, TValue value);
    }
}