using OsmReader;
using OsmReader.PbfDataObjects;

namespace OSM_pbf_convert
{
    public interface IBlobProcessor<T>
    {
        T BlobRead(Blob blob);
        void ProcessPrimitives(PrimitiveAccessor accessor, T data);
        void Finish();
    }
}