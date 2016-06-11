namespace OSM_pbf_convert
{
    public enum ProtobufReaderState
    {
        None = 0,
        Field,
        EndOfMessage,
        EndOfFile
    }
}