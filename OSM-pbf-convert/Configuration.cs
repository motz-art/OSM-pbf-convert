namespace OSM_pbf_convert;

public class Configuration
{
    public string PbfFileName { get; set; }
    public bool CanReadExistingFiles { get; set; }
    public ulong StartPosition { get; set; }
    public string ActionName { get; set; }
    public ulong WaysStartOffset { get; set; }
    public ulong RelationsStartOffset { get; set; }
    public string? DataPath { get; set; } = "./Blocks/";
}