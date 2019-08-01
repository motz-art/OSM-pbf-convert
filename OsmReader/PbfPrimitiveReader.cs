using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Threading.Tasks;
using OsmReader.PbfDataObjects;
using ProtobufMapper;
using ProtocolBuffers;

namespace OsmReader
{
    public class PbfPrimitiveReader
    {
        private readonly long length;
        private readonly ProtobufReader reader;
        private static Mapper<Way> wayMapper;

        public PbfPrimitiveReader(Stream stream, long length)
        {
            this.length = length;
            this.reader = new ProtobufReader(stream, length);
        }

        public static PbfPrimitiveReader Create(Blob blob, bool preload = true)
        {
            Stream dataStream;

            if (blob.RawData != null)
            {
                dataStream = blob.RawData;

                if (preload)
                {
                    var data = new byte[blob.RawSize];
                    var len = dataStream.Read(data, 0, data.Length);
                    if (len != data.Length)
                    {
                        throw new InvalidOperationException($"Can't read {data.Length} bytes of data!");
                    }

                    dataStream = new MemoryStream(data, 0, len);
                }
            }
            else if (blob.DeflateData != null)
            {
                dataStream = blob.DeflateData;

                if (preload)
                {
                    var data = new byte[blob.DeflateData.Length];
                    var len = dataStream.Read(data, 0, data.Length);
                    if (len != data.Length)
                    {
                        throw new InvalidOperationException($"Can't read {data.Length} bytes of data!");
                    }

                    dataStream = new MemoryStream(data, 0, len);
                }

                dataStream.Seek(2, SeekOrigin.Begin);

                dataStream = new DeflateStream(dataStream, CompressionMode.Decompress);
            }
            else
            {
                var type = blob.BZipData != null ? "BZip" : blob.LZMAData != null ? "LZMA" : "Unknown";
                throw new NotImplementedException($"Blob of type {type} is not supported.");
            }

            return new PbfPrimitiveReader(dataStream, blob.RawSize);
        }

        public async Task<OsmHeader> ReadHeader()
        {
            await reader.BeginReadMessageAsync(length);
            var header = new OsmHeader();
            while (reader.State == ProtobufReaderState.Field)
            {
                switch (reader.FieldNumber)
                {
                    case 1:
                        header.BoundBox = await ParseBoundBoxAsync();
                        break;
                    case 4:
                        header.RequiredFeatures = await reader.ReadStringAsync();
                        break;
                    case 5:
                        header.OptionalFeatures = await reader.ReadStringAsync();
                        break;
                    case 16:
                        header.WritingProgram = await reader.ReadStringAsync();
                        break;
                    case 17:
                        header.Source = await reader.ReadStringAsync();
                        break;
                    default:
                        await reader.SkipAsync();
                        break;
                }
            }
            await reader.EndReadMessageAsync();
            return header;
        }

        public PrimitiveBlock ReadData()
        {
            reader.BeginReadMessage(length);
            var result = new PrimitiveBlock();
            result.PrimitiveGroup = new List<PrimitiveGroup>();
            while (reader.State == ProtobufReaderState.Field)
            {
                switch (reader.FieldNumber)
                {
                    case 1:
                        result.Strings = ReadStringTable();
                        break;
                    case 2:
                        result.PrimitiveGroup.Add(ReadPrimitiveGroup());
                        break;
                    case 17:
                        result.Granularity = (int)reader.ReadInt64();
                        break;
                    case 19:
                        result.LatOffset = reader.ReadInt64();
                        break;
                    case 20:
                        result.LonOffset = reader.ReadInt64();
                        break;
                    case 18:
                        result.DateGranularity = (int)reader.ReadInt64();
                        break;
                    default:
                        reader.Skip();
                        break;
                }
            }
            reader.EndReadMessage();
            return result;
        }

        private string[] ReadStringTable()
        {
            reader.BeginReadMessage();
            var strings = new List<string>();
            while (reader.State == ProtobufReaderState.Field)
            {
                switch (reader.FieldNumber)
                {
                    case 1:
                        strings.Add(reader.ReadString());
                        break;
                    default:
                        reader.Skip();
                        break;
                }
            }
            reader.EndReadMessage();
            return strings.ToArray();
        }

        private PrimitiveGroup ReadPrimitiveGroup()
        {
            reader.BeginReadMessage();
            var result = new PrimitiveGroup();

            while (reader.State == ProtobufReaderState.Field)
            {
                switch (reader.FieldNumber)
                {
                    case 1:
                        if (result.Nodes == null)
                        {
                            result.Nodes = new List<Node>();
                        }
                        result.Nodes.Add(ReadNode());
                        break;
                    case 2:
                        result.DenseNodes = ReadDenseNodes();
                        break;
                    case 3:
                        if (result.Ways == null)
                        {
                            result.Ways = new List<Way>();
                        }
                        result.Ways.Add(ReadWay());
                        break;
                    case 4:
                        if (result.Relations == null)
                        {
                            result.Relations = new List<Relation>();
                        }
                        result.Relations.Add(ReadRelation());
                        break;
                    default:
                        reader.Skip();
                        break;
                }
            }
            reader.EndReadMessage();
            return result;
        }

        private Way ReadWay()
        {
            reader.BeginReadMessage();
            var result = new Way();
            while (reader.State == ProtobufReaderState.Field)
            {
                switch (reader.FieldNumber)
                {
                        case 1:
                            result.Id = reader.ReadInt64();
                            break;
                        case 2:
                            result.Keys = reader.ReadPackedInt64Array();
                            break;
                        case 3:
                            result.Values = reader.ReadPackedInt64Array();
                            break;
                        case 4:
                            result.Info = ReadInfo();
                            break;
                        case 8:
                            result.Refs = reader.ReadPackedSInt64Array();
                            break;
                        default:
                            reader.Skip();
                            break;
                }
            }
            reader.EndReadMessage();
            return result;
        }

        private Node ReadNode()
        {
            reader.BeginReadMessage();
            var result = new Node();

            while (reader.State == ProtobufReaderState.Field)
            {
                switch (reader.FieldNumber)
                {
                    case 1:
                        result.Id = reader.ReadSInt64();
                        break;
                    case 2:
                        result.Keys = reader.ReadPackedInt64Array().Cast<uint>().ToList();
                        break;
                    case 3:
                        result.Values = reader.ReadPackedInt64Array().Cast<uint>().ToList();
                        break;
                    case 4:
                        result.Info = ReadInfo();
                        break;
                    case 8:
                        result.Lat = reader.ReadSInt64();
                        break;
                    case 9:
                        result.Lon = reader.ReadSInt64();
                        break;
                    default:
                        reader.Skip();
                        break;
                }
            }
            reader.EndReadMessage();
            return result;
        }

        private Info ReadInfo()
        {
            reader.BeginReadMessage();
            var result = new Info();

            while (reader.State == ProtobufReaderState.Field)
            {
                switch (reader.FieldNumber)
                {
                    case 1:
                        result.Version = reader.ReadInt32();
                        break;
                    case 2:
                        result.Timestamp = reader.ReadInt32();
                        break;
                    case 3:
                        result.ChangeSet = reader.ReadInt64();
                        break;
                    case 4:
                        result.UserId = reader.ReadInt32();
                        break;
                    case 5:
                        result.UserStringId = reader.ReadInt32();
                        break;
                    case 6:
                        result.Visible = reader.ReadInt64() != 0;
                        break;
                    default:
                        reader.Skip();
                        break;
                }
            }
            reader.EndReadMessage();
            return result;
        }

        private DenseNodes ReadDenseNodes()
        {
            reader.BeginReadMessage();
            var result = new DenseNodes();
            while (reader.State == ProtobufReaderState.Field)
            {
                switch (reader.FieldNumber)
                {
                    case 1:
                        result.Ids.AddRange(reader.ReadPackedSInt64Array());
                        break;
                    case 8:
                        result.Latitudes.AddRange(reader.ReadPackedSInt64Array());
                        break;
                    case 9:
                        result.Longitudes.AddRange(reader.ReadPackedSInt64Array());
                        break;
                    case 10:
                        result.KeysValues.AddRange(reader.ReadPackedInt64Array().Select(x=>(int)x));
                        break;
                    default:
                        reader.Skip();
                        break;
                }
            }
            reader.EndReadMessage();
            return result;
        }

        private Relation ReadRelation()
        {
            reader.BeginReadMessage();
            var result = new Relation();
            while (reader.State == ProtobufReaderState.Field)
            {
                switch (reader.FieldNumber)
                {
                        case 1:
                            result.Id = reader.ReadInt64();
                            break;
                        case 2:
                            result.Keys.AddRange(reader.ReadPackedInt64Array());
                            break;
                        case 3:
                            result.Values.AddRange(reader.ReadPackedInt64Array());
                            break;
                        case 4:
                            result.Info = ReadInfo();
                            break;
                        case 8:
                            result.Roles.AddRange(reader.ReadPackedInt64Array());
                            break;
                        case 9:
                            result.MemberIds.AddRange(reader.ReadPackedInt64Array());
                            break;
                        case 10:
                            result.MemberType.AddRange(reader.ReadPackedInt64Array().Select(x => (RelationMemberTypes)x));
                            break;
                }                
            }
            reader.EndReadMessage();
            return result;
        }
        
        private async Task<BoundBox> ParseBoundBoxAsync()
        {
            await reader.BeginReadMessageAsync();
            var result = new BoundBox();
            while (reader.State == ProtobufReaderState.Field)
            {
                switch (reader.FieldNumber)
                {
                    case 1:
                        result.Left = await reader.ReadSInt64Async();
                        break;
                    case 2:
                        result.Right = await reader.ReadSInt64Async();
                        break;
                    case 3:
                        result.Top = await reader.ReadSInt64Async();
                        break;
                    case 4:
                        result.Bottom = await reader.ReadSInt64Async();
                        break;
                    default:
                        await reader.SkipAsync();
                        break;
                }
            }
            await reader.EndReadMessageAsync();
            return result;
        }
    }
}