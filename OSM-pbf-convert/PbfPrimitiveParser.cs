using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Threading.Tasks;

namespace OSM_pbf_convert
{
    public class PbfPrimitiveParser
    {
        private BlobHeader blobHeader;
        private Blob blob;
        private ProtobufReader reader;

        public PbfPrimitiveParser(BlobHeader blobHeader, Blob blob)
        {
            this.blobHeader = blobHeader;
            this.blob = blob;

            Stream dataStream;

            if (blob.Type == BlobTypes.Raw)
            {
                dataStream = blob.Data;
            }
            else if (blob.Type == BlobTypes.ZLib)
            {
                blob.Data.Seek(2, SeekOrigin.Begin);
                dataStream = new DeflateStream(blob.Data, CompressionMode.Decompress);
            }
            else
            {
                throw new NotImplementedException($"Compression of type {blob.Type} is not supported.");
            }

            this.reader = new ProtobufReader(dataStream, blob.RawSize);
        }

        public async Task<OsmHeader> ParseHeader()
        {
            await reader.BeginReadMessageAsync(blob.RawSize);
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

        public PrimitiveBlock ParseData()
        {
            reader.BeginReadMessage(blob.RawSize);
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
                    case 2:
                        result.DenseNodes = ReadDenseNodes();
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
                    default:
                        reader.Skip();
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