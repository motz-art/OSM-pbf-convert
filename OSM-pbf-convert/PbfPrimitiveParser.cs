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
            await reader.BeginReadMessage(blob.RawSize);
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

        public async Task ParseDataAsync()
        {
            await reader.BeginReadMessage(blob.RawSize);
            var result = new PrimitiveBlock();
            while (reader.State == ProtobufReaderState.Field)
            {
                switch (reader.FieldNumber)
                {
                    case 1:
                        result.Strings = await ReadStringTableAsync();
                        break;
                    case 2:
                        result.PrimitiveGroup = await ReadPrimitiveGroupAsync();
                        break;
                    case 17:
                        result.Granularity = (int)await reader.ReadInt64Async();
                        break;
                    case 19:
                        result.LatOffset = await reader.ReadInt64Async();
                        break;
                    case 20:
                        result.LonOffset = await reader.ReadInt64Async();
                        break;
                    case 18:
                        result.DateGranularity = (int)await reader.ReadInt64Async();
                        break;
                    default:
                        await reader.SkipAsync();
                        break;
                }
            }
            await reader.EndReadMessageAsync();
        }

        private async Task<string[]> ReadStringTableAsync()
        {
            await reader.BeginReadMessage();
            var strings = new List<string>();
            while (reader.State == ProtobufReaderState.Field)
            {
                switch (reader.FieldNumber)
                {
                    case 1:
                        strings.Add(await reader.ReadStringAsync());
                        break;
                    default:
                        await reader.SkipAsync();
                        break;
                }
            }
            await reader.EndReadMessageAsync();
            return strings.ToArray();
        }

        private async Task<PrimitiveGroup> ReadPrimitiveGroupAsync()
        {
            await reader.BeginReadMessage();
            var result = new PrimitiveGroup();
            while (reader.State == ProtobufReaderState.Field)
            {
                switch (reader.FieldNumber)
                {
                    case 2:
                        result.DenseNodes = await ReadDenseNodesAsync();
                        break;
                    default:
                        await reader.SkipAsync();
                        break;
                }
            }
            await reader.EndReadMessageAsync();
            return result;
        }

        private async Task<DenseNodes> ReadDenseNodesAsync()
        {
            await reader.BeginReadMessage();
            var result = new DenseNodes();
            while (reader.State == ProtobufReaderState.Field)
            {
                switch (reader.FieldNumber)
                {
                    case 1:
                        result.Ids.AddRange(await reader.ReadPaskedSInt64ArrayAsync());
                        break;
                    case 8:
                        result.Latitudes.AddRange(await reader.ReadPaskedSInt64ArrayAsync());
                        break;
                    case 9:
                        result.Longitudes.AddRange(await reader.ReadPaskedSInt64ArrayAsync());
                        break;
                    default:
                        await reader.SkipAsync();
                        break;
                }
            }
            await reader.EndReadMessageAsync();
            return result;
        }

        private async Task<BoundBox> ParseBoundBoxAsync()
        {
            await reader.BeginReadMessage();
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