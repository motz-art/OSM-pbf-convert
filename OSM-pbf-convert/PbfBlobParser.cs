using ProtobufMapper;
using ProtocolBuffers;
using System;
using System.Globalization;
using System.IO;
using System.IO.Compression;
using System.Threading.Tasks;

namespace OSM_pbf_convert
{
    public class PbfBlobParser
    {
        ProtobufReader reader;
        long fileLength;
        private readonly Mapper<BlobHeader> blobHeaderMapper;
        private readonly Mapper<Blob> blobMapper;

        public PbfBlobParser(Stream stream)
        {
            fileLength = stream.Length;
            reader = new ProtobufReader(stream, stream.Length);

            var generator = CreateMaps();
            blobHeaderMapper = generator.CreateMapper<BlobHeader>();
            blobMapper = generator.CreateMapper<Blob>();
        }

        static MapGenerator CreateMaps()
        {
            var generator = new MapGenerator();
            generator.Configure<BlobHeader>()
                .Property(x => x.Type, 1)
                .Property(x => x.DataSize, 3);

            generator.Configure<Blob>()
                .Property(x => x.RawData, 1)
                .Property(x => x.RawSize, 2)
                .Property(x => x.DeflateData, 3)
                .Property(x => x.BZipData, 4)
                .Property(x => x.LZMAData, 5);

            return generator;
        }

        private Task<uint> ReadHeaderLength()
        {
            return reader.ReadInt32BigEndianAsync();
        }

        public async Task<BlobHeader> ReadBlobHeader()
        {
            try
            {
                if (reader.Position == fileLength)
                {
                    return null;
                }
                var headerSize = await ReadHeaderLength();

                var startPosition = reader.Position;
                var result = await blobHeaderMapper.ReadMessageAsync(reader, headerSize);
                result.StartPosition = startPosition;

                return result;

                await reader.BeginReadMessageAsync(headerSize);

                while (reader.State != ProtobufReaderState.EndOfMessage)
                {
                    switch (reader.FieldNumber)
                    {
                        case 1:
                            result.Type = await reader.ReadStringAsync();
                            break;
                        case 3:
                            result.DataSize = await reader.ReadUInt64Async();
                            break;
                        default:
                            await reader.SkipAsync();
                            break;
                    }
                }
                await reader.EndReadMessageAsync();

                return result;
            }
            catch (Exception e)
            {
                throw new InvalidOperationException("Error reading BlobHeader.", e);
            }

        }

        public async Task<Blob> ReadBlobAsync(BlobHeader header)
        {
            var startPosition = reader.Position;
            try
            {
                 Console.WriteLine($"Offset: {startPosition.ToString("#,##0", CultureInfo.CurrentUICulture)}, Reading {header.DataSize.ToString("#,##0", CultureInfo.CurrentUICulture)}");

                var result = await blobMapper.ReadMessageAsync(reader, (long)header.DataSize);

                return result;

                await reader.BeginReadMessageAsync((long)header.DataSize);
                while (reader.State != ProtobufReaderState.EndOfMessage)
                {
                    switch (reader.FieldNumber)
                    {
                        case 1:
                            result.RawData = await reader.ReadAsStreamAsync();
                            result.RawSize = result.RawData.Length;
                            break;
                        case 2:
                            result.RawSize = (long)await reader.ReadUInt64Async();
                            break;
                        case 3:
                            var blobDataStream = new MemoryStream();
                            var stream = await reader.ReadAsStreamAsync();
                            stream.Seek(2, SeekOrigin.Begin);
                            using(var inflate = new DeflateStream(stream, CompressionMode.Decompress))
                            {
                                await inflate.CopyToAsync(blobDataStream);
                            }
                            blobDataStream.Position = 0;
                            result.RawData = blobDataStream;
                            break;
                        case 4:
                            result.LZMAData = await reader.ReadAsStreamAsync();
                            break;
                        case 5:
                            result.BZipData = await reader.ReadAsStreamAsync();
                            break;
                        default:
                            await reader.SkipAsync();
                            break;
                    }
                }
                await reader.EndReadMessageAsync();
                return result;

            }
            catch (Exception e)
            {
                throw new InvalidOperationException("Error reading Blob.", e);
            }
        }
    }
}