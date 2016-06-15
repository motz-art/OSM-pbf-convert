﻿using System;
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

        public PbfBlobParser(Stream stream)
        {
            fileLength = stream.Length;
            reader = new ProtobufReader(stream, stream.Length);
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
                var result = new BlobHeader();
                result.StartPosition = reader.Position;

                var headerSize = await ReadHeaderLength();
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

                await reader.BeginReadMessageAsync((long)header.DataSize);
                Blob result = new Blob();
                while (reader.State != ProtobufReaderState.EndOfMessage)
                {
                    switch (reader.FieldNumber)
                    {
                        case 1:
                            result.Type = BlobTypes.Raw;
                            result.Data = await reader.ReadAsStreamAsync();
                            result.RawSize = result.Data.Length;
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
                            result.Type = BlobTypes.Raw;
                            result.Data = blobDataStream;
                            break;
                        case 4:
                            result.Type = BlobTypes.LZMA;
                            result.Data = await reader.ReadAsStreamAsync();
                            break;
                        case 5:
                            result.Type = BlobTypes.BZip;
                            result.Data = await reader.ReadAsStreamAsync();
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