﻿using ProtobufMapper;
using ProtocolBuffers;
using System;
using System.IO;
using System.Threading.Tasks;
using OsmReader.PbfDataObjects;

namespace OSM_pbf_convert
{
    public class PbfBlobParser
    {
        private readonly ProtobufReader reader;
        private readonly long fileLength;
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

        private static MapGenerator CreateMaps()
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
            }
            catch (Exception e)
            {
                throw new InvalidOperationException("Error reading BlobHeader.", e);
            }

        }

        public async Task<Blob> ReadBlobDataAsync(BlobHeader header)
        {
            if (header == null) throw new ArgumentNullException(nameof(header));
            try
            {
                var result = await blobMapper.ReadMessageAsync(reader, (long)header.DataSize);
                result.Header = header;
                return result;
            }
            catch (Exception e)
            {
                throw new InvalidOperationException($"Error reading '{header.Type}' Blob, StartPosition: {header.StartPosition}.", e);
            }
        }

        public void SkipBlob(ulong headerDataSize)
        {
            reader.SkipLength((long)headerDataSize);
        }

        public static async Task<Blob> ReadBlobAsync(PbfBlobParser parser)
        {
            var blobHeader = await parser.ReadBlobHeader();
            Blob blob = null;
            if (blobHeader != null)
            {
                blob = await parser.ReadBlobDataAsync(blobHeader);
            }

            return blob;
        }
    }
}