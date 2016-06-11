using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OSM_pbf_convert
{
    public class PbfParser
    {
        ProtobufReader reader;

        public PbfParser(Stream stream)
        {
            reader = new ProtobufReader(stream);
        }

        private Task<uint> ReadHeaderLength()
        {
            return reader.ReadInt32BigEndian();
        }

        public async Task<BlobHeader> ReadBlobHeader()
        {
            var headerSize = await ReadHeaderLength();
            await reader.BeginReadMessage(headerSize);
            var result = new BlobHeader();

            while (reader.State != ProtobufReaderState.EndOfMessage)
            {
                switch (reader.FieldNumber)
                {
                    case 1: result.Type = await reader.ReadStringAsync();
                        break;
                    case 3: result.DataSize = await reader.ReadUInt64Async();
                        break;
                    default: reader.Skip();
                        break;
                }
            }
            await reader.EndReadMessageAsync();

            return result;
        }

        public async Task<Blob> ReadBlobAsync(BlobHeader header)
        {
            await reader.BeginReadMessage((long)header.DataSize);
            Blob result = new Blob();
            while (reader.State != ProtobufReaderState.EndOfMessage)
            {
                switch (reader.FieldNumber)
                {
                    case 1: result.Type = BlobTypes.Raw;
                        result.Data = await reader.ReadAsStreamAsync();
                        result.RawSize = result.Data.Length;
                        break;
                    case 2: result.RawSize = (long) await reader.ReadUInt64Async();
                        break;
                    case 3: result.Type = BlobTypes.ZLib;
                        result.Data = await reader.ReadAsStreamAsync();
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
                        reader.Skip();
                        break;
                }
            }
            await reader.EndReadMessageAsync();
            return result;
        }
    }

    class Program
    {
        static async Task Process(string fileName)
        {
            using (var stream = File.OpenRead(fileName))
            {
                var parser = new PbfParser(stream);
                while (true)
                {
                    var blobHeader = await parser.ReadBlobHeader();
                    var message = await parser.ReadBlobAsync(blobHeader);
                }
            }
            Console.ReadLine();
        }

        static void Main(string[] args)
        {
            Task.Run(() => Process(args[0]).Wait()).Wait();
        }
    }
}
