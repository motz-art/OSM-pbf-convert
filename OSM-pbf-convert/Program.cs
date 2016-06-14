using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OSM_pbf_convert
{
    class Program
    {
        static async Task Process(string fileName)
        {
            ulong maxBlobSize = 0;
            long maxDataSize = 0;
            var blobCnt = 0;

            using (var stream = File.OpenRead(fileName))
            {
                try {
                    var parser = new PbfBlobParser(stream);
                    while (true)
                    {
                        var blobHeader = await parser.ReadBlobHeader();
                        if (blobHeader == null) break;
                        var message = await parser.ReadBlobAsync(blobHeader);
                        blobCnt++;
                        maxBlobSize = Math.Max(maxBlobSize, blobHeader.DataSize);
                        maxDataSize = Math.Max(maxDataSize, message.RawSize);
                        var primitiveParser = new PbfPrimitiveParser(blobHeader, message);
                        if (blobHeader.Type == "OSMHeader")
                        {
                            var header = await primitiveParser.ParseHeader();
                        }
                        if (blobHeader.Type == "OSMData")
                        {
                            await primitiveParser.ParseDataAsync();
                        }
                    }
                }
                catch (Exception e)
                {
                    Console.WriteLine($"Error processing file. After reading {stream.Position} of {stream.Length} bytes.");
                    Console.WriteLine(e);
                }
            }
            Console.WriteLine($"Statistic: max blob {maxBlobSize}, data: {maxDataSize}, total cnt: {blobCnt}");
            Console.ReadLine();
        }

        static void Main(string[] args)
        {
            if (args.Length != 1)
            {
                Console.WriteLine("Pbf file name is not specified.");
                return;
            }
            Task.Run(() => Process(args[0]).Wait()).Wait();
        }
    }
}
