using System;
using System.Collections.Generic;
using System.Globalization;
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
            long nodeCnt = 0;

            using (var stream = File.OpenRead(fileName))
            {
                try {
                    var parser = new PbfBlobParser(stream);
                    Task<int> currentPrimitiveProcessingTask = null;
                    while (true)
                    {
                        var blobHeader = await parser.ReadBlobHeader();
                        if (blobHeader == null) break;
                        var message = await parser.ReadBlobAsync(blobHeader);
                        blobCnt++;
                        maxBlobSize = Math.Max(maxBlobSize, blobHeader.DataSize);
                        maxDataSize = Math.Max(maxDataSize, message.RawSize);
                        if (currentPrimitiveProcessingTask!=null)
                        {
                            nodeCnt += await currentPrimitiveProcessingTask;
                            Console.WriteLine($"Nodes: {nodeCnt.ToString("#,##0", CultureInfo.CurrentUICulture)}.");
                        }
                        currentPrimitiveProcessingTask = Task.Run(() => ProcessNodeCount(blobHeader, message));
                    }
                }
                catch (Exception e)
                {
                    Console.WriteLine($"Error processing file. After reading {stream.Position} of {stream.Length} bytes.");
                    Console.WriteLine(e);
                }
            }
            Console.WriteLine($"Statistic: max blob {maxBlobSize}, data: {maxDataSize}, total cnt: {blobCnt}, nodes: {nodeCnt}");
            Console.ReadLine();
        }

        private static async Task<int> ProcessNodeCount(BlobHeader blobHeader, Blob message)
        {
            var primitiveParser = new PbfPrimitiveReader(blobHeader, message);
            var nodeCnt = 0;
            if (blobHeader.Type == "OSMHeader")
            {
                var header = await primitiveParser.ParseHeader();
            }
            if (blobHeader.Type == "OSMData")
            {
                var data = primitiveParser.ParseData();
                nodeCnt += data?.PrimitiveGroup?.Select(x=>x.DenseNodes?.Ids?.Count ?? 0).Sum() ?? 0;
            }

            return nodeCnt;
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
