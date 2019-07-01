using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace OSM_pbf_convert
{
    class Program
    {
        private static List<BlobIdsInfo> info = new List<BlobIdsInfo>();

        static async Task Process(string fileName)
        {
            ulong maxBlobSize = 0;
            long maxDataSize = 0;
            var blobCnt = 0;
            long nodeCnt = 0;

            using (var stream = File.OpenRead(fileName))
            using (var nodesStream = File.Open(fileName + ".nodes.dat", FileMode.OpenOrCreate))
            {
                try
                {
                    var parser = new PbfBlobParser(stream);
                    var writer = new NodesIndexWriter(nodesStream);
                    var pool = new Semaphore(Environment.ProcessorCount + 2, Environment.ProcessorCount + 2);
                    while (true)
                    {
                        var blobHeader = await parser.ReadBlobHeader();
                        if (blobHeader == null) break;
                        var message = await parser.ReadBlobAsync(blobHeader);
                        blobCnt++;
                        maxBlobSize = Math.Max(maxBlobSize, blobHeader.DataSize);
                        maxDataSize = Math.Max(maxDataSize, message.RawSize);

                        var headerType = blobHeader.Type;
                        var reader = PbfPrimitiveReader.Create(message);
                        
                        pool.WaitOne();
                        
                        Console.Write($" Nodes: {nodeCnt.ToString("#,##0", CultureInfo.CurrentUICulture)}.\r");

                        Task.Run(async () =>
                        {
                            try
                            {
                                var cnt = await ProcessBlob(writer, headerType, reader);
                                Interlocked.Add(ref nodeCnt, (long) cnt);
                            }
                            catch (Exception e)
                            {
                                Console.WriteLine(e);
                            }
                            finally
                            {
                                pool.Release(1);
                            }
                        });
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

        private static async Task<int> ProcessBlob(NodesIndexWriter nodesIndexWriter, string blobHeaderType, PbfPrimitiveReader pbfPrimitiveReader)
        {
            var nodeCnt = 0;
            var minId = long.MaxValue;
            var maxId = long.MinValue;
            if (blobHeaderType == "OSMHeader")
            {
                var header = await pbfPrimitiveReader.ReadHeader();
            }
            if (blobHeaderType == "OSMData")
            {
                var data = pbfPrimitiveReader.ReadData();
                var nodes = PrimitiveDecoder.DecodeDenseNodes(data);

                foreach (var node in nodes)
                {
                    minId = Math.Min(minId, node.Id);
                    maxId = Math.Max(maxId, node.Id);
                    nodeCnt++;

                    var lon = (uint)Math.Round((node.Lon - 180) / 180 * uint.MaxValue);
                    var lat = (uint)Math.Round((node.Lat - 90) / 90 * uint.MaxValue);
                    nodesIndexWriter.Write(node.Id, lon, lat);
                }

            }

            info.Add(new BlobIdsInfo
            {
                MinNodeId = minId,
                MaxNodeId = maxId,
                NodesCount = nodeCnt
            });

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

    internal class BlobIdsInfo
    {
        public int Id { get; set; }
        public long MinNodeId { get; set; }
        public long MaxNodeId { get; set; }
        public int NodesCount { get; set; }
    }
}
