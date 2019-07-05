using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace OSM_pbf_convert
{
    class MapBuilder
    {
        public static async Task Process(string fileName, string indexFileName)
        {
            var index = BlobIdsInfo.ReadIdsIndex(indexFileName).ToList();
            
            Console.WriteLine($"Index size: {index.Count}");

            using (var stream = File.OpenRead(fileName))
            {
                foreach (var info in index.Where(x => x.WaysCount > 0).Take(5))
                {
                    stream.Position = 0;
                    var parser = new PbfBlobParser(stream);
                    parser.SkipBlob((ulong) (info.StartPosition - 4));
                    var header = await parser.ReadBlobHeader();
                    var blob = await parser.ReadBlobAsync(header);
                    var primitiveReader = PbfPrimitiveReader.Create(blob);
                    var data = primitiveReader.ReadData();

                    var ways = PrimitiveDecoder.DecodeWays(data).ToList();
                    
                    Console.WriteLine();

                    foreach (var way in ways)
                    {
                        var nodeIds = way.NodeIds.Distinct().OrderBy(x => x).ToList();

                        var blobsToRead = FindBlobIdsToRead(index, nodeIds);

                        Console.WriteLine($"Id: {way.Id}, Cnt: {blobsToRead.Count}. {string.Join(", ",blobsToRead)}");
                    }

                }
            }

            Console.WriteLine("Done! Press any key....");
            Console.ReadKey();
        }

        private static List<BlobIdsInfo> FindBlobIdsToRead(List<BlobIdsInfo> index, List<long> nodeIds)
        {
            var blobsToRead = new List<BlobIdsInfo>();
            if (nodeIds.Count == 0) return blobsToRead;

            var nodeIndex = 0;
            var lastId = nodeIds[nodeIndex];

            for (int i = 0; i < index.Count; i++)
            {
                var info = index[i];
                if (info.NodesCount <= 0) continue;

                while (info.MaxNodeId < lastId && nodeIndex > 0)
                {
                    nodeIndex--;
                    lastId = nodeIds[nodeIndex];
                }

                while (info.MinNodeId > lastId && nodeIndex < nodeIds.Count - 1)
                {
                    nodeIndex++;
                    lastId = nodeIds[nodeIndex];
                }

                if (lastId <= info.MaxNodeId && lastId >= info.MinNodeId)
                {
                    blobsToRead.Add(info);
                }
            }

            return blobsToRead;
        }
    }

    class Program
    {
        static void Main(string[] args)
        {
            if (args.Length != 2)
            {
                Console.WriteLine("Pbf action and file name is not specified.");
                return;
            }
            if (args[0] == "blob-index")
                Task.Run(() => BlobIdsIndexer.Process(args[0])).Wait();
            
            if (args[1] == "tst")
                Task.Run(() => MapBuilder.Process(args[0], args[0] + ".nodes.dat")).Wait();
        }
    }
}
