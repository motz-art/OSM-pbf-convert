using System;
using System.Threading.Tasks;

namespace OSM_pbf_convert
{
    public class Program
    {
        public static async Task Main(string[] args)
        {
            if (args.Length < 2)
            {
                Console.WriteLine("Pbf action and file name is not specified.");
                return;
            }

            if (args[0] == "blob-index")
            {
                using (var indexer = PbfFileProcessor.Create(args[1],
                    new IdsIndexerBlobProcessor(args[1] + ".blobs.dat")))
                {
                    await indexer.Process().ConfigureAwait(false);
                }
            }

            if (args[0] == "nodes-index")
            {
                using (var indexer = PbfFileProcessor.Create(args[1], new NodesIndexBlobProcessor(args[1])))
                {
                    await indexer.Process().ConfigureAwait(false);
                }
            }

            if (args[0] == "join")
            {
                using (var indexer = PbfFileProcessor.Create(
                    args[1], 
                    new NodesToWaysJoinProcessor(args[1], true), 
                    ulong.Parse(args[2])))
                {
                    await indexer.Process();
                }
            }

            if (args[0] == "tst")
            {
                Task.Run(() => MapBuilder.Process(args[1], args[1] + ".blobs.dat")).Wait();
            }

            if (args[0] == "heat-map")
            {
                using (var indexer = PbfFileProcessor.Create(args[1], new HeatMapProcessor(args[1])))
                {
                    await indexer.Process().ConfigureAwait(false);
                }
            }

            Console.WriteLine("Done!");
            Console.ReadKey();
        }
    }
}
