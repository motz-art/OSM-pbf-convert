using System;
using System.Globalization;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace OSM_pbf_convert
{
    public class Program
    {
        public static async Task Main(string[] args)
        {
            if (args.Length != 2)
            {
                Console.WriteLine("Pbf action and file name is not specified.");
                return;
            }

            if (args[0] == "blob-index")
            {
                using (var indexer = new BlobIdsIndexer(args[1]))
                {
                    await Task.Run(() => indexer.Process(args[1]));
                }
            }

            if (args[0] == "tst")
            {
                Task.Run(() => MapBuilder.Process(args[1], args[1] + ".nodes.dat")).Wait();
            }
        }
    }
}
