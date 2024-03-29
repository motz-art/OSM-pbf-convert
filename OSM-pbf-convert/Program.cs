﻿using System;
using System.Globalization;
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

            var configuration = new Configuration
            {
                ActionName = args[0],
                PbfFileName = args[1],
                CanReadExistingFiles = false
            };

            if (args.Length > 2)
            {
                configuration.WaysStartOffset = ulong.Parse(args[2], CultureInfo.InvariantCulture);
            }

            if (args.Length > 3)
            {
                configuration.RelationsStartOffset = ulong.Parse(args[3], CultureInfo.InvariantCulture);
            }

            if (args[0] == "blob-index")
                using (var indexer = PbfFileProcessor.Create(args[1],
                    new IdsIndexerBlobProcessor(args[1] + ".blobs.dat")))
                {
                    await indexer.Process().ConfigureAwait(false);
                }

            if (args[0] == "nodes-index")
                using (var indexer = PbfFileProcessor.Create(args[1], new NodesIndexBlobProcessor(args[1])))
                {
                    await indexer.Process().ConfigureAwait(false);
                }

            if (args[0] == "join")
                using (var indexer = PbfFileProcessor.Create(
                    args[1],
                    new NodesToWaysJoinProcessor(configuration),
                     0 //ulong.Parse(args[2])
                    ))
                {
                    await indexer.Process();
                }

            if (args[0] == "heat-map")
                using (var indexer = PbfFileProcessor.Create(args[1], new HeatMapProcessor(args[1])))
                {
                    await indexer.Process().ConfigureAwait(false);
                }

            if (args[0] == "tags")
                using (var indexer = PbfFileProcessor.Create(args[1], new TagsProcessor(args[1] + ".tags")))
                {
                    await indexer.Process().ConfigureAwait(false);
                }

            if (args[0] == "spatial")
                using (var indexer = PbfFileProcessor.Create(args[1], new SpatialProcessor(new SpatialIndex(configuration.DataPath))))
                {
                    await indexer.Process().ConfigureAwait(false);
                }

            if (args[0] == "ways-file")
                using (var processor = new WaysFileProcessor(args[1] + ".ways.dat"))
                {
                    processor.Process();
                }

            if (args[0] == "merge-rel")
                using (var processor = new RelationsProcessor(configuration))
                using (var fileProcessor = PbfFileProcessor.Create(args[1], processor, ulong.Parse(args[3])))
                {
                    await fileProcessor.Process().ConfigureAwait(false);
                }

            if (args[0] == "tags-stat")
            {
                using (var processor = new TagStatsProcessor(args[1]))
                {
                    using (var fileProcessor = PbfFileProcessor.Create(args[1], processor))
                    {
                        await fileProcessor.Process().ConfigureAwait(false);
                    }
                }
            }

            Console.WriteLine("Done!");
            Console.ReadKey();
        }
    }
}