using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using OSM_pbf_convert;

internal static class BlobIdsIndexer
{
    private static readonly List<BlobIdsInfo> infos = new List<BlobIdsInfo>();

    public static async Task Process(string fileName)
    {
        ulong maxBlobSize = 0;
        long maxDataSize = 0;
        var blobCnt = 0;
        long nodeCnt = 0;
        var poolCount = Environment.ProcessorCount + 2;
        var pool = new Semaphore(poolCount, poolCount);

        using (var stream = File.OpenRead(fileName))
        {
            try
            {
                var parser = new PbfBlobParser(stream);
                // parser.SkipBlob(1789874958 - 4);
                while (true)
                {
                    var blobHeader = await parser.ReadBlobHeader();
                    if (blobHeader == null) break;
                    blobCnt++;

                    if (blobCnt < 0)
                    {
                        parser.SkipBlob(blobHeader.DataSize);
                        Console.Write($" Skipped: {blobCnt}.         \r");
                        continue;
                    }

                    var message = await parser.ReadBlobAsync(blobHeader);
                    maxBlobSize = Math.Max(maxBlobSize, blobHeader.DataSize);
                    maxDataSize = Math.Max(maxDataSize, message.RawSize);

                    var headerType = blobHeader.Type;

                    var reader = PbfPrimitiveReader.Create(message);

                    var startPosition = blobHeader.StartPosition;
                    var info = new BlobIdsInfo
                    {
                        StartPosition = startPosition
                    };

                    infos.Add(info);

                    pool.WaitOne();

                    Console.Write($" Nodes: {nodeCnt.ToString("#,##0", CultureInfo.CurrentUICulture)}.\r");

                    Task.Run(async () =>
                    {
                        try
                        {
                            await ProcessBlob(headerType, reader, info);
                            Interlocked.Add(ref nodeCnt, info.NodesCount);
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

                for (var i = 0; i < poolCount; i++) pool.WaitOne();

                BlobIdsInfo.WriteIdsIndex(fileName, infos);
            }
            catch (Exception e)
            {
                Console.WriteLine($"Error processing file. After reading {stream.Position} of {stream.Length} bytes.");
                Console.WriteLine(e);
            }
        }

        Console.WriteLine(
            $"Statistic: max blob {maxBlobSize}, data: {maxDataSize}, total cnt: {blobCnt}, nodes: {nodeCnt}");
        Console.ReadLine();
    }

    public static async Task ProcessBlob(string blobHeaderType,
        PbfPrimitiveReader pbfPrimitiveReader, BlobIdsInfo info)
    {
        var nodeCnt = 0;
        var minId = long.MaxValue;
        var maxId = long.MinValue;
        var waysCnt = 0;
        var relationCnt = 0;

        if (blobHeaderType == "OSMHeader")
        {
            var header = await pbfPrimitiveReader.ReadHeader();
        }

        if (blobHeaderType == "OSMData")
        {
            var data = pbfPrimitiveReader.ReadData();

            if (data != null || data.PrimitiveGroup != null)
            {
                var nodes = PrimitiveDecoder.DecodeDenseNodes(data);

                foreach (var node in nodes)
                {
                    minId = Math.Min(minId, node.Id);
                    maxId = Math.Max(maxId, node.Id);
                    nodeCnt++;

                    var lon = (uint) Math.Round((node.Lon - 180) / 180 * uint.MaxValue);
                    var lat = (uint) Math.Round((node.Lat - 90) / 90 * uint.MaxValue);
                    //nodesIndexWriter.Write(node.Id, lon, lat);
                }

                foreach (var way in data.PrimitiveGroup.Where(x => x.Ways != null).SelectMany(x => x.Ways)) waysCnt++;

                foreach (var relation in data.PrimitiveGroup.Where(x => x.Relations != null)
                    .SelectMany(x => x.Relations)) relationCnt++;
            }
        }

        info.MinNodeId = minId;
        info.MaxNodeId = maxId;
        info.NodesCount = nodeCnt;
        info.WaysCount = waysCnt;
        info.RelationsCount = relationCnt;
    }
}