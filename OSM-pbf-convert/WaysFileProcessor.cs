using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using HuffmanCoding;

namespace OSM_pbf_convert
{
    public class WaysFileProcessor : IDisposable
    {
        private Stream stream;

        public WaysFileProcessor(string fileName)
        {
            stream = File.Open(fileName, FileMode.Open, FileAccess.Read);
        }

        public void Process()
        {
            using (var reader = new BinaryReader(stream, Encoding.UTF8, true))
            {
                var ways = ReadAllWays(reader);
                
                var cnt = 0L;
                var maxNodes = 0L;
                var totalNodes = 0L;
                var watch = Stopwatch.StartNew();
                var show = TimeSpan.FromMilliseconds(1000);

                var lenStat = new int[80];

                var sizes = new List<int>();

                foreach (var way in ways)
                {
                    cnt++;

                    if (maxNodes < way.Nodes.Count)
                    {
                        maxNodes = way.Nodes.Count;
                    }

                    if (way.Nodes.Count < lenStat.Length)
                    {
                        lenStat[way.Nodes.Count]++;
                    }

                    var size = CalcBoxSize(way.Nodes);
                    sizes.Add(size);

                    totalNodes += way.Nodes.Count;

                    if (watch.Elapsed - show > TimeSpan.Zero)
                    {
                        Console.SetCursorPosition(0, 0);
                        Console.WriteLine($"Ways: {cnt:#,###}, MaxNodes: {maxNodes:#,###}, {(double)totalNodes / cnt:#.##} nodes/way. Elapsed: {watch.Elapsed}.  ");

                        for (int i = 0; i < lenStat.Length; i++)
                        {
                            Console.Write($" | {i.ToString().PadLeft(2)}: {lenStat[i].ToString("#,###").PadLeft(12)}");
                            if (i % 5 == 4)
                            {
                                Console.WriteLine();
                            }
                        }

                        show = watch.Elapsed + TimeSpan.FromMilliseconds(500);
                    }
                }

                {
                    var i = 0;
                    var j = 0;
                    var next = 0;
                    var step = (int)cnt / 50;
                    foreach (var s in sizes.OrderByDescending(x => x))
                    {
                        i++;
                        if (i >= next)
                        {
                            Console.Write($" | {j.ToString().PadLeft(3)}: {s.ToString("#,###").PadLeft(12)}");
                            next += step;
                            j++;
                            if (j % 5 == 0) Console.WriteLine();
                        }
                    }
                }

            }
        }

        private int CalcBoxSize(IList<MapNode> nodes)
        {
            var minLat = nodes.Min(x => x.Lat);
            var maxLat = nodes.Max(x => x.Lat);
            var minLon = nodes.Min(x => x.Lon);
            var maxLon = nodes.Max(x => x.Lon);

            return Math.Max(maxLat - minLat, maxLon - minLon);
        }

        private IEnumerable<MapWay> ReadAllWays(BinaryReader reader)
        {
            var id = 0UL;

            while (reader.BaseStream.Position < reader.BaseStream.Length)
            {
                id += reader.Read7BitEncodedInt();
                var count = (int)reader.Read7BitEncodedInt();
                var list = new List<MapNode>(count);
                var lat = 0;
                var lon = 0;
                for (int i = 0; i < count; i++)
                {
                    lat += (int) reader.Read7BitEncodedInt();
                    lon += (int) reader.Read7BitEncodedInt();

                    var node= new MapNode
                    {
                        Lat = lat,
                        Lon = lon
                    };

                    list.Add(node);
                }

                var way = new MapWay
                {
                    Id = id,
                    Nodes = list
                };

                yield return way;
            }
        }

        public void Dispose()
        {
            stream?.Dispose();
        }
    }

    public class MapWay
    {
        public ulong Id { get; set; }
        public IList<MapNode> Nodes { get; set; }
    }
}