using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using HuffmanCoding;

namespace OSM_pbf_convert
{
    internal static class SpatialData
    {
        public static void WriteSequence(IEnumerable<ulong> buf, string name)
        {
            var watch = Stopwatch.StartNew();
            ulong cnt = 0;
            using (var stream = File.Create(name))
            {
                using (var writer = new BufByteWriter(stream))
                {
                    ulong lastIndex = 0;

                    foreach (var index in buf)
                    {
                        cnt++;

                        writer.Write7BitEncodedInt(index - lastIndex);

                        lastIndex = index;
                    }
                }
            }

            watch.Stop();
            Console.WriteLine(
                $"Written: {cnt}, in {watch.Elapsed}, speed: {cnt / watch.Elapsed.TotalSeconds / 1000} k/s.");
        }

        public static IEnumerable<ulong> ReadSequence(string name)
        {
            using (var stream = File.OpenRead(name))
            {
                ulong last = 0;
                using (var reader = new BufByteReader(stream))
                {
                    while (reader.CanRead())
                    {
                        last += reader.Read7BitEncodedUInt64();
                        yield return last;
                    }
                }
            }
        }
    }
}