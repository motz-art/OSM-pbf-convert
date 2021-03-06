﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using OsmReader;
using OsmReader.PbfDataObjects;

namespace OSM_pbf_convert
{
    public class TagStatsProcessor : IBlobProcessor<string>, IDisposable
    {
        public Dictionary<string, long> tagValuesStat = new Dictionary<string, long>();
        private long totalLength = 0;
        private long totalTagsCount = 0;
        private string fileName;
        private Stopwatch watch;
        private long nextShow = 1000;

        private int reset = 0;

        public TagStatsProcessor(string fileName)
        {
            this.fileName = fileName;
            watch = Stopwatch.StartNew();
        }

        public void Dispose()
        {
            watch.Stop();
        }

        public string BlobRead(Blob blob)
        {
            return String.Empty;
        }

        public void ProcessPrimitives(PrimitiveAccessor accessor, string data)
        {
            var present = false;
            foreach (var node in accessor.Nodes)
            {
                present = true;
                if (node.Tags != null)
                {
                    foreach (var tag in node.Tags)
                    {
                        AddTag(tag);
                    }
                }
            }

            if (!present && reset == 0)
            {
                reset = 1;
                WriteStats(".node-stats");
                tagValuesStat.Clear();
            }

            present = false;
            foreach (var way in accessor.Ways)
            {
                present = true;
                if (way.Tags != null)
                {
                    foreach (var tag in way.Tags)
                    {
                        AddTag(tag);
                    }
                }
            }

            if (!present && reset == 1)
            {
                reset = 2;
                WriteStats(".way-stats");
                tagValuesStat.Clear();
            }

            foreach (var relation in accessor.Relations)
            {
                if (relation.Tags != null)
                {
                    foreach (var tag in relation.Tags)
                    {
                        AddTag(tag);
                    }
                }
            }

            Console.Write($" Count: {tagValuesStat.Count:0,000}. Total: {totalTagsCount:0,000}. Length: {totalLength:0,000}.                 \r");

            if (watch.ElapsedMilliseconds > nextShow)
            {
                ShowTop();
                nextShow = watch.ElapsedMilliseconds  + 5000;
            }

            FilterStatIfNeeded();
        }

        private void ShowTop()
        {
            var top = tagValuesStat.Where(x => x.Value > 100).OrderByDescending(x => x.Value).Take(400);
            Console.WriteLine();
            foreach (var pair in top)
            {
                Console.Write($"{pair.Value.ToString("0,000").PadLeft(8)}:{pair.Key.PadRight(24).Substring(0, 24)} ");
                if (Console.CursorLeft > Console.WindowWidth - 36)
                {
                    if (Console.CursorTop >= Console.WindowHeight)
                    {
                        break;
                    }
                    Console.WriteLine();
                }
            }
            Console.SetCursorPosition(0,0);
        }

        private void FilterStatIfNeeded()
        {
            if (tagValuesStat.Count < 10_000_000)
                return;

            var keysToRemove = new List<string>();
            foreach (var pair in tagValuesStat)
            {
                if (pair.Value < 100)
                {
                    keysToRemove.Add(pair.Key);        
                }
            }

            Console.WriteLine($"Removing: ${keysToRemove.Count}.        ");

            foreach (var key in keysToRemove)
            {
                tagValuesStat.Remove(key);
            }
        }

        private void AddTag(OsmTag tag)
        {
            totalLength += tag.Key.Length;
            totalTagsCount++;

            var key = $"{tag.Key}";
            if (!tagValuesStat.TryGetValue(key, out var count))
            {
                count = 0;
            }

            count++;
            tagValuesStat[key] = count;
        }

        public void Finish()
        {
            WriteStats(".rest-stats");
        }

        private void WriteStats(string sufix = null)
        {
            if (sufix == null)
            {
                sufix = ".stat";
            }
            using (var statsFile = File.Create(fileName + sufix))
            using (var writer = new BinaryWriter(statsFile, Encoding.UTF8, true))
            {
                writer.Write(tagValuesStat.Count);
                foreach (var tag in tagValuesStat.OrderByDescending(x => x.Value).ThenBy(x => x.Key))
                {
                    writer.Write(tag.Key);
                    writer.Write(tag.Value);
                }
            }
        }

        public Dictionary<string, int> ReadStats(string fileName, int minCount = 10)
        {
            var res = new Dictionary<string, int>();
            using (var statsFile = File.Open(fileName, FileMode.Open, FileAccess.Read))
            using (var reader = new BinaryReader(statsFile, Encoding.UTF8, true))
            {
                var totalCount = reader.ReadInt32();
                for (int i = 0; i < totalCount; i++)
                {
                    var key = reader.ReadString();
                    var cnt = reader.ReadInt64();
                    if (cnt < minCount)
                    {
                        return res;
                    }
                    res.Add(key, i);
                }
            }

            return res;
        }
    }
}