using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Text;
using HuffmanCoding;
using ProtocolBuffers;

namespace OSM_pbf_convert
{
    public class WaysDataFile : IDisposable
    {
        private struct WayInfoOffset
        {
            public ulong Id { get; set; }
            public long Offset { get; set; }
        }

        private readonly Stream waysStream;
        private readonly BinaryWriter waysWriter;

        private readonly DeltaWriter idWriter;
        private readonly DeltaWriter nodeIdWriter;
        private readonly DeltaWriter latWriter;
        private readonly DeltaWriter lonWriter;

        private readonly Stream infoStream;
        private MemoryMappedFile infoFile;
        private MemoryMappedViewAccessor infoAccessor;
        private MemoryMappedViewStream infoMemStream;
        private long infoLength;

        private readonly BinaryWriter infoWriter;
        private readonly BinaryReader infoReader;

        private ulong totalWaysCount;
        private const int OffsetsLimit = 1_000_000;
        private ulong skip = 50;
        private readonly List<WayInfoOffset> offsets = new List<WayInfoOffset>(OffsetsLimit);

        public WaysDataFile(string fileName)
        {
            var exists = File.Exists(fileName);
            var indexFileName = fileName + ".idx";
            if (!exists)
            {
                waysStream = File.Open(fileName, FileMode.Create);
                infoStream = File.Open(indexFileName, FileMode.Create);
            }
            else
            {
                waysStream = new FileStream(fileName, FileMode.Open, FileAccess.ReadWrite, 
                    FileShare.None, 4096, FileOptions.RandomAccess);
//
//                infoStream = new FileStream(indexFileName, FileMode.Open, FileAccess.ReadWrite,
//                    FileShare.None, 65536, FileOptions.SequentialScan);
            
                infoFile = MemoryMappedFile.CreateFromFile(indexFileName, FileMode.Open, "waysIndex");
                infoStream = infoFile.CreateViewStream();
            }
            waysWriter = new BinaryWriter(waysStream, Encoding.UTF8, true);

            idWriter = new DeltaWriter(waysWriter);
            nodeIdWriter = new DeltaWriter(waysWriter);
            latWriter = new DeltaWriter(waysWriter);
            lonWriter = new DeltaWriter(waysWriter);

            infoWriter = new BinaryWriter(infoStream, Encoding.UTF8, true);
            infoReader = new BinaryReader(infoStream, Encoding.UTF8, true);

            if (exists)
            {
                var info = new FileInfo(indexFileName);
                infoLength = info.Length;
                ReadOffsets();
            }
        }

        public void Dispose()
        {
            waysWriter?.Dispose();
            waysStream?.Dispose();

            infoWriter?.Dispose();
            infoStream?.Dispose();
            infoFile?.Dispose();
        }

        public void WriteWay(SWay way)
        {
            if (way == null) throw new ArgumentNullException(nameof(way));


            WriteWayData(way);

            AddOffsetsIndex((ulong) way.Id, infoStream.Position);
            WriteWayInfo(way);
        }

        private void AddOffsetsIndex(ulong wayId, long offset)
        {
            totalWaysCount++;
            if (totalWaysCount % skip == skip - 1)
            {
                offsets.Add(new WayInfoOffset
                {
                    Id = wayId,
                    Offset = offset
                });
                if (offsets.Count >= OffsetsLimit)
                {
                    skip += skip;
                    int j = 0;
                    for (int i = 1; i < offsets.Count; i += 2)
                    {
                        offsets[j] = offsets[i];
                        j++;
                    }

                    offsets.RemoveRange(j, offsets.Count - j);
                }
            }
        }

        public void Flush()
        {
            infoWriter.Flush();
            infoStream.Flush();

            waysWriter.Flush();
            waysStream.Flush();
        }

        private void ReadOffsets()
        {
            var watch = Stopwatch.StartNew();
            long nextShow = 300;
            long cnt = 0;

            while (infoStream.Position < infoLength)
            {
                if (watch.ElapsedMilliseconds >= nextShow)
                {
                    nextShow += 100;
                    Console.Write($"{infoStream.Position:0,000}. Read: {infoStream.Position/watch.ElapsedMilliseconds:0,000} KB/s, Qty: {cnt/watch.Elapsed.TotalSeconds:0,000}/s.    \r");
                }

                cnt++;
                var offset = infoStream.Position;
                var wayId = infoReader.Read7BitEncodedInt();

                AddOffsetsIndex(wayId, offset);

                infoReader.ReadInt64(); // Skip 8 bytes;
                infoReader.Skip7BitInt();
                infoReader.Skip7BitInt();
                infoReader.Skip7BitInt();
                infoReader.Skip7BitInt();
            }

        }

        public WayInfo FindWayInfo(ulong id)
        {
            var offsetIndex = FindBlockIndex(id);
            if (offsetIndex < 0) return null;
            var offset = offsets[offsetIndex].Offset;

            infoStream.Position = offset;


            var offsetLimit = offsetIndex < offsets.Count - 1 ? offsets[offsetIndex + 1].Offset : infoLength;
            while (infoStream.Position < offsetLimit)
            {
                var cid = infoReader.Read7BitEncodedInt();
                if (cid == id)
                {
                    var minLat = infoReader.ReadInt32();
                    var minLon = infoReader.ReadInt32();

                    var midLat = (int)EncodeHelpers.DecodeZigZag(infoReader.Read7BitEncodedInt()) + minLat;
                    var midLon = (int)EncodeHelpers.DecodeZigZag(infoReader.Read7BitEncodedInt()) + minLon;

                    var maxLat = (int)EncodeHelpers.DecodeZigZag(infoReader.Read7BitEncodedInt()) + midLat;
                    var maxLon = (int)EncodeHelpers.DecodeZigZag(infoReader.Read7BitEncodedInt()) + midLon;
                    return new WayInfo
                    {
                        Id = cid,
                        MidLat = midLat,
                        MidLon = midLon,
                        Rect = new BoundingRect
                        {
                            MinLat = minLat,
                            MinLon = minLon,
                            MaxLat = maxLat,
                            MaxLon = maxLon
                        }
                    };
                }

                infoStream.Position += 8;
                infoReader.Skip7BitInt();
                infoReader.Skip7BitInt();
                infoReader.Skip7BitInt();
                infoReader.Skip7BitInt();
            }

            return null;
        }

        private void WriteWayInfo(SWay way)
        {
            infoWriter.Write7BitEncodedInt(way.Id);
            var rect = way.Rect;
            infoWriter.Write(rect.MinLat);
            infoWriter.Write(rect.MinLon);

            infoWriter.Write7BitEncodedInt(EncodeHelpers.EncodeZigZag(way.MidLat - rect.MinLat));
            infoWriter.Write7BitEncodedInt(EncodeHelpers.EncodeZigZag(way.MidLon - rect.MinLon));

            infoWriter.Write7BitEncodedInt(EncodeHelpers.EncodeZigZag(rect.MaxLat - way.MidLat));
            infoWriter.Write7BitEncodedInt(EncodeHelpers.EncodeZigZag(rect.MaxLon - way.MidLon));

            infoLength = infoStream.Position;
        }


        private int FindBlockIndex(ulong nodeId)
        {
            var lowIndex = 0;
            var lowId = offsets[lowIndex].Id;
            if (nodeId < lowId)
            {
                return lowIndex - 1;
            }

            var highIndex = offsets.Count - 1;
            var highId = offsets[highIndex].Id;
            if (nodeId >= highId)
            {
                return highIndex;
            }

            while (highIndex - lowIndex > 1)
            {
                var i = (highIndex + lowIndex) >> 1;

                var cId = offsets[i].Id;

                if (nodeId == cId)
                {
                    return i;
                }

                if (nodeId > cId)
                {
                    lowIndex = i;
                }
                else if (nodeId < cId)
                {
                    highIndex = i;
                }
            }

            return lowIndex;
        }

        private void WriteWayData(SWay way)
        {
            idWriter.WriteIncrementOnly((ulong) way.Id);

            var nodes = way.Nodes;

            waysWriter.Write7BitEncodedInt((ulong) nodes.Count);

            nodeIdWriter.Reset();
            latWriter.Reset();
            lonWriter.Reset();

            foreach (var node in nodes)
            {
                nodeIdWriter.WriteZigZag(node.Id);
                latWriter.WriteZigZag(node.Lat);
                lonWriter.WriteZigZag(node.Lon);
            }
        }
    }

    public class WayInfo
    {
        public ulong Id { get; set; }
        public int MidLat { get; set; }
        public int MidLon { get; set; }
        public BoundingRect Rect { get; set; }
    }
}