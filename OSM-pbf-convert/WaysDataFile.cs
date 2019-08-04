using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using HuffmanCoding;
using ProtocolBuffers;

namespace OSM_pbf_convert
{
    public class WaysDataFile : IDisposable
    {
        struct WayInfoOffset
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
        private readonly BinaryWriter infoWriter;
        private BinaryReader infoReader;

        private ulong totalWaysCount;
        private const int offsetsLimit = 1_000_000;
        private ulong skip = 8;
        List<WayInfoOffset> offsets = new List<WayInfoOffset>(offsetsLimit);

        public WaysDataFile(string fileName)
        {
            var exists = File.Exists(fileName);
            if (!exists)
            {
                waysStream = File.Open(fileName, FileMode.Create);
                waysWriter = new BinaryWriter(waysStream, Encoding.UTF8, true);

                idWriter = new DeltaWriter(waysWriter);
                nodeIdWriter = new DeltaWriter(waysWriter);
                latWriter = new DeltaWriter(waysWriter);
                lonWriter = new DeltaWriter(waysWriter);

                infoStream = File.Open(fileName + ".idx", FileMode.Create);
                infoWriter = new BinaryWriter(infoStream, Encoding.UTF8, true);
                infoReader = new BinaryReader(infoStream, Encoding.UTF8, true);
            }
            else
            {
                throw new NotImplementedException();
            }
        }

        public void Dispose()
        {
            waysWriter?.Dispose();
            waysStream?.Dispose();

            infoWriter?.Dispose();
            infoStream?.Dispose();
        }

        public void WriteWay(SWay way)
        {
            if (way == null) throw new ArgumentNullException(nameof(way));


            WriteWayData(way);

            AddOffsetsIndex(way);
            WriteWayInfo(way);
        }

        private void AddOffsetsIndex(SWay way)
        {
            totalWaysCount++;
            if (totalWaysCount % skip == skip - 1)
            {
                offsets.Add(new WayInfoOffset
                {
                    Id = (ulong) way.Id,
                    Offset = infoStream.Position
                });
                if (offsets.Count >= offsetsLimit)
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

        public WayInfo FindWayInfo(ulong id)
        {
            var offsetIndex = FindBlockIndex(id);
            if (offsetIndex < 0) return null;
            var offset = offsets[offsetIndex].Offset;

            infoStream.Position = offset;


            var offsetLimit = offsetIndex < offsets.Count - 1 ? offsets[offsetIndex + 1].Offset : infoStream.Length;
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
                            MaxLat = maxLat
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