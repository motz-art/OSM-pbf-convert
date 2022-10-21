using System;
using System.IO;
using ProtocolBuffers;
using HuffmanCoding;

namespace OSM_pbf_convert
{
    public class BoundingRect
    {
        public int MaxLat { get; set; }

        public int MinLat { get; set; }

        public int MaxLon { get; set; }

        public int MinLon { get; set; }
        public int LatSize => MaxLat - MinLat;
        public int LonSize => MaxLon - MinLon;

        public BoundingRect()
        {
            Reset();
        }

        public BoundingRect Clone()
        {
            return new BoundingRect
            {
                MaxLat = MaxLat,
                MinLat = MinLat,
                MaxLon = MaxLon,
                MinLon = MinLon
            };
        }

        public void Reset()
        {
            MaxLat = int.MinValue;
            MinLat = int.MaxValue;
            MaxLon = int.MinValue;
            MinLon = int.MaxValue;
        }

        public BoundingRect Extend(int lat, int lon)
        {
            MinLat = Math.Min(MinLat, lat);
            MaxLat = Math.Max(MaxLat, lat);
            MinLon = Math.Min(MinLon, lon);
            MaxLon = Math.Max(MaxLon, lon);
            return this;
        }

        public BoundingRect Extend(BoundingRect other)
        {
            MinLat = Math.Min(MinLat, other.MinLat);
            MaxLat = Math.Max(MaxLat, other.MaxLat);
            MinLon = Math.Min(MinLon, other.MinLon);
            MaxLon = Math.Max(MaxLon, other.MaxLon);
            return this;
        }

        public bool Contains(int lat, int lon)
        {
            return MinLat < lat && MaxLat >= lat && MinLon < lon && MaxLon >= lon;
        }

        public override string ToString()
        {
            return
                $"Lat: {Helpers.IntToCoord(MinLat):#.####} / {Helpers.IntToCoord(MaxLat):#.####} x Lon: {Helpers.IntToCoord(MinLon):#.####} / {Helpers.IntToCoord(MaxLon):#.####}";
        }
    }

    public static class BoundingRectReadWriteHelper
    {
        public static void WriteTo(this BoundingRect rect, BinaryWriter writer)
        {
            writer.Write7BitEncodedInt(EncodeHelpers.EncodeZigZag(rect.MinLat));
            writer.Write7BitEncodedInt(EncodeHelpers.EncodeZigZag(rect.MinLon));
            writer.Write7BitEncodedInt(rect.MaxLat - rect.MinLat);
            writer.Write7BitEncodedInt(rect.MaxLon - rect.MinLon);
        }

        public static void SkipBoundingRect(this BinaryReader reader)
        {
            reader.Skip7BitInt();
            reader.Skip7BitInt();
            reader.Skip7BitInt();
            reader.Skip7BitInt();
        }

        public static BoundingRect ReadBoundingRect(this BinaryReader reader)
        {
            var minLat = (int)EncodeHelpers.DecodeZigZag(reader.Read7BitEncodedInt());
            var minLon = (int)EncodeHelpers.DecodeZigZag(reader.Read7BitEncodedInt());

            var sizeLat = (int) reader.Read7BitEncodedInt();
            var sizeLon = (int) reader.Read7BitEncodedInt();
            return new BoundingRect
            {
                MinLat = minLat,
                MinLon = minLon,
                MaxLat = minLat + sizeLat,
                MaxLon = minLon + sizeLon
            };
        }
    }
}