using System;
using System.IO;

namespace HuffmanCoding
{
    public static class StorageHelpers
    {
        public static void Write7BitEncodedInt(this BinaryWriter writer, long val)
        {
            Write7BitEncodedInt(writer, (ulong)val);
        }
        public static void Write7BitEncodedInt(this BinaryWriter writer, ulong val)
        {
            if (writer == null) throw new ArgumentNullException(nameof(writer));

            var buf = new byte[10];
            var i = buf.Length-1;

            buf[i] = (byte)(val & 0b0111_1111);
            val >>= 7;

            while (val != 0)
            {
                i--;
                buf[i] = (byte) (0b1000_0000 | (val & 0b0111_1111));
                val >>= 7;
            }

            writer.Write(buf, i, buf.Length - i);
        }
        public static void Write7BitEncodedInt(this BufByteWriter writer, ulong val)
        {
            if (writer == null) throw new ArgumentNullException(nameof(writer));

            var buf = new byte[10];
            var i = buf.Length-1;

            buf[i] = (byte)(val & 0b0111_1111);
            val >>= 7;

            while (val != 0)
            {
                i--;
                buf[i] = (byte) (0b1000_0000 | (val & 0b0111_1111));
                val >>= 7;
            }

            for (int j = i; j < buf.Length; j++)
            {
                writer.WriteByte(buf[j]);
            }
        }

        public static ulong Read7BitEncodedUInt64(this BinaryReader reader)
        {
            if (reader == null) throw new ArgumentNullException(nameof(reader));

            ulong res = 0;

            ulong b;

            do
            {
                b = reader.ReadByte();
                res = (res << 7) + (b & 0b0111_1111);
            } while (b >= 0b1000_0000);

            return res;
        }

        public static void Skip7BitInt(this BinaryReader reader)
        {
            if (reader == null) throw new ArgumentNullException(nameof(reader));

            byte b;
            do
            {
                b = reader.ReadByte();
            }while (b >= 0b1000_0000);
        }

        public static ulong Read7BitEncodedUInt64(this BufByteReader reader)
        {
            if (reader == null) throw new ArgumentNullException(nameof(reader));

            ulong res = 0;

            ulong b;

            do
            {
                b = reader.ReadByte();
                res = (res << 7) + (b & 0b0111_1111);
            } while (b >= 0b1000_0000);

            return res;
        }
    }
}