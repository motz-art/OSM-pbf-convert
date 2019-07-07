using System;
using System.IO;

namespace HuffmanCoding
{
    public static class StorageHelpers
    {
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

        public static ulong Read7BitEncodedInt(this BinaryReader reader)
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