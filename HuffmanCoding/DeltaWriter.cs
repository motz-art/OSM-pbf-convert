using System;
using System.IO;
using ProtocolBuffers;

namespace HuffmanCoding
{
    public class DeltaWriter
    {
        private readonly BinaryWriter writer;

        private ulong last;
        private long lastSigned;

        public DeltaWriter(BinaryWriter writer)
        {
            this.writer = writer ?? throw new ArgumentNullException(nameof(writer));
        }

        public void Reset()
        {
            last = 0;
            lastSigned = 0;
        }

        public void WriteZigZag(long value)
        {
            var diff = value - lastSigned;
            writer.Write7BitEncodedInt(EncodeHelpers.EncodeZigZag(diff));
            lastSigned = value;
        }

        public void WriteIncrementOnly(ulong value)
        {
            if (value < last) throw new ArgumentException("value should not decrement!");

            var diff = value - last;
            writer.Write7BitEncodedInt(diff);
            last = value;
        }
    }
}