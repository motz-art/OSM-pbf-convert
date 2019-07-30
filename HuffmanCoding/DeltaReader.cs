using System;
using System.IO;
using ProtocolBuffers;

namespace HuffmanCoding
{
    public class DeltaReader
    {
        private readonly BinaryReader reader;
        private long lastSigned;

        private ulong lastUnsigned;

        public DeltaReader(BinaryReader reader)
        {
            this.reader = reader ?? throw new ArgumentNullException(nameof(reader));
        }

        public void Reset()
        {
            lastSigned = 0;
            lastUnsigned = 0;
        }

        public long ReadZigZag()
        {
            lastSigned += EncodeHelpers.DecodeZigZag(reader.Read7BitEncodedInt());
            return lastSigned;
        }

        public ulong ReadIncrementOnly()
        {
            lastUnsigned += reader.Read7BitEncodedInt();
            return lastUnsigned;
        }
    }
}