namespace ProtocolBuffers
{
    public static class EncodeHelpers
    {
        public static uint DecodeInt32BigEndian(byte[] bytes)
        {
            return (((uint)bytes[0]) << 24) + (((uint)bytes[1]) << 16) + (((uint)bytes[2]) << 24) + ((uint)bytes[3]);
        }

        public static long DecodeZigZag(ulong tmp)
        {
            var value = (long)(tmp >> 1);
            if ((tmp & 0x01) != 0)
            {
                value = -1 ^ value;
            }

            return value;
        }
    }
}