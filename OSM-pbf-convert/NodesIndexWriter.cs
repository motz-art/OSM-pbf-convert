using System;
using System.IO;

namespace OSM_pbf_convert
{
    internal class NodesIndexWriter
    {
        private Stream nodesStream;
        private byte[] buffer;
        private long writePosition;

        public NodesIndexWriter(Stream nodesStream)
        {
            this.nodesStream = nodesStream;
            buffer = new byte[64 * 1024];
        }

        public void Write(long id, uint lon, uint lat)
        {
            return;
            var position = id * 8;
            var offset = position - writePosition;
            if (offset < 0 || offset >= buffer.Length)
            {
                nodesStream.Position = writePosition;
                nodesStream.Write(buffer, 0, buffer.Length);

                offset = position % buffer.Length;
                writePosition = position - offset;

                if (writePosition < nodesStream.Length)
                {
                    nodesStream.Read(buffer, 0, buffer.Length);
                }
                else
                {
                    Array.Clear(buffer, 0, buffer.Length);
                }
            }

            buffer[offset + 0] = (byte)lon;
            buffer[offset + 1] = (byte)(lon >> 8);
            buffer[offset + 2] = (byte)(lon >> 16);
            buffer[offset + 3] = (byte)(lon >> 24);
            buffer[offset + 4] = (byte)lat;
            buffer[offset + 5] = (byte)(lat >> 8);
            buffer[offset + 6] = (byte)(lat >> 16);
            buffer[offset + 7] = (byte)(lat >> 24);
        }
    }
}