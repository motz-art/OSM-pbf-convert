using System;
using System.IO;
using System.Threading.Tasks;

namespace HuffmanCoding
{
    public class BufByteWriter : IDisposable
    {
        private Stream stream;

        private const int BufSize = 64 * 1024;
        private byte[] buf = new byte[BufSize];
        private int position = 0;

        private byte[] tmp = new byte[BufSize];
        private Task writeTask;

        public BufByteWriter(Stream stream)
        {
            this.stream = stream;
        }

        public void WriteByte(byte b)
        {
            buf[position++] = b;
            if (position >= buf.Length)
            {
                WriteBuf();
            }
        }

        private void WriteBuf()
        {
            if (writeTask != null)
            {
                writeTask.Wait();
                writeTask = null;
            }

            var tmpBuf = tmp;
            tmp = buf;
            buf = tmpBuf;

            var length = position;

            if (length > 0)
            {
                writeTask = WriteData(length);

            }

            position = 0;
        }

        private async Task WriteData(int length)
        {
            await stream.WriteAsync(tmp, 0, length);
        }

        public void Dispose()
        {
            Flush();
            if (writeTask != null)
            {
                writeTask.Wait();
                writeTask = null;
            }
        }

        public void Flush()
        {
            WriteBuf();
        }
    }
}