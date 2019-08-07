using System;
using System.IO;
using System.Threading.Tasks;

namespace HuffmanCoding
{
    public class BufByteReader : IDisposable
    {
        private readonly Stream stream;

        private const int size = 64 * 1024;

        private byte[] curBuf = new byte[size];
        private int curLength;

        private byte[] tmpBuf = new byte[size];
        private int tmpLength;

        private int bufPosition;

        private bool endOfFile;
        private Task readTask;

        public BufByteReader(Stream stream)
        {
            this.stream = stream;
            StartRead();
        }

        private void StartRead()
        {
            readTask = Task.Run(ReadBuf);
        }

        private async Task ReadBuf()
        {
            tmpLength = await stream.ReadAsync(tmpBuf, 0, tmpBuf.Length).ConfigureAwait(false);
            if (tmpLength <= 0)
            {
                endOfFile = true;
            }
        }

        private void EndRead()
        {
            if (readTask == null)
            {
                StartRead();
            }

            readTask.Wait();
            if (tmpLength <= 0) return;

            var tmp = curBuf;
            curBuf = tmpBuf;
            tmpBuf = tmp;
            curLength = tmpLength;
            bufPosition = 0;
            tmpLength = 0;

            if (!endOfFile)
            {
                StartRead();
            }
        }

        public bool CanRead()
        {
            if (bufPosition < curLength)
            {
                return true;
            }
            EndRead();
            return !endOfFile;
        }

        public byte ReadByte()
        {
            if (!CanRead())
            {
                throw new InvalidOperationException("Can't read beyond end of stream.");
            }
            return curBuf[bufPosition++];
        }

        public void Dispose()
        {
        }
    }
}