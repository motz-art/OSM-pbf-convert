using System;
using System.IO;
using System.Threading.Tasks;

namespace HuffmanCoding
{
    public class BufByteReader
    {
        private Stream stream;
        
        const int size = 64 * 1024;
        
        byte[] curBuf = new byte[size];
        private int curLength = 0;
        
        byte[] tmpBuf = new byte[size];
        private int tmpLength = 0;

        private int bufPosition = 0;

        private bool endOfFile = false;
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
            tmpLength = await stream.ReadAsync(tmpBuf, 0, tmpBuf.Length);
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
    }
}