using System;
using System.IO;

namespace OSM_pbf_convert
{
    internal class SubStream : Stream
    {
        private long length;
        private long startPosition;
        private long position = 0;
        private Stream stream;

        public SubStream(Stream stream, long startPosition, long length)
        {
            this.stream = stream;
            this.startPosition = startPosition;
            this.length = length;
        }

        public override bool CanRead
        {
            get
            {
                return true;
            }
        }

        public override bool CanSeek
        {
            get
            {
                return true;
            }
        }

        public override bool CanWrite
        {
            get
            {
                return false;
            }
        }

        public override long Length
        {
            get
            {
                return length;
            }
        }

        public override long Position
        {
            get
            {
                return position;
            }

            set
            {
                position = value;
            }
        }

        public override void Flush()
        {
            stream.Flush();
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            var oldPosition = stream.Position;
            stream.Position = startPosition + position;
            var countToRead = Math.Min(count, (int)(length - position));
            if (countToRead == 0)
            {
                return 0;
            }
            var result = stream.Read(buffer, offset, countToRead);
            position += result;
            stream.Position = oldPosition;
            return result;
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            long newPosition = 0;
            if (origin == SeekOrigin.Begin)
            {
                newPosition = offset;
            }
            else if (origin == SeekOrigin.End)
            {
                newPosition = length + offset;
            }
            else if (origin == SeekOrigin.Current)
            {
                newPosition = position + offset;
            }

            if (newPosition < 0 || newPosition > length)
            {
                throw new ArgumentOutOfRangeException("New position is out of range.");
            }

            position = newPosition;
            return position;
        }

        public override void SetLength(long value)
        {
            throw new NotSupportedException();
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            throw new NotSupportedException();
        }
    }
}