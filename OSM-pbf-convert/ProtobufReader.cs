using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;

namespace OSM_pbf_convert
{
    public class ProtobufReader
    {
        Stream stream;
        byte[] buf;
        int available = 0;
        int offset = 0;
        bool isEOF = false;
        Stack<long> messageStack = new Stack<long>();
        private ulong currentKey;

        public ProtobufReader(Stream stream)
        {
            this.stream = stream;
            buf = new byte[10224 * 8];
            messageStack.Push(stream.Length);
        }

        public long Position
        {
            get
            {
                return stream.Position + offset;
            }
        }

        public FieldTypes FieldType
        {
            get
            {
                return (FieldTypes)(currentKey & 0x07);
            }
        }

        public UInt64 FieldNumber
        {
            get
            {
                return currentKey >> 3;
            }
        }

        public async Task EndReadMessageAsync()
        {
            if (messageStack.Count == 1)
            {
                throw new InvalidOperationException("Message stack is empty.");
            }
            if (Position != messageStack.Peek())
            {
                throw new InvalidOperationException("Message is not read till the end.");
            }
            messageStack.Pop();
            if (messageStack.Count > 1)
            {
                await UpdateState();
            }
            else
            {
                State = ProtobufReaderState.None;
            }
        }

        internal void Skip()
        {
            throw new NotImplementedException(string.Format("Can't skip #{0} of type {1}.", FieldNumber, FieldType));
        }

        public async Task<Stream> ReadAsStreamAsync()
        {
            if (FieldType != FieldTypes.LengthDelimited)
            {
                throw new NotSupportedException();
            }
            var length = await ReadVarUInt64();
            var bytes = await ReadBytesAsync((uint)length);
            await UpdateState();
            return new MemoryStream(bytes);
        }

        public ProtobufReaderState State { get; private set;}

        public async Task<uint> ReadInt32BigEndian()
        {
            var bytes = await ReadBytesAsync(4);
            return (((uint)bytes[0]) << 24) + (((uint)bytes[1]) << 16) + (((uint)bytes[2]) << 24) + ((uint)bytes[3]);
        }

        public async Task<UInt64> ReadUInt64Async()
        {
            if (FieldType == FieldTypes.VarInt)
            {
                var result = await ReadVarUInt64();
                await UpdateState();
                return result;
            }
            throw new InvalidOperationException(FieldType + " is not supported wire type for UInt64.");
        }

        public async Task<string> ReadStringAsync()
        {
            if (FieldType != FieldTypes.LengthDelimited)
            {
                throw new InvalidOperationException("Not supported string types.");
            }
            var length = await ReadVarUInt64();
            var bytes = await ReadBytesAsync((uint)length);
            await UpdateState();
            return Encoding.UTF8.GetString(bytes);
        }

        public async Task<UInt64> ReadVarUInt64()
        {
            ulong result = 0;
            var shift = 0;
            byte b;
            do
            {
                b = await ReadByte();
                result = result + ((b & (ulong)0x7f) << shift);
                shift += 7;
                if (shift > 63)
                {
                    throw new InvalidOperationException("Can't read more than 64 bit as unsigned var int.");
                }
            } while ((b & 0x80) > 0);
            return result;
        }

        public async Task<Int64> ReadVarInt64()
        {
            Int64 result = 0;
            byte b;
            while (true)
            {
                b = await ReadByte();
                result = (result << 7) + (b & 0x7f);
                if ((b & 0x80) == 0)
                {
                    if ((b & 0x40) != 0)
                    {
                        result = -1 & result;
                    }

                    break;
                }
            }
            return result;
        }

        public async Task BeginReadMessage(long length)
        {
            if (length < 0)
            {
                throw new ArgumentException("length should be zero or greater.");
            }
            messageStack.Push(Position + length);
            await UpdateState();
        }

        private async Task UpdateState()
        {
            if (Position == messageStack.Peek())
            {
                State = ProtobufReaderState.EndOfMessage;
                return;
            }
            if (Position > messageStack.Peek())
            {
                throw new InvalidProgramException("Position should not be more then message boundary. Review ProtobufReader code.");
            }

            currentKey = await ReadVarUInt64();
            State = ProtobufReaderState.Field;
        }

        private async Task<byte> ReadByte()
        {
            if (isEOF)
            {
                throw new InvalidOperationException("Can't read beyond end of file.");
            }
            if (Position + 1 > messageStack.Peek())
            {
                throw new InvalidOperationException("Length exceeds message boundary.");
            }

            if (offset == available)
            {
                available = await stream.ReadAsync(buf, 0, buf.Length);
                offset = 0;
                if (available == 0)
                {
                    isEOF = true;
                    throw new InvalidOperationException("Can't read beyond end of file.");
                }
            }
            var i = offset;
            offset++;
            return buf[i];
        }

        private async Task<byte[]> ReadBytesAsync(uint length)
        {
            var result = new byte[length];
            var resultOffset = 0;
            if (Position + length > messageStack.Peek())
            {
                throw new InvalidOperationException("Length exceeds message boundary.");
            }
            while (true)
            {
                if (offset >= available)
                {
                    available = await stream.ReadAsync(buf, 0, buf.Length);
                    offset = 0;
                    if (available == 0)
                    {
                        isEOF = true;
                        throw new InvalidOperationException("EOF");
                    }
                }
                if (available - offset >= length - resultOffset)
                {
                    Array.Copy(buf, offset, result, resultOffset, length - resultOffset);
                    offset += (int)length - resultOffset;
                    return result;
                }
                if (available - offset < length - resultOffset)
                {
                    Array.Copy(buf, offset, result, resultOffset, available - offset);
                    resultOffset += available - offset;
                    offset = available;
                }
            }
        }

    }
}