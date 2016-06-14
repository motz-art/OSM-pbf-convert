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
        long position = 0;
        bool isEOF = false;
        Stack<long> messageStack = new Stack<long>();
        private ulong currentKey;

        public ProtobufReader(Stream stream, long length)
        {
            this.stream = stream;
            messageStack.Push(length);
        }

        public long Position
        {
            get
            {
                return position;
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

        public async Task SkipAsync()
        {
            switch (FieldType)
            {
                case FieldTypes.VarInt:
                    await ReadVarUInt64();
                    break;
                case FieldTypes.LengthDelimited:
                    var length = await ReadVarUInt64();
                    await ReadBytesAsync((uint)length);
                    break;
                case FieldTypes.Fixed32:
                    await ReadBytesAsync(4);
                    break;
                case FieldTypes.Fixed64:
                    await ReadBytesAsync(8);
                    break;
                default:
                    throw new NotImplementedException(string.Format("Can't skip #{0} of type {1}.", FieldNumber, FieldType));
            }
            await UpdateState();
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

        public async Task<long> ReadSInt64Async()
        {
            var tmp = await ReadUInt64Async();
            long value = ZigZagDecode(tmp);
            return value;
        }

        private static long ZigZagDecode(ulong tmp)
        {
            var value = (long)(tmp >> 1);
            if ((tmp & 0x01) != 0)
            {
                value = -1 ^ value;
            }

            return value;
        }

        public async Task<long> ReadInt64Async()
        {
            return (long)await ReadUInt64Async();
        }

        public ProtobufReaderState State { get; private set; }

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
                b = await ReadByteAsync();
                result = result + ((b & (ulong)0x7f) << shift);
                shift += 7;
                if (shift > 63)
                {
                    throw new InvalidOperationException("Can't read more than 64 bit as unsigned var int.");
                }
            } while ((b & 0x80) > 0);
            return result;
        }

        public async Task<Int64> ReadVarInt64Async()
        {
            return (long)await ReadUInt64Async();
        }

        public async Task BeginReadMessage()
        {
            if (FieldType == FieldTypes.LengthDelimited)
            {
                var size = await ReadVarUInt64();
                await BeginReadMessage((long)size);
            }
            else
            {
                throw new InvalidOperationException($"Cant start reading message from {FieldType} field.");
            }
        }

        public async Task BeginReadMessage(long length)
        {
            if (length < 0)
            {
                throw new ArgumentException("length should be zero or greater.");
            }
            var endOfMessage = Position + length;
            if (endOfMessage > messageStack.Peek())
            {
                throw new ArgumentException("Length of message is out of parrent message bound.");
            }
            messageStack.Push(endOfMessage);
            await UpdateState();
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

        public async Task<IEnumerable<long>> ReadPaskedSInt64ArrayAsync()
        {
            var result = new List<long>();
            if (FieldType == FieldTypes.LengthDelimited)
            {
                var size = await ReadVarUInt64();
                var endPosition = Position + (long)size;
                while (Position < endPosition)
                {
                    result.Add(ZigZagDecode(await ReadVarUInt64()));
                }
                await UpdateState();
            }
            else
            {
                result.Add(await ReadSInt64Async());
            }
            return result;
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

        private async Task<byte> ReadByteAsync()
        {
            if (isEOF)
            {
                throw new InvalidOperationException("Can't read beyond end of file.");
            }
            if (Position >= messageStack.Peek())
            {
                throw new InvalidOperationException("Length exceeds message boundary.");
            }
            var buf = new byte[1];
            var bytesRead = await stream.ReadAsync(buf, 0, 1);
            if (bytesRead == 0)
            {
                isEOF = true;
                throw new InvalidOperationException("Can't read beyond end of file.");
            }
            position += bytesRead;
            return buf[0];
        }

        private async Task<byte[]> ReadBytesAsync(uint length)
        {
            var result = new byte[length];
            int resultOffset = 0;
            if (Position + length > messageStack.Peek())
            {
                throw new InvalidOperationException("Length exceeds message boundary.");
            }

            while (resultOffset < length)
            {
                var bytesRead = await stream.ReadAsync(result, resultOffset, 
                    (int)length - resultOffset);
                if (bytesRead == 0)
                {
                    throw new InvalidOperationException("Unexpected end of file.");
                }
                position += bytesRead;
                resultOffset += bytesRead;
            }
            return result;
        }

    }
}