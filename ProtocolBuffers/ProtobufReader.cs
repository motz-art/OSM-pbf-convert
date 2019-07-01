using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ProtocolBuffers
{
    public class ProtobufReader
    {
        private readonly Stream stream;
        private readonly Stack<long> messageStack = new Stack<long>();
        private ulong currentKey;

        public ProtobufReader(Stream stream, long length)
        {
            this.stream = stream;
            messageStack.Push(length);
        }

        public long Position { get; private set; } = 0;

        public FieldTypes FieldType => (FieldTypes)(currentKey & 0x07);

        public ulong FieldNumber => currentKey >> 3;

        public ProtobufReaderState State { get; private set; }

        public void Skip()
        {
            switch (FieldType)
            {
                case FieldTypes.VarInt:
                    ReadVarUInt64();
                    break;
                case FieldTypes.LengthDelimited:
                    var length = ReadVarUInt64();
                    ReadBytes((uint)length);
                    break;
                case FieldTypes.Fixed32:
                    ReadBytes(4);
                    break;
                case FieldTypes.Fixed64:
                    ReadBytes(8);
                    break;
                default:
                    throw new NotSupportedException($"Can't skip field number {FieldNumber} of type {FieldType}.");
            }
            UpdateState();
        }

        public async Task SkipAsync()
        {
            switch (FieldType)
            {
                case FieldTypes.VarInt:
                    await ReadVarUInt64Async();
                    break;
                case FieldTypes.LengthDelimited:
                    var length = await ReadVarUInt64Async();
                    await ReadBytesAsync((uint)length);
                    break;
                case FieldTypes.Fixed32:
                    await ReadBytesAsync(4);
                    break;
                case FieldTypes.Fixed64:
                    await ReadBytesAsync(8);
                    break;
                default:
                    throw new NotSupportedException($"Can't skip field number {FieldNumber} of type {FieldType}.");
            }
            await UpdateStateAsync();
        }

        public Stream ReadAsStream()
        {
            if (FieldType != FieldTypes.LengthDelimited)
            {
                throw new NotSupportedException();
            }
            var length = ReadVarInt64();
            if (stream.CanSeek)
            {
                var resultStream = new SubStream(stream, stream.Position, length);
                stream.Position = stream.Position + length;
                Position = Position + length;
                UpdateState();
                return resultStream;
            }
            var bytes = ReadBytes((uint)length);
            UpdateState();
            return new MemoryStream(bytes);
        }

        public async Task<Stream> ReadAsStreamAsync()
        {
            if (FieldType != FieldTypes.LengthDelimited)
            {
                throw new NotSupportedException();
            }
            var length = await ReadVarInt64Async();
            if (stream.CanSeek)
            {
                var resultStream = new SubStream(stream, stream.Position, length);
                stream.Position = stream.Position + length;
                Position = Position + length;
                await UpdateStateAsync();
                return resultStream;
            }
            var bytes = await ReadBytesAsync((uint)length);
            await UpdateStateAsync();
            return new MemoryStream(bytes);
        }

        public long ReadSInt64()
        {
            var tmp = ReadUInt64();
            long value = EncodeHelpers.DecodeZigZag(tmp);
            return value;
        }

        public async Task<long> ReadSInt64Async()
        {
            var tmp = await ReadUInt64Async();
            long value = EncodeHelpers.DecodeZigZag(tmp);
            return value;
        }

        public long ReadInt64()
        {
            return (long)ReadUInt64();
        }

        public async Task<long> ReadInt64Async()
        {
            return (long)await ReadUInt64Async();
        }

        public uint ReadInt32BigEndian()
        {
            var bytes = ReadBytes(4);
            return EncodeHelpers.DecodeInt32BigEndian(bytes);
        }

        public async Task<uint> ReadInt32BigEndianAsync()
        {
            var bytes = await ReadBytesAsync(4);
            return EncodeHelpers.DecodeInt32BigEndian(bytes);
        }

        public ulong ReadUInt64()
        {
            if (FieldType == FieldTypes.VarInt)
            {
                var result = ReadVarUInt64();
                UpdateState();
                return result;
            }
            throw new InvalidOperationException(FieldType + " is not supported wire type for UInt64.");
        }

        public async Task<ulong> ReadUInt64Async()
        {
            if (FieldType == FieldTypes.VarInt)
            {
                var result = await ReadVarUInt64Async();
                await UpdateStateAsync();
                return result;
            }
            throw new InvalidOperationException(FieldType + " is not supported wire type for UInt64.");
        }

        public string ReadString()
        {
            if (FieldType != FieldTypes.LengthDelimited)
            {
                throw new InvalidOperationException("Not supported string types.");
            }
            var length = ReadVarUInt64();
            var bytes = ReadBytes((uint)length);
            UpdateState();
            return Encoding.UTF8.GetString(bytes);
        }

        public async Task<string> ReadStringAsync()
        {
            if (FieldType != FieldTypes.LengthDelimited)
            {
                throw new InvalidOperationException("Not supported string types.");
            }
            var length = await ReadVarUInt64Async();
            var bytes = await ReadBytesAsync((uint)length);
            await UpdateStateAsync();
            return Encoding.UTF8.GetString(bytes);
        }

        public int ReadInt32()
        {
            return (int)ReadInt64();
        }

        public async Task<int> ReadInt32Async()
        {
            return (int)await ReadInt64Async();
        }

        public ulong ReadVarUInt64()
        {
            ulong result = 0;
            var shift = 0;
            byte b;
            do
            {
                b = ReadByte();
                result = result + ((b & (ulong)0x7f) << shift);
                shift += 7;
                if (shift > 63)
                {
                    throw new InvalidOperationException("Can't read more than 64 bit as unsigned var int.");
                }
            } while ((b & 0x80) > 0);
            return result;
        }

        public async Task<ulong> ReadVarUInt64Async()
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

        public long ReadVarInt64()
        {
            return (long)ReadVarUInt64();
        }

        public async Task<long> ReadVarInt64Async()
        {
            return (long)await ReadVarUInt64Async();
        }

        public void BeginReadMessage()
        {
            if (FieldType == FieldTypes.LengthDelimited)
            {
                var size = ReadVarUInt64();
                BeginReadMessage((long)size);
            }
            else
            {
                throw new InvalidOperationException($"Cant start reading message from {FieldType} field.");
            }
        }

        public void BeginReadMessage(long length)
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
            UpdateState();
        }

        public async Task BeginReadMessageAsync()
        {
            if (FieldType == FieldTypes.LengthDelimited)
            {
                var size = await ReadVarUInt64Async();
                await BeginReadMessageAsync((long)size);
            }
            else
            {
                throw new InvalidOperationException($"Cant start reading message from {FieldType} field.");
            }
        }

        public async Task BeginReadMessageAsync(long length)
        {
            if (length < 0)
            {
                throw new ArgumentException("length should be zero or greater.");
            }
            var endOfMessage = Position + length;
            if (endOfMessage > messageStack.Peek())
            {
                throw new ArgumentException("Length of message is out of parent message bound.");
            }
            messageStack.Push(endOfMessage);
            await UpdateStateAsync();
        }

        public void EndReadMessage()
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
                UpdateState();
            }
            else
            {
                State = ProtobufReaderState.None;
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
                await UpdateStateAsync();
            }
            else
            {
                State = ProtobufReaderState.None;
            }
        }

        public long[] ReadPackedInt64Array()
        {
            var result = new List<long>();
            if (FieldType == FieldTypes.LengthDelimited)
            {
                var size = ReadVarUInt64();
                var endPosition = Position + (long)size;
                while (Position < endPosition)
                {
                    result.Add(ReadVarInt64());
                }
                UpdateState();
            }
            else
            {
                result.Add(ReadSInt64());
            }
            return result.ToArray();
        }

        public async Task<int[]> ReadPackedInt32ArrayAsync()
        {
            var result = new List<int>();
            if (FieldType == FieldTypes.LengthDelimited)
            {
                var size = await ReadVarUInt64Async();
                var endPosition = Position + (long)size;
                while (Position < endPosition)
                {
                    result.Add((int)await ReadVarInt64Async());
                }
                await UpdateStateAsync();
            }
            else
            {
                result.Add((int)await ReadSInt64Async());
            }
            return result.ToArray();
        }

        public async Task<long[]> ReadPackedInt64ArrayAsync()
        {
            var result = new List<long>();
            if (FieldType == FieldTypes.LengthDelimited)
            {
                var size = await ReadVarUInt64Async();
                var endPosition = Position + (long)size;
                while (Position < endPosition)
                {
                    result.Add(await ReadVarInt64Async());
                }
                await UpdateStateAsync();
            }
            else
            {
                result.Add(await ReadSInt64Async());
            }
            return result.ToArray();
        }

        public long[] ReadPackedSInt64Array()
        {
            var result = ReadPackedInt64Array();
            return result.Select(x => EncodeHelpers.DecodeZigZag((ulong)x)).ToArray();
        }

        public async Task<long[]> ReadPackedSInt64ArrayAsync()
        {
            var result = await ReadPackedInt64ArrayAsync();
            return result.Select(x => EncodeHelpers.DecodeZigZag((ulong)x)).ToArray();
        }

        private void UpdateState()
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

            currentKey = ReadVarUInt64();
            State = ProtobufReaderState.Field;
        }

        private async Task UpdateStateAsync()
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

            currentKey = await ReadVarUInt64Async();
            State = ProtobufReaderState.Field;
        }

        private byte ReadByte()
        {
            var buf = ReadBytes(1);
            return buf[0];
        }

        private async Task<byte> ReadByteAsync()
        {
            var buf = await ReadBytesAsync(1);
            return buf[0];
        }

        private byte[] ReadBytes(uint length)
        {
            var result = new byte[length];
            int resultOffset = 0;
            if (Position + length > messageStack.Peek())
            {
                throw new InvalidOperationException("Length exceeds message boundary.");
            }

            while (resultOffset < length)
            {
                var bytesRead = stream.Read(result, resultOffset,
                    (int)length - resultOffset);
                if (bytesRead == 0)
                {
                    throw new InvalidOperationException("Unexpected end of file.");
                }
                Position += bytesRead;
                resultOffset += bytesRead;
            }
            return result;
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
                Position += bytesRead;
                resultOffset += bytesRead;
            }
            return result;
        }
    }

}
