using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using ProtocolBuffers;

namespace ProtobufMapper
{
    public class Mapper<T> where T : new()
    {
        readonly Dictionary<ulong, PropertyReader<T>> propertyReaders;

        internal Mapper(Dictionary<ulong, PropertyReader<T>> propertyReaders)
        {
            this.propertyReaders = propertyReaders;
        }

        public T ReadMessage(ProtobufReader reader)
        {
            return ReadMessageAsync(reader).Result;
        }

        public async Task<T> ReadMessageAsync(ProtobufReader reader)
        {
            var message = new T();
            await reader.BeginReadMessageAsync();
            await ReadMessagePropertiesAsync(reader, message);
            await reader.EndReadMessageAsync();
            return message;
        }

        public T ReadMessage(ProtobufReader reader, long size)
        {
            return ReadMessageAsync(reader, size).Result;
        }

        public async Task<T> ReadMessageAsync(ProtobufReader reader, long size)
        {
            var message = new T();
            await reader.BeginReadMessageAsync(size);
            await ReadMessagePropertiesAsync(reader, message);
            await reader.EndReadMessageAsync();
            return message;
        }

        private async Task ReadMessagePropertiesAsync(ProtobufReader reader, T message)
        {
            var num = 0;
            while (reader.State != ProtobufReaderState.EndOfMessage)
            {
                PropertyReader<T> propertyReader;
                if (propertyReaders.TryGetValue(reader.FieldNumber, out propertyReader))
                {
                    try
                    {
                        num++;
                        await propertyReader.Read(reader, message);
                    }
                    catch (Exception e)
                    {
                        throw new InvalidOperationException("", e);
                    }
                }
                else
                {
                    await reader.SkipAsync();
                }
            }
        }
    }
}