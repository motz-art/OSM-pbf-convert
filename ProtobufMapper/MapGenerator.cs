using ProtocolBuffers;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Reflection;
using System.Linq.Expressions;
using System.IO;

namespace ProtobufMapper
{
    public class MapGenerator
    {
        Dictionary<Type, MapTypeConfiguration> typeConfigurations = new Dictionary<Type, MapTypeConfiguration>();

        public MapTypeConfiguration<T> Configure<T>()
        {
            var result = new MapTypeConfiguration<T>();
            typeConfigurations.Add(typeof(T), result);
            return result;
        }

        public Mapper<T> CreateMapper<T>() where T : new()
        {
            var typeConfiguration = typeConfigurations[typeof(T)];
            var readers = typeConfiguration.propertyConfigurations.ToDictionary(x => x.Value.Order, x => CreatePropertyReader<T>(x.Key, x.Value));
            return new Mapper<T>(readers);
        }

        private Func<ProtobufReader, T, Task> CreatePropertyReader<T>(PropertyInfo propertyInfo, PropertyConfiguration propertyConfiguration)
        {
            var callRead = CreateReadCall(propertyInfo.PropertyType, propertyConfiguration);
            var readerParam = GetReaderParam(callRead);

            var messageParam = Expression.Parameter(typeof(T));
            Type taskType = typeof(Task<>).MakeGenericType(propertyInfo.PropertyType);
            var valueParam = Expression.Parameter(taskType);
            Type assignActionType = typeof(Action<>).MakeGenericType(taskType);
            var assign = Expression.Lambda(assignActionType,
                    Expression.Assign(Expression.Property(messageParam, propertyInfo), 
                    Expression.Property(valueParam, "Result")), valueParam);

            var continueWithMethod = taskType.GetMethod("ContinueWith", new[] { assignActionType });

            var resultExpression = Expression.Lambda<Func<ProtobufReader, T, Task>>(
                    Expression.Call(callRead, continueWithMethod, assign), readerParam, messageParam);

            return resultExpression.Compile();
        }

        private ParameterExpression GetReaderParam(MethodCallExpression callRead)
        {
            return (ParameterExpression)callRead.Object;
        }

        private MethodCallExpression CreateReadCall(Type propertyType, PropertyConfiguration propertyConfiguration)
        {
            if (propertyType == typeof(int))
            {
                Expression<Func<ProtobufReader, Task<int>>> lambda = x => x.ReadInt32Async();
                return (MethodCallExpression)lambda.Body;
            }
            if (propertyType == typeof(long))
            {
                Expression<Func<ProtobufReader, Task<long>>> lambda = x => x.ReadInt64Async();
                return (MethodCallExpression)lambda.Body;
            }
            if (propertyType == typeof(ulong))
            {
                Expression<Func<ProtobufReader, Task<ulong>>> lambda = x => x.ReadUInt64Async();
                return (MethodCallExpression)lambda.Body;
            }
            if (propertyType == typeof(string))
            {
                Expression<Func<ProtobufReader, Task<string>>> lambda = x => x.ReadStringAsync();
                return (MethodCallExpression)lambda.Body;
            }
            if (propertyType == typeof(Stream))
            {
                Expression<Func<ProtobufReader, Task<Stream>>> lambda = x => x.ReadAsStreamAsync();
                return (MethodCallExpression)lambda.Body;
            }
            throw new NotSupportedException($"Properties of type {propertyType} are not supported.");
        }
    }

    public class Mapper<T> where T : new()
    {
        Dictionary<ulong, Func<ProtobufReader, T, Task>> propertyReaders;

        internal Mapper(Dictionary<ulong, Func<ProtobufReader, T, Task>> propertyReaders)
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
            while (reader.State != ProtobufReaderState.EndOfMessage)
            {
                Func<ProtobufReader, T, Task> propertyReader;
                if (propertyReaders.TryGetValue(reader.FieldNumber, out propertyReader))
                {
                    await propertyReader(reader, message);
                }
                else
                {
                    await reader.SkipAsync();
                }
            }
        }
    }
}
