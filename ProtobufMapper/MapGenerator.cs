using ProtocolBuffers;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Reflection;
using System.Linq.Expressions;
using System.IO;

namespace ProtobufMapper
{
    public class MapGenerator
    {
        private readonly Dictionary<Type, MapTypeConfiguration> typeConfigurations = new Dictionary<Type, MapTypeConfiguration>();

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

        private PropertyReader<T> CreatePropertyReader<T>(PropertyInfo propertyInfo, PropertyConfiguration propertyConfiguration)
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
                    Expression.Call(callRead.Body, continueWithMethod, assign), readerParam, messageParam);

            var reader = resultExpression.Compile();

            return new PropertyReader<T>
            {
                PropertyName = propertyInfo.Name,
                Read = reader
            };
        }

        private ParameterExpression GetReaderParam(LambdaExpression lambdaExpression)
        {
            return lambdaExpression.Parameters.Single();
        }

        private LambdaExpression CreateReadCall(Type propertyType, PropertyConfiguration propertyConfiguration)
        {
            if (propertyType == typeof(int))
            {
                Expression<Func<ProtobufReader, Task<int>>> lambda = x => x.ReadInt32Async();
                return lambda;
            }
            if (propertyType == typeof(long))
            {
                Expression<Func<ProtobufReader, Task<long>>> lambda = x => x.ReadInt64Async();
                return lambda;
            }
            if (propertyType == typeof(ulong))
            {
                Expression<Func<ProtobufReader, Task<ulong>>> lambda = x => x.ReadUInt64Async();
                return lambda;
            }
            if (propertyType == typeof(string))
            {
                Expression<Func<ProtobufReader, Task<string>>> lambda = x => x.ReadStringAsync();
                return lambda;
            }
            if (propertyType == typeof(Stream))
            {
                Expression<Func<ProtobufReader, Task<Stream>>> lambda = x => x.ReadAsStreamAsync();
                return lambda;
            }

            if (propertyType == typeof(long[]))
            {
                Expression<Func<ProtobufReader, Task<long[]>>> lambda = x => x.ReadPackedSInt64ArrayAsync();
                return lambda;
            }

            if (propertyType == typeof(List<long>))
            {
                Expression<Func<ProtobufReader, Task<List<long>>>> lambda = x => ListFromArray(x.ReadPackedSInt64ArrayAsync());

                return lambda;
            }

            throw new NotSupportedException($"Properties of type {propertyType} are not supported.");
        }

        private static Task<List<T>> ListFromArray<T>(Task<T[]> readTask)
        {
            return readTask.ContinueWith(x => new List<T>(x.Result));
        }
    }
}
