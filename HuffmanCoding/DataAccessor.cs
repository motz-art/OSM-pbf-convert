using System;
using System.Collections;
using System.IO;
using System.Linq.Expressions;
using System.Text;

namespace HuffmanCoding
{
    public class DataAccessor
    {
        public DataAccessorMapBuilder<T> Define<T>()
        {
            return new DataAccessorMapBuilder<T>();
        }

        public IDataWriter<T> CreateWriter<T>(Stream stream)
        {
            return new DataWriter<T>(new BinaryWriter(stream, Encoding.UTF8, true));
        }
    }

    public class DataWriter<T> : IDataWriter<T>
    {
        private readonly BinaryWriter binaryWriter;
        private IPropertyWriter<T>[] writers;

        public DataWriter(BinaryWriter binaryWriter)
        {
            this.binaryWriter = binaryWriter;
        }

        public void Write(T obj)
        {
            if (obj == null)
            {
                throw new ArgumentNullException(nameof(obj));
            }

            foreach (var writer in writers)
            {
                writer.Write(obj, binaryWriter);
            }
        }

        public void Dispose()
        {
            binaryWriter?.Dispose();
        }
    }

    internal interface IPropertyWriter<TObj>
    {
        void Write(TObj obj, BinaryWriter binaryWriter);
    }

    public interface IDataWriter<T> : IDisposable
    {
        void Write(T obj);
    }

    public class DataAccessorMapBuilder<T>
    {
        public void Map(Expression<Func<T, int>> propertyExpression, Action<NumericPropertyConfig> cfg)
        {
            var config = new NumericPropertyConfig();
            cfg(config);
        }
    }

    public class NumericPropertyConfig
    {
        public NumericPropertyConfig DeltaCoded()
        {
            return this;
        }
    }

    public class PropertyFormatConfiguration<T>
    {
    }
}