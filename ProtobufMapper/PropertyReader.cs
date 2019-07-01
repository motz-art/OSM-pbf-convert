using System;
using System.Threading.Tasks;
using ProtocolBuffers;

namespace ProtobufMapper
{
    public class PropertyReader<T>
    {
        public string PropertyName { get; set; }
        public Func<ProtobufReader, T, Task> Read { get;set; }
    }
}