using System.IO;
using FluentAssertions;
using NUnit.Framework;

namespace ProtocolBuffers.Tests
{
    [TestFixture]
    public class ProtobufReaderTests
    {
        [Test]
        public void TestInt32Read()
        {
            var bytes = new byte[] { 0x08, 0x96, 0x01 };
            using (var stream = new MemoryStream(bytes))
            {
                var reader = new ProtobufReader(stream, stream.Length);
                reader.BeginReadMessage(stream.Length);
                reader.FieldNumber.Should().Be(1);
                reader.FieldType.Should().Be(FieldTypes.VarInt);
                var value = reader.ReadInt32();
                value.Should().Be(150);
            }
        }

        [Test]
        public void TestStringRead()
        {
            var bytes = new byte[] {0x12, 0x07, 0x74, 0x65, 0x73, 0x74, 0x69, 0x6e, 0x67};
            using (var stream = new MemoryStream(bytes))
            {
                var reader = new ProtobufReader(stream, stream.Length);
                reader.BeginReadMessage(stream.Length);
                reader.FieldNumber.Should().Be(2);
                reader.FieldType.Should().Be(FieldTypes.LengthDelimited);
                var value = reader.ReadString();
                value.Should().Be("testing");
            }
        }
    }
}
