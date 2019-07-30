using System.IO;
using System.Text;
using FluentAssertions;
using NUnit.Framework;

namespace HuffmanCoding.Tests
{
    [TestFixture]
    public class DeltaWriterReaderTests
    {
        [Test]
        public void Test()
        {
            var stream = new MemoryStream();
            var breader = new BinaryReader(stream, Encoding.UTF8, true);
            var bwriter = new BinaryWriter(stream, Encoding.UTF8, true);

            var writer = new DeltaWriter(bwriter);
            var reader = new DeltaReader(breader);

            writer.WriteZigZag(10);
            writer.WriteZigZag(15);
            writer.WriteZigZag(7);
            writer.WriteZigZag(12);
            writer.WriteZigZag(1);

            bwriter.Flush();

            stream.Position = 0;

            reader.ReadZigZag().Should().Be(10);
            reader.ReadZigZag().Should().Be(15);
            reader.ReadZigZag().Should().Be(7);
            reader.ReadZigZag().Should().Be(12);
            reader.ReadZigZag().Should().Be(1);
        }
    }
}