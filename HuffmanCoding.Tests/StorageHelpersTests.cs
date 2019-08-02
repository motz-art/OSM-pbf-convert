using System.IO;
using System.Text;
using FluentAssertions;
using NUnit.Framework;

namespace HuffmanCoding.Tests
{
    [TestFixture]
    public class StorageHelpersTests
    {
        [Test]
        [TestCase(0ul, 1)]
        [TestCase(1ul, 1)]
        [TestCase(127ul, 1)]
        [TestCase(128ul, 2)]
        [TestCase(16*1024ul, 2)]
        [TestCase(ulong.MaxValue, 10)]
        public void WriteReadTest(ulong value, int length)
        {
            using (var stream = new MemoryStream())
            {
                var writer = new BinaryWriter(stream, Encoding.UTF8, false);

                StorageHelpers.Write7BitEncodedInt(writer, value);

                stream.Position = 0;
                var reader = new BinaryReader(stream, Encoding.UTF8, false);

                StorageHelpers.Read7BitEncodedInt(reader).Should().Be(value);
            }
        }
    }
}
