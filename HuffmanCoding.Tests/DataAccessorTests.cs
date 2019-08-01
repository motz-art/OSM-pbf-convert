using System.IO;
using NUnit.Framework;

namespace HuffmanCoding.Tests
{
    [TestFixture]
    public class DataAccessorTests
    {
        class Dto
        {
            public int Id { get; set; }
        }

        [Test]
        public void Test1()
        {
            var accessor = new DataAccessor();
            accessor.Define<Dto>()
                .Map(x => x.Id, cfg => cfg.DeltaCoded());

            using (var stream = new MemoryStream())
            {
                using (var writer = accessor.CreateWriter<Dto>(stream))
                {
                    writer.Write(new Dto {Id = 1});
                }
            }
        }
    }
}