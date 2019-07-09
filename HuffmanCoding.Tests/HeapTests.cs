using NUnit.Framework;

namespace HuffmanCoding.Tests
{
    [TestFixture]
    public class HeapTests
    {
        [Test]
        [TestCase(0, ExpectedResult = -1)]
        [TestCase(1, ExpectedResult = 0)]
        [TestCase(2, ExpectedResult = 0)]
        [TestCase(3, ExpectedResult = 1)]
        [TestCase(4, ExpectedResult = 1)]
        [TestCase(5, ExpectedResult = 2)]
        [TestCase(6, ExpectedResult = 2)]
        [TestCase(7, ExpectedResult = 3)]
        [TestCase(8, ExpectedResult = 3)]
        public int CalcParentIndexTest(int i)
        {
            return Heap<int>.Parent(i);
        }

        [Test]
        [TestCase(0, ExpectedResult = 1)]
        [TestCase(1, ExpectedResult = 3)]
        [TestCase(2, ExpectedResult = 5)]
        [TestCase(3, ExpectedResult = 7)]
        public int CalcLeftChildIndexTest(int i)
        {
            return Heap<int>.LeftChild(i);
        }

        [Test]
        [TestCase(0, ExpectedResult = 2)]
        [TestCase(1, ExpectedResult = 4)]
        [TestCase(2, ExpectedResult = 6)]
        [TestCase(3, ExpectedResult = 8)]
        public int CalcRightChildIndexTest(int i)
        {
            return Heap<int>.RightChild(i);
        }
    }
}