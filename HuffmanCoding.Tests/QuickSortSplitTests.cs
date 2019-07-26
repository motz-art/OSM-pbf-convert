using System;
using System.Linq;
using FluentAssertions;
using NUnit.Framework;

namespace HuffmanCoding.Tests
{
    [TestFixture]
    public class QuickSortSplitTests
    {
        [Test]
        public void TestSplitRandomArray()
        {
            var rnd = new Random(125);

            var arr = new int[1000];
            for (var i = 0; i < arr.Length; i++)
            {
                arr[i] = rnd.Next();
            }

            var splitter = new QuickSortSplitter<int>();

            var result = splitter.Split(arr);
            
            result.Should().Be(500);

            Console.WriteLine($"{splitter.runCount}/{splitter.totalLength}.");

            var max = arr.Take(500).Max();
            var min = arr.Skip(500).Min();

            max.Should().BeLessOrEqualTo(min);
        }
        [Test]
        public void TestSplitSubsequences()
        {
            var rnd = new Random(125);

            var arr = new int[1000];
            for (var i = 0; i < arr.Length; i++)
            {
                arr[i] = i % 300;
            }

            var splitter = new QuickSortSplitter<int>();

            var result = splitter.Split(arr);
            
            result.Should().Be(500);
            Console.WriteLine($"{splitter.runCount}/{splitter.totalLength}.");

            var max = arr.Take(500).Max();
            var min = arr.Skip(500).Min();

            max.Should().BeLessOrEqualTo(min);
        }
    }
}