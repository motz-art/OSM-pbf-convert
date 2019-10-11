using System;
using System.Collections.Generic;

namespace HuffmanCoding
{
    public class QuickSortSplitter<T>
    {
        private IComparer<T> comparer;
        public long runCount = 0;
        public long totalLength = 0;

        public QuickSortSplitter() : this(Comparer<T>.Default)
        {

        }

        public QuickSortSplitter(IComparer<T> comparer)
        {
            this.comparer = comparer;
        }

        public int Split(IList<T> list, int tolerance = 0, int? firstItem = null, int? lastItem = null)
        {
            if (list == null)
            {
                throw new ArgumentNullException(nameof(list));
            }

            var begin = firstItem ?? 0;
            var end = lastItem ?? list.Count - 1;
            var mid = (end + begin + 1) / 2;
            var res = begin;

            while (end - begin > 1)
            {
                var median = FindMedian(list, begin, end, comparer);

                res = RunIteration(list, begin, end, median);

                if (Math.Abs(res - mid) <= tolerance)
                {
                    return res;
                }

                if (res < mid)
                {
                    if (begin == res)
                    {
                        begin = res + 1;
                    }
                    else
                    {
                        begin = res;
                    }
                }
                else
                {
                    if (end == res)
                    {
                        end = res - 1;
                    }
                    else
                    {
                        end = res;
                    }
                }
            }

            return res;
        }

        private int RunIteration(IList<T> list, int begin, int end, T median)
        {
            runCount++;
            totalLength += end - begin;

            var l = begin;
            var h = end;

            while (l < h)
            {
                while (l < h && comparer.Compare(list[l], median) < 0)
                {
                    l++;
                }

                var lv = list[l];

                while (l < h && comparer.Compare(list[h], lv) >= 0)
                {
                    h--;
                }

                if (l >= h)
                {
                    return h;
                }

                Swap(list, l, h);
            }

            return h;
        }

        private void Swap(IList<T> list, int l, int h)
        {
            var tmp = list[l];
            list[l] = list[h];
            list[h] = tmp;
        }

        private T FindMedian(IList<T> list, int begin, int end, IComparer<T> comparer)
        {
            var i = (begin + end) / 2;
            return list[i];
        }
    }
}