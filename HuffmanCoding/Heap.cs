using System;
using System.Collections.Generic;

namespace HuffmanCoding
{
    public class Heap<T>
    {
        private List<T> items = new List<T>();
        private IComparer<T> comparer;

        public Heap() : this((IComparer<T>) Comparer<T>.Default)
        {

        }

        public Heap(IComparer<T> comparer)
        {
            this.comparer = comparer;
        }

        public void Heapify()
        {
            throw new NotImplementedException("");
        }

        public int LiftUp(int i)
        {
            while (i > 0)
            {
                var p = Parent(i);
                if (comparer.Compare(items[i], items[p]) >= 0) return i;
                Swap(i, p);
                i = p;
            }

            return 0;
        }

        public int PushDown(int i)
        {
            var l = LeftChild(i);
            while (l < items.Count)
            {
                var r = l+1;
                if (comparer.Compare(items[l], items[r]) < 0)
                {
                    if (comparer.Compare(items[l], items[i]) >= 0)
                    {
                        return i;
                    }
                    Swap(i, l);
                    i = l;
                }
                else
                {
                    if (comparer.Compare(items[r], items[i]) >= 0)
                    {
                        return i;
                    }
                    Swap(i, r);
                    i = r;
                }
                l = LeftChild(i);
            }
            return i;
        }

        private bool HasChild(int i)
        {
            return LeftChild(i) < items.Count;
        }

        private void Swap(int i1, int i2)
        {
            var tmp = items[i1];
            items[i1] = items[i2];
            items[i2] = tmp;
        }

        public static int Parent(int i)
        {
            return (i - 1) >> 1;
        }

        public static int LeftChild(int i)
        {
            return (i << 1) + 1;
        }

        public static int RightChild(int i)
        {
            return (i << 1) + 2;
        }
    }
}