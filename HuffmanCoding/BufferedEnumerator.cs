using System;
using System.Collections;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace HuffmanCoding
{
    public class BufferedEnumerator<T> : IEnumerator<T>
    {
        private const int size = 1000;
        T[] buffer = new T[size];
        private int position = -1;
        private int length;

        private IEnumerator<T> source;

        public BufferedEnumerator(IEnumerable<T> source)
        {
            var enumerator = source.GetEnumerator();
            this.source = enumerator;
        }
        public BufferedEnumerator(IEnumerator<T> source)
        {
            this.source = source;
        }

        public void StartReader()
        {
            Task.Run(Read);
        }

        private void Read()
        {
            while (source.MoveNext())
            {
                buffer[length] = source.Current;
                length++;
                if (length >= size)
                {
                    length = 0;
                }

                while (length == position)
                {
                    Thread.SpinWait(10);                    
                }
            }

        }

        public bool MoveNext()
        {
            position++;
            if (position >= size)
            {
                position = 0;
            }
            if (position != length)
            {
                return true;
            }

            return position != length;
        }

        public void Reset()
        {
            throw new NotImplementedException();
        }

        public T Current { get; }

        object IEnumerator.Current => Current;

        public void Dispose()
        {
            throw new NotImplementedException();
        }
    }
}