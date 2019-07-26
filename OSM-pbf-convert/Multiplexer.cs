using System;
using System.Collections.Concurrent;
using System.Threading;

namespace OSM_pbf_convert
{
    public class Multiplexer<T> : IBlobProcessor<Multiplexer<T>.BlobInfo>
    {
        public class BlobInfo
        {
            public int Id { get; set; }
            public Blob Blob { get; set; }
            public PrimitiveAccessor Accessor { get; set; }
            public BlobInfo Next { get; set; }
            public AutoResetEvent Event { get; } = new AutoResetEvent(false);
        }

        private IBlobProcessor<T> subProcessor;
        private int blobCnt;
        private Semaphore pool;
        private ConcurrentQueue<BlobInfo> queue = new ConcurrentQueue<BlobInfo>();

        public Multiplexer(IBlobProcessor<T> subProcessor)
        {
            this.subProcessor = subProcessor;
            pool = new Semaphore(10,10);
        }

        public BlobInfo BlobRead(Blob blob)
        {
            var info = new BlobInfo
            {
                Id = Interlocked.Increment(ref blobCnt),
                Blob = blob,
            };

            if (info.Id == 1)
            {
                var t = new Thread(ProcessBlobs)
                {
                    Name = "BlobProcessing"
                };
                t.Start();
            }
            
            pool.WaitOne();

            queue.Enqueue(info);

            return info;
        }

        public void ProcessPrimitives(PrimitiveAccessor accessor, BlobInfo data)
        {
            var d = accessor.Data;
            data.Accessor = accessor;
        }

        private void ProcessBlobs()
        {
            BlobInfo item;
            while (!queue.TryDequeue(out item))
            {
                Thread.Sleep(1);
            }

            var data = subProcessor.BlobRead(item.Blob);
            item.Event.WaitOne();
            subProcessor.ProcessPrimitives(item.Accessor, data);
            pool.Release(1);
        }

        public void Finish()
        {
            throw new NotImplementedException();
        }
    }
}