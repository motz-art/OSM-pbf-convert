using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace OSM_pbf_convert
{
    public class PbfFileProcessor<T> : IDisposable
    {
        private readonly Stream stream;
        private readonly IBlobProcessor<T> processor;

        public PbfFileProcessor(string fileName, IBlobProcessor<T> processor)
        {
            this.processor = processor;
            stream = File.OpenRead(fileName);
        }

        public async Task Process()
        {
            var poolCount = 1; //Environment.ProcessorCount + 2;
            var pool = new Semaphore(poolCount, poolCount);

            try
            {
                var parser = new PbfBlobParser(stream);
                Blob blob;
                while ((blob = await PbfBlobParser.ReadBlobAsync(parser)) != null)
                {
                    if (blob.Header.Type != "OSMData")
                        continue;

                    var data = processor.BlobRead(blob);

                    var reader = PbfPrimitiveReader.Create(blob);
                    var accessor = new PrimitiveAccessor(reader);
                    pool.WaitOne();

                    Task.Run(async () =>
                    {
                        try
                        {
                            processor.ProcessPrimitives(accessor, data);
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine(e);
                        }
                        finally
                        {
                            pool.Release(1);
                        }
                    });
                }

                for (var i = 0; i < poolCount; i++) pool.WaitOne();

                processor.Finish();
            }
            catch (Exception e)
            {
                Console.WriteLine($"Error processing file. After reading {stream.Position} of {stream.Length} bytes.");
                Console.WriteLine(e);
            }
        }

        public void Dispose()
        {
            stream?.Dispose();

            if (processor != null && processor is IDisposable disposable)
            {
                disposable.Dispose();
            }
        }
    }

    public static class PbfFileProcessor
    {
        public static PbfFileProcessor<T> Create<T>(string fileName, IBlobProcessor<T> processor)
        {
            return new PbfFileProcessor<T>(fileName, processor);
        }
    }
}