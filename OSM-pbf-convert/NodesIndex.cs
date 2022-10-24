using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Text;
using HuffmanCoding;
using ProtocolBuffers;

namespace OSM_pbf_convert
{
    public class NodesIndex : IDisposable
    {
        private struct NodesOffset
        {
            public long Id { get; set; }
            public byte Offset { get; set; }

            public override string ToString()
            {
                return $"{nameof(Id)}: {Id}, {nameof(Offset)}: {Offset}";
            }
        }

        private const long BlockSize = 4096;
        private readonly List<NodesOffset> index = new List<NodesOffset>(1_000_000);

        private long lastId;
        private int lastLat;
        private int lastLon;

        private long lastReset;
        private long lastIndexId;

        private bool canWrite;

        private readonly Stream indexStream;
        private readonly BinaryWriter indexWriter;

        private readonly MemoryMappedFile memFile;
        private readonly Stream stream;
        private readonly BinaryWriter writer;

        public NodesIndex(Configuration config)
        {
            var indexFileName = config.PbfFileName + ".idx";
            var nodesFileName = config.PbfFileName + ".nodes.dat";

            if (config.CanReadExistingFiles && File.Exists(indexFileName))
            {
                memFile = MemoryMappedFile.CreateFromFile(nodesFileName, FileMode.Open, "nodes.dat");

                stream = memFile.CreateViewStream();

//                stream = new FileStream(nodesFileName, 
//                    FileMode.Open, FileAccess.ReadWrite, FileShare.None, 
//                    (int)BlockSize, FileOptions.SequentialScan);

                writer = new BinaryWriter(stream, Encoding.UTF8, true);
                indexStream = File.Open(indexFileName, FileMode.Open);
                indexWriter = new BinaryWriter(indexStream, Encoding.UTF8, true);

                canWrite = false;

                LoadIndex();
            }
            else
            {
                stream = new FileStream(nodesFileName,
                    FileMode.Create, FileAccess.ReadWrite, FileShare.None,
                    (int)BlockSize, FileOptions.SequentialScan);

                canWrite = true;

                writer = new BinaryWriter(stream, Encoding.UTF8, true);
                indexStream = File.Open(indexFileName, FileMode.Create);
                indexWriter = new BinaryWriter(indexStream, Encoding.UTF8, true);
            }
        }


        private void LoadIndex()
        {
            index.Clear();
            lastIndexId = 0;
            using (var reader = new BinaryReader(indexStream, Encoding.UTF8, true))
            {
                while (indexStream.Position < indexStream.Length)
                {
                    var offset = reader.ReadByte();
                    if (offset == 255)
                    {
                        break;
                    }

                    lastIndexId += (long)reader.Read7BitEncodedUInt64();

                    index.Add(new NodesOffset
                    {
                        Id = lastIndexId,
                        Offset = offset
                    });
                }
            }
        }

        public void WriteNode(MapNode node)
        {
            if (!canWrite)
            {
                throw new InvalidOperationException("Index is in read only mode.");
            }
            if (stream.Position >= lastReset + BlockSize)
            {
                lastReset += BlockSize;
                var offset = (byte)(stream.Position - lastReset);

                index.Add(new NodesOffset
                {
                    Id = node.Id,
                    Offset = offset
                });

                indexWriter.Write(offset);

                var ciid = (ulong)(node.Id - lastIndexId);
                indexWriter.Write7BitEncodedInt(ciid);
                lastIndexId = node.Id;

                lastLat = 0;
                lastLon = 0;
                lastId = 0;
            }

            var cid = (ulong)(node.Id - lastId);
            writer.Write7BitEncodedInt(cid);
            lastId = node.Id;

            var cLat = node.Lat;
            writer.Write7BitEncodedInt(EncodeHelpers.EncodeZigZag(cLat - lastLat));
            lastLat = cLat;

            var cLon = node.Lon;
            writer.Write7BitEncodedInt(EncodeHelpers.EncodeZigZag(cLon - lastLon));
            lastLon = cLon;
        }

        public IEnumerable<MapNode> ReadAllNodesById(IEnumerable<long> ids)
        {
            // ToDo: rewrite this, with potentially using mem mapped files.
            var buf = new byte[BlockSize];
            var lastBlockOffset = -1L;

            var lId = 0L;
            var lLat = 0;
            var lLon = 0;
            var offset = 0;
            ulong v = 0;

            var searchWatch = new Stopwatch();
            var readerWatch = new Stopwatch();
            var reads = 0;
            var seek = 0;

            bool continuation = false;

            var bufLength = 0;

            foreach (var nodeId in ids)
            {
                searchWatch.Start();
                var blockIndex = FindBlockIndex(nodeId);
                searchWatch.Stop();

                var state = 0;

                do
                {
                    var blockOffset = (blockIndex + 1) * BlockSize;

                    if (lastBlockOffset != blockOffset)
                    {
                        readerWatch.Start();
                        reads++;
                        if (lastBlockOffset + BlockSize != blockOffset)
                        {
                            stream.Position = blockOffset;
                            seek++;
                        }

                        bufLength = stream.Read(buf, 0, buf.Length);
                        readerWatch.Stop();

                        if (bufLength == 0)
                        {
                            yield break;
                        }

                        lastBlockOffset = blockOffset;

                        if (!continuation)
                        {
                            offset = blockIndex >= 0 ? index[blockIndex].Offset : 0;
                            state = 0;
                            lId = 0;
                            lLat = 0;
                            lLon = 0;
                        }
                    }
                    else if (nodeId < lId)
                    {
                        offset = blockIndex >= 0 ? index[blockIndex].Offset : 0;
                        lId = 0;
                        lLat = 0;
                        lLon = 0;
                    }

                    while (state >= 0 && offset < bufLength)
                    {
                        var b = buf[offset];
                        offset++;
                        v = (v << 7) | (b & 0b0111_1111ul);

                        if ((b & 0b1000_0000) == 0)
                        {
                            switch (state)
                            {
                                case 0:
                                    lId += (long)v;
                                    state = 1;
                                    break;
                                case 1:
                                    lLat += (int)EncodeHelpers.DecodeZigZag(v);
                                    state = 2;
                                    break;
                                case 2:
                                    lLon += (int)EncodeHelpers.DecodeZigZag(v);
                                    state = 0;

                                    if (lId > nodeId)
                                    {
                                        state = -1;
                                    }
                                    if (lId == nodeId)
                                    {
                                        state = -1;
                                        yield return new MapNode
                                        {
                                            Id = lId,
                                            Lat = lLat,
                                            Lon = lLon
                                        };
                                    }

                                    if (continuation)
                                    {
                                        continuation = false;
                                        lId = 0;
                                        lLat = 0;
                                        lLon = 0;
                                    }
                                    break;
                            }

                            v = 0;
                        }

                    }

                    if (state >= 0)
                    {
                        blockIndex++;
                        offset = 0;
                        continuation = true;
                    }
                } while (state >= 0);
            }

            Console.WriteLine($"Search time: ${searchWatch.Elapsed}, Read time: ${readerWatch.Elapsed}. ${reads}/${seek}.");
        }

        private int FindBlockIndex(long nodeId)
        {
            var lowIndex = 0;
            var lowId = index[lowIndex].Id;
            if (nodeId < lowId)
            {
                return lowIndex - 1;
            }

            var highIndex = index.Count - 1;
            var highId = index[highIndex].Id;
            if (nodeId >= highId)
            {
                return highIndex;
            }

            while (highIndex - lowIndex > 1)
            {
                var i = (highIndex + lowIndex) >> 1;

                var cId = index[i].Id;

                if (nodeId == cId)
                {
                    return i;
                }

                if (nodeId > cId)
                {
                    lowIndex = i;
                }
                else if (nodeId < cId)
                {
                    highIndex = i;
                }
            }

            return lowIndex;
        }



        public void Flush()
        {
            writer.Flush();
            stream.Flush();
            indexWriter.Flush();
            indexStream.Flush();
        }

        public void Dispose()
        {
            writer?.Dispose();
            indexWriter?.Dispose();
            stream?.Dispose();
            indexStream?.Dispose();
        }
    }
}