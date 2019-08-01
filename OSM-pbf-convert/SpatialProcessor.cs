using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using HuffmanCoding;
using OsmReader;
using OsmReader.PbfDataObjects;
using ProtocolBuffers;

namespace OSM_pbf_convert
{
    public interface IMapObject
    {
        long Id { get; }
        RelationMemberTypes Type { get; }

        int MidLat { get; }
        int MidLon { get; }

        int Size { get; }

        BoundingRect Rect { get; }
    }

    public class SNode : IMapObject
    {
        public long Id { get; set; }
        public RelationMemberTypes Type => RelationMemberTypes.Node;
        public int MidLat => Lat;
        public int MidLon => Lon;
        public int Size => 1;
        public BoundingRect Rect => new BoundingRect().Extend(Lat, Lon);
        public int Lat { get; set; }
        public int Lon { get; set; }
    }

    public class SWay : IMapObject
    {
        public long Id { get; set; }
        public int WayType { get; set; }
        public IList<WayNode> Nodes { get; set; }
        RelationMemberTypes IMapObject.Type => RelationMemberTypes.Way;

        public int MidLat => (int) Nodes.Average(x => x.Lat);
        public int MidLon => (int) Nodes.Average(x => x.Lon);
        public int Size => 2 + Nodes.Count;

        public BoundingRect Rect => Nodes.Aggregate(new BoundingRect(), (rect, node) =>
        {
            rect.Extend(node.Lat, node.Lon);
            return rect;
        });
    }

    public struct WayNode
    {
        public WayNode(int lat, int lon)
        {
            Lat = lat;
            Lon = lon;
        }

        public int Lat { get; set; }
        public int Lon { get; set; }
    }

    public class SpatialBlock : IDisposable
    {
        public string FileName { get; }

        private readonly DeltaWriter nodeIdWriter;
        private readonly DeltaWriter nodeLatWriter;
        private readonly DeltaWriter nodeLonWriter;
        private readonly BinaryReader reader;

        private readonly Stream stream;

        private readonly DeltaWriter wayIdWriter;
        private readonly DeltaWriter wayLatWriter;
        private readonly DeltaWriter wayLonWriter;
        private readonly BinaryWriter writer;

        private int nodesCount;

        private readonly DeltaReader wayIdReader;
        private readonly DeltaReader wayLatReader;
        private readonly DeltaReader wayLonReader;

        private int wayNodesCount;
        private int waysCount;

        public SpatialBlock(string fileName)
        {
            FileName = fileName;
            var exists = File.Exists(fileName);
            Directory.CreateDirectory(Path.GetDirectoryName(fileName));
            stream = File.Open(fileName, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite);
            stream = new BufferedStream(stream, 65536);
            writer = new BinaryWriter(stream, Encoding.UTF8, true);
            reader = new BinaryReader(stream, Encoding.UTF8, true);

            nodeIdWriter = new DeltaWriter(writer);
            nodeLatWriter = new DeltaWriter(writer);
            nodeLonWriter = new DeltaWriter(writer);

            wayIdWriter = new DeltaWriter(writer);
            wayLatWriter = new DeltaWriter(writer);
            wayLonWriter = new DeltaWriter(writer);

            wayIdReader = new DeltaReader(reader);
            wayLatReader = new DeltaReader(reader);
            wayLonReader = new DeltaReader(reader);

            if (exists) ReadLastNodeData();
        }

        public BoundingRect BoundingRect { get; } = new BoundingRect();

        public int Size => nodesCount + waysCount + wayNodesCount;

        private void ReadLastNodeData()
        {
            waysCount =52758;
            var ways = ReadAllWays();
            foreach (var sWay in ways)
            {
                foreach (var node in sWay.Nodes)
                {
                    BoundingRect.Extend(node.Lat, node.Lon);
                }
            }

            throw new NotImplementedException();
        }

        public bool CanAdd(IMapObject item)
        {
            return BoundingRect.Contains(item.MidLat, item.MidLon);
        }

        public void Add(SNode node)
        {
            BoundingRect.Extend(node.Lat, node.Lon);
            WriteNode(node);
        }

        public void Add(SWay way)
        {
            foreach (var node in way.Nodes) BoundingRect.Extend(node.Lat, node.Lon);

            WriteWay(way);
        }

        public void Flush()
        {
            writer.Flush();
            stream.Flush();
        }

        private void WriteNode(SNode node)
        {
            if (waysCount > 0) throw new InvalidOperationException("Can't write node after ways.");
            nodesCount++;

            nodeIdWriter.WriteZigZag(node.Id);
            nodeLatWriter.WriteZigZag(node.Lat);
            nodeLonWriter.WriteZigZag(node.Lon);
        }

        private void WriteWay(SWay way)
        {
            if (waysCount == 0) writer.Write((byte) 0);

            waysCount++;

            wayIdWriter.WriteZigZag(way.Id);
            writer.Write7BitEncodedInt((ulong) way.WayType);
            writer.Write7BitEncodedInt((ulong) way.Nodes.Count);

            wayLatWriter.Reset();
            wayLonWriter.Reset();

            foreach (var node in way.Nodes)
            {
                wayNodesCount++;
                wayLatWriter.WriteZigZag(node.Lat);
                wayLonWriter.WriteZigZag(node.Lon);
            }
        }

        private SWay[] ReadAllWays()
        {
            var split = reader.ReadByte();
            if (split != 0) throw new InvalidOperationException("Not all ways were read.");

            wayIdReader.Reset();
            wayLatReader.Reset();
            wayLonReader.Reset();

            var allWays = new SWay[waysCount];

            for (var i = 0; i < allWays.Length; i++)
            {
                var id = wayIdReader.ReadZigZag();
                var type = (int) reader.Read7BitEncodedInt();
                var count = (int) reader.Read7BitEncodedInt();

                wayLatReader.Reset();
                wayLonReader.Reset();

                var nodes = new List<WayNode>(count);
                for (var j = 0; j < count; j++)
                {
                    var lat = (int) wayLatReader.ReadZigZag();
                    var lon = (int) wayLonReader.ReadZigZag();

                    nodes.Add(new WayNode(
                        lat,
                        lon
                    ));
                }

                var way = new SWay
                {
                    Id = id,
                    WayType = type,
                    Nodes = nodes
                };

                allWays[i] = way;
            }

            return allWays;
        }

        public Split Split(string otherFileName)
        {
            var latSize = BoundingRect.LatSize;
            var lonSize = BoundingRect.LonSize;

            var allNodes = ReadAllNodes();
            var allWays = ReadAllWays();
            var allItems = new List<IMapObject>(nodesCount + waysCount);
            allItems.AddRange(allNodes);
            allItems.AddRange(allWays);

            Comparison<IMapObject> comparison;
            var splitByLatitude = latSize >= lonSize;
            if (splitByLatitude)
                comparison = (a, b) => a.MidLat - b.MidLat;
            else
                comparison = (a, b) => a.MidLon - b.MidLon;

            var splitter = new QuickSortSplitter<IMapObject>(Comparer<IMapObject>.Create(comparison));
            var position = splitter.Split(allItems, allItems.Count / 100);

            var splitValue = splitByLatitude ? allItems[position].MidLat : allItems[position].MidLon;


            var task = Task.Run(() =>
            {
                Clear();
                AddAll(this, allItems, 0, position);
            });

            var other = new SpatialBlock(otherFileName);

            AddAll(other, allItems, position, allItems.Count);

            task.Wait();

            return new Split
            {
                SplitByLatitude = splitByLatitude,
                SplitValue = splitValue,
                FirstChild = new Split{ Block = this },
                SecondChild = new Split{ Block = other }
            };
        }

        private void Clear()
        {
            writer.Flush();
            stream.Flush();

            stream.Position = 0;
            stream.SetLength(0);

            BoundingRect.Reset();

            nodeIdWriter.Reset();
            nodeLatWriter.Reset();
            nodeLonWriter.Reset();

            wayIdWriter.Reset();
            wayLatWriter.Reset();
            wayLonWriter.Reset();

            wayIdReader.Reset();
            wayLatReader.Reset();
            wayLonReader.Reset();

            nodesCount = 0;
            waysCount = 0;
            wayNodesCount = 0;
        }

        private static void AddAll(SpatialBlock other, IReadOnlyList<IMapObject> items, int start, int end)
        {
            for (var i = start; i < end; i++)
            {
                if (items[i].Type != RelationMemberTypes.Node) continue;
                other.Add((SNode)items[i]);
            }

            for (var i = start; i < end; i++)
            {
                if (items[i].Type != RelationMemberTypes.Way) continue;
                other.Add((SWay)items[i]);
            }
        }

        private SNode[] ReadAllNodes()
        {
            var allNodes = new SNode[nodesCount];

            Flush();
            stream.Position = 0;

            var id = 0L;
            var lat = 0;
            var lon = 0;
            for (var i = 0; i < allNodes.Length; i++)
            {
                var inc = EncodeHelpers.DecodeZigZag(reader.Read7BitEncodedInt());
                if (inc == 0) throw new InvalidOperationException("Unexpected end of nodes.");
                id += inc;
                lat += (int) EncodeHelpers.DecodeZigZag(reader.Read7BitEncodedInt());
                lon += (int) EncodeHelpers.DecodeZigZag(reader.Read7BitEncodedInt());

                allNodes[i] = new SNode
                {
                    Id = id,
                    Lat = lat,
                    Lon = lon
                };
            }

            return allNodes;
        }

        public override string ToString()
        {
            return
                $"{FileName}: {Size} (${BoundingRect})";
        }

        public void Dispose()
        {
            Flush();
            reader?.Dispose();
            writer?.Dispose();
            stream?.Dispose();
        }
    }

    public class SpatialIndex : IDisposable
    {
        private const int BlockLimit = 100_000_000;
        private const int ReducedBlockLimit = 1_000_000;

        private readonly Split root = new Split
        {
            Block = new SpatialBlock(GetFileName(1))
        };

        private int lastBlock = 1;

        public void Add(SNode node)
        {
            var split = FindBlock(node);

            split.Block.Add(node);

            if (split.Block.Size >= BlockLimit) SplitBlock(split);
        }

        public void Add(SWay way)
        {
            var split = FindBlock(way);

            split.Block.Add(way);

            if (split.Block.Size >= BlockLimit) SplitBlock(split);
        }

        public void Finish()
        {
            SplitToReducedSize(root);
            WriteSplitInfo();
        }

        private void SplitToReducedSize(Split split)
        {
            if (split.Block == null)
            {
                SplitToReducedSize(split.FirstChild);
                SplitToReducedSize(split.SecondChild);
            }
            else
            {
                if (split.Block.Size > ReducedBlockLimit)
                {
                    SplitBlock(split);
                    SplitToReducedSize(split);
                }
            }
        }

        private void WriteSplitInfo()
        {
            using (var stream = File.Open("./Blocks/split-info.dat", FileMode.Create, FileAccess.Write))
            using (var writer = new BinaryWriter(stream, Encoding.UTF8, true))
            {
                WriteSplitInfo(root, writer);
            }

        }
        private void WriteSplitInfo(Split split, BinaryWriter writer)
        {
            if (split.Block == null)
            {
                writer.Write(split.SplitByLatitude ?(byte)1 : (byte)2);

                if (!split.SplitValue.HasValue)
                {
                    throw new InvalidOperationException("Split must have SplitValue when no Block is defined.");
                }

                writer.Write(split.SplitValue.Value);
                WriteSplitInfo(split.FirstChild, writer);
                WriteSplitInfo(split.SecondChild, writer);
            }
            else
            {
                split.Block.Flush();
                writer.Write((byte)0);
                writer.Write(split.Block.FileName);
            }
        }

        private void SplitBlock(Split split)
        {
            var block = split.Block;

            lastBlock++;
            var splitInfo = block.Split(GetFileName(lastBlock));

            split.SplitByLatitude = splitInfo.SplitByLatitude;
            split.SplitValue = splitInfo.SplitValue;
            split.FirstChild = splitInfo.FirstChild;
            split.SecondChild = splitInfo.SecondChild;
            split.Block = null;
        }

        private static string GetFileName(int block)
        {
            return $"./Blocks/sp-{block:0000}.map";
        }

        private Split FindBlock(IMapObject obj)
        {
            var current = root;
            while (current.Block == null)
                if (current.SplitByLatitude)
                    current = obj.MidLat < current.SplitValue ? current.FirstChild : current.SecondChild;
                else
                    current = obj.MidLon < current.SplitValue ? current.FirstChild : current.SecondChild;
            return current;
        }

        public void Dispose()
        {
            WriteSplitInfo();
            root?.Dispose();
        }
    }

    public class Split : IDisposable
    {
        public int? SplitValue { get; set; }
        public bool SplitByLatitude { get; set; }
        public SpatialBlock Block { get; set; }
        public Split FirstChild { get; set; }
        public Split SecondChild { get; set; }

        public override string ToString()
        {
            if (Block != null) return "Block: " + Block;

            return $"{(SplitByLatitude ? "Latitude" : "Longitude")}: {SplitValue}.";
        }

        public void Dispose()
        {
            Block?.Dispose();
            FirstChild?.Dispose();
            SecondChild?.Dispose();
        }
    }

    public class SpatialProcessor : IBlobProcessor<string>
    {
        private readonly SpatialIndex index = new SpatialIndex();
        private long totalCnt;


        public string BlobRead(Blob blob)
        {
            return string.Empty;
        }

        public void ProcessPrimitives(PrimitiveAccessor accessor, string data)
        {
            Console.Write($"Nodes: {totalCnt:#,###}.           ");
            foreach (var node in accessor.Nodes)
            {
                totalCnt++;
                var sNode = new SNode
                {
                    Id = node.Id,
                    Lat = Helpers.CoordAsInt(node.Lat),
                    Lon = Helpers.CoordAsInt(node.Lon)
                };
                index.Add(sNode);
            }
        }

        public void Finish()
        {
            throw new NotImplementedException();
        }
    }
}