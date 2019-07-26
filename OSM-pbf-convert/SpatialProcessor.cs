using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using HuffmanCoding;
using ProtocolBuffers;

namespace OSM_pbf_convert
{
    public class DeltaReader
    {
        private readonly BinaryReader reader;
        private long lastSigned;

        private ulong lastUnsigned;

        public DeltaReader(BinaryReader reader)
        {
            this.reader = reader ?? throw new ArgumentNullException(nameof(reader));
        }

        public void Reset()
        {
            lastSigned = 0;
            lastUnsigned = 0;
        }

        public long ReadZigZag()
        {
            lastSigned += EncodeHelpers.DecodeZigZag(reader.Read7BitEncodedInt());
            return lastSigned;
        }

        public ulong ReadIncrementOnly()
        {
            lastUnsigned += reader.Read7BitEncodedInt();
            return lastUnsigned;
        }
    }

    public class DeltaWriter
    {
        private readonly BinaryWriter writer;

        private ulong last;
        private long lastSigned;

        public DeltaWriter(BinaryWriter writer)
        {
            this.writer = writer ?? throw new ArgumentNullException(nameof(writer));
        }

        public void Reset()
        {
            last = 0;
            lastSigned = 0;
        }

        public void WriteZigZag(long value)
        {
            var diff = value - lastSigned;
            writer.Write7BitEncodedInt(EncodeHelpers.EncodeZigZag(diff));
            lastSigned = value;
        }

        public void WriteIncrementOnly(ulong value)
        {
            if (value < last) throw new ArgumentException("value should not decrement!");

            var diff = value - last;
            writer.Write7BitEncodedInt(diff);
            last = value;
        }
    }

    public interface IMapObject
    {
        ulong Id { get; }
        RelationMemberTypes Type { get; }

        int MidLat { get; }
        int MidLon { get; }

        int Size { get; }

        BoundingRect Rect { get; }
    }

    public class SNode : IMapObject
    {
        public ulong Id { get; set; }
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
        public ulong Id { get; set; }
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

    public class SpatialBlock
    {
        private readonly string fileName;

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
            this.fileName = fileName;
            var exists = File.Exists(fileName);
            Directory.CreateDirectory(Path.GetDirectoryName(fileName));
            stream = File.Open(fileName, FileMode.OpenOrCreate, FileAccess.ReadWrite);
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

        public BoundingRect BoundingRect { get; set; }

        public int Size => nodesCount + waysCount + wayNodesCount;

        private void ReadLastNodeData()
        {
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

            nodeIdWriter.WriteIncrementOnly(node.Id);
            nodeLatWriter.WriteZigZag(node.Lat);
            nodeLonWriter.WriteZigZag(node.Lon);
        }

        private void WriteWay(SWay way)
        {
            if (waysCount == 0) writer.Write((byte) 0);

            waysCount++;

            wayIdWriter.WriteIncrementOnly(way.Id);
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
            stream.Position = 0;
            stream.SetLength(0);

            waysCount = 0;
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

            var id = 0UL;
            var lat = 0;
            var lon = 0;
            for (var i = 0; i < allNodes.Length; i++)
            {
                var inc = reader.Read7BitEncodedInt();
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

        private SWay[] ReadAllWays()
        {
            var split = reader.ReadByte();
            if (split != 0) throw new InvalidOperationException("Not all ways were read.");

            var allWays = new SWay[waysCount];

            for (var i = 0; i < allWays.Length; i++)
            {
                var id = wayIdReader.ReadIncrementOnly();
                var type = (int) reader.Read7BitEncodedInt();
                var count = (int) reader.Read7BitEncodedInt();

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

        public override string ToString()
        {
            return
                $"{fileName}: {Size} (${BoundingRect})";
        }
    }

    public class SpatialIndex
    {
        private const int BlockLimit = 10_000_000;

        private readonly Split root = new Split
        {
            Block = new SpatialBlock("./Blocks/sp-0001.map")
        };

        private int lastBlock = 1;

        public void Add(SNode node)
        {
            var split = FindBlock(node);

            split.Block.Add(node);

            if (split.Block.Size >= BlockLimit) SplitBlock(split);
        }

        private void SplitBlock(Split split)
        {
            var block = split.Block;

            lastBlock++;
            var splitInfo = block.Split($"./Blocks/sp-{lastBlock:000}.map");

            split.SplitByLatitude = splitInfo.SplitByLatitude;
            split.SplitValue = splitInfo.SplitValue;
            split.FirstChild = splitInfo.FirstChild;
            split.SecondChild = splitInfo.SecondChild;
            split.Block = null;
        }

        private Split FindBlock(SNode node)
        {
            var current = root;
            while (current.Block == null)
                if (current.SplitByLatitude)
                    current = node.Lat < current.SplitValue ? current.FirstChild : current.SecondChild;
                else
                    current = node.Lon < current.SplitValue ? current.FirstChild : current.SecondChild;
            return current;
        }
    }

    public class Split
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
                    Id = (ulong) node.Id,
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