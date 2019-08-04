using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using HuffmanCoding;
using OsmReader;
using ProtocolBuffers;

namespace OSM_pbf_convert
{
    public class SpatialBlock : IDisposable
    {
        public string FileName { get; }

        private readonly DeltaWriter nodeIdWriter;
        private readonly DeltaWriter nodeLatWriter;
        private readonly DeltaWriter nodeLonWriter;
        private readonly BinaryReader reader;

        private readonly Stream stream;

        private readonly DeltaWriter wayIdWriter;
        private readonly DeltaWriter wayNodeIdWriter;
        private readonly DeltaWriter wayLatWriter;
        private readonly DeltaWriter wayLonWriter;
        private readonly BinaryWriter writer;

        private int nodesCount;

        private readonly DeltaReader wayIdReader;
        private readonly DeltaReader wayNodeIdReader;
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
            wayNodeIdWriter = new DeltaWriter(writer);
            wayLatWriter = new DeltaWriter(writer);
            wayLonWriter = new DeltaWriter(writer);

            wayIdReader = new DeltaReader(reader);
            wayNodeIdReader = new DeltaReader(reader);
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

            wayNodeIdWriter.Reset();
            wayLatWriter.Reset();
            wayLonWriter.Reset();

            foreach (var node in way.Nodes)
            {
                wayNodesCount++;
                wayNodeIdWriter.WriteZigZag((long)node.Id);
                wayLatWriter.WriteZigZag(node.Lat);
                wayLonWriter.WriteZigZag(node.Lon);
            }
        }

        private SWay[] ReadAllWays()
        {
            var split = reader.ReadByte();
            if (split != 0) throw new InvalidOperationException("Not all ways were read.");

            wayIdReader.Reset();
            wayNodeIdReader.Reset();
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
                    var nodeId = (ulong) wayNodeIdReader.ReadZigZag();
                    var lat = (int) wayLatReader.ReadZigZag();
                    var lon = (int) wayLonReader.ReadZigZag();

                    nodes.Add(new WayNode(
                        nodeId,
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

        public SpatialSplitInfo Split(string otherFileName)
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

            return new SpatialSplitInfo
            {
                SplitByLatitude = splitByLatitude,
                SplitValue = splitValue,
                FirstChild = new SpatialSplitInfo{ Block = this },
                SecondChild = new SpatialSplitInfo{ Block = other }
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
            wayNodeIdWriter.Reset();
            wayLatWriter.Reset();
            wayLonWriter.Reset();

            wayIdReader.Reset();
            wayNodeIdReader.Reset();
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
}