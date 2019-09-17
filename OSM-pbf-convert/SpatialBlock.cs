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

        private readonly DeltaWriter relIdWriter;
        private readonly DeltaWriter relLatWriter;
        private readonly DeltaWriter relLonWriter;
        private readonly DeltaReader relIdReader;
        private readonly DeltaReader relLatReader;
        private readonly DeltaReader relLonReader;

        private int relsCount;

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

            relIdWriter = new DeltaWriter(writer);
            relLatWriter = new DeltaWriter(writer);
            relLonWriter = new DeltaWriter(writer);

            relIdReader = new DeltaReader(reader);
            relLatReader = new DeltaReader(reader);
            relLonReader = new DeltaReader(reader);

            if (exists) ReadLastNodeData();
        }

        public BoundingRect BoundingRect { get; } = new BoundingRect();

        public int Size => nodesCount + waysCount + wayNodesCount + relsCount;

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

        public void Add(SRel rel)
        {
            WriteRel(rel);
        }

        private void WriteRel(SRel rel)
        {
            if (relsCount == 0)
            {
                writer.Write((byte) 0);
            }

            relsCount++;

            relIdWriter.WriteZigZag(rel.Id);
            writer.Write7BitEncodedInt((int)rel.ObjectType);
            relLatWriter.WriteZigZag(rel.MidLat);
            relLatWriter.WriteZigZag(rel.MidLon);
            writer.Write7BitEncodedInt((int)rel.ItemType);
            writer.Write7BitEncodedInt(rel.ItemId);
        }

        public void Flush()
        {
            writer.Flush();
            stream.Flush();
        }

        private void WriteNode(SNode node)
        {
            if (waysCount > 0) throw new InvalidOperationException("Can't write node after ways.");
            if (relsCount > 0) throw new InvalidOperationException("Can't write node after relations.");
            nodesCount++;

            nodeIdWriter.WriteZigZag(node.Id);
            nodeLatWriter.WriteZigZag(node.Lat);
            nodeLonWriter.WriteZigZag(node.Lon);

            WriteTags(node.Tags);
        }

        private void WriteTags(IList<STagInfo> tags)
        {
            writer.Write7BitEncodedInt(tags?.Count ?? 0);
            if (tags != null)
            {
                foreach (var tagInfo in tags)
                {
                    if (tagInfo.TagId.HasValue)
                    {
                        writer.Write((byte) 1);
                        writer.Write7BitEncodedInt(tagInfo.TagId.Value);
                    }
                    else if (tagInfo.KeyId.HasValue && tagInfo.Value != null)
                    {
                        writer.Write((byte) 2);
                        writer.Write7BitEncodedInt(tagInfo.KeyId.Value);
                        writer.Write(tagInfo.Value);
                    }
                    else if (!string.IsNullOrWhiteSpace(tagInfo.Key) && tagInfo.Value != null)
                    {
                        writer.Write((byte) 3);
                        writer.Write(tagInfo.Key);
                        writer.Write(tagInfo.Value);
                    }
                    else
                    {
                        throw new InvalidOperationException("Invalid value of STagInfo.");
                    }
                }
            }
        }

        private void WriteWay(SWay way)
        {
            if (relsCount > 0) throw new InvalidOperationException("Can't write way after relations.");

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

            WriteTags(way.Tags);
        }

        private SWay[] ReadAllWays()
        {
            var split = reader.ReadByte();
            if (split != 0) throw new InvalidOperationException("Not all nodes were read.");

            wayIdReader.Reset();
            wayNodeIdReader.Reset();
            wayLatReader.Reset();
            wayLonReader.Reset();

            var allWays = new SWay[waysCount];

            for (var i = 0; i < allWays.Length; i++)
            {
                var id = wayIdReader.ReadZigZag();
                var type = (int) reader.Read7BitEncodedInt();
                var nodes = ReadWayNodes();

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

        private IEnumerable<IMapObject> ReadAllRels()
        {
            var split = reader.ReadByte();
            if (split != 0) throw new InvalidOperationException("Not all ways were read.");

            relIdReader.Reset();
            relLatReader.Reset();
            relLonReader.Reset();

            var allRels = new SRel[relsCount];

            for (int i = 0; i < allRels.Length; i++)
            {
                var id = relIdReader.ReadZigZag();
                var type = (int) reader.Read7BitEncodedInt();
                var lat = relLatReader.ReadZigZag();
                var lon = relLonReader.ReadZigZag();
                var itemType = (int) reader.Read7BitEncodedInt();
                var itemId = (long) reader.Read7BitEncodedInt();

                allRels[i] = new SRel
                {
                    Id = id,
                    RelType = type,
                    MidLat = (int)lat,
                    MidLon = (int)lon,
                    ItemType = (RelationMemberTypes) itemType,
                    ItemId = itemId
                };
            }

            return allRels;
        }

        private List<WayNode> ReadWayNodes()
        {
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

            return nodes;
        }

        public SpatialSplitInfo Split(string otherFileName)
        {
            var latSize = BoundingRect.LatSize;
            var lonSize = BoundingRect.LonSize;

            var allNodes = ReadAllNodes();
            var allWays = ReadAllWays();
            var allRels = ReadAllRels();
            var allItems = new List<IMapObject>(nodesCount + waysCount + relsCount);
            allItems.AddRange(allNodes);
            allItems.AddRange(allWays);
            allItems.AddRange(allRels);

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

            relIdWriter.Reset();
            relLatWriter.Reset();
            relLonWriter.Reset();

            relIdReader.Reset();
            relLatReader.Reset();
            relLonReader.Reset();

            nodesCount = 0;
            waysCount = 0;
            wayNodesCount = 0;
            relsCount = 0;
        }

        private static void AddAll(SpatialBlock block, IReadOnlyList<IMapObject> items, int start, int end)
        {
            for (var i = start; i < end; i++)
            {
                if (items[i].ObjectType != RelationMemberTypes.Node) continue;
                block.Add((SNode)items[i]);
            }

            for (var i = start; i < end; i++)
            {
                if (items[i].ObjectType != RelationMemberTypes.Way) continue;
                block.Add((SWay)items[i]);
            }

            for (var i = start; i < end; i++)
            {
                if (items[i].ObjectType != RelationMemberTypes.Relation) continue;
                block.Add((SRel)items[i]);
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

                var tags = ReadTags();

                allNodes[i] = new SNode
                {
                    Id = id,
                    Lat = lat,
                    Lon = lon,
                    Tags = tags
                };
            }

            return allNodes;
        }

        private List<STagInfo> ReadTags()
        {
            var count = (int)reader.Read7BitEncodedInt();
            var tags = new List<STagInfo>(count);

            for (var i = 0; i < count; i++)
            {
                var type = reader.ReadByte();
                var tag = new STagInfo();
                switch (type)
                {
                    case 1:
                        tag.TagId = (int)reader.Read7BitEncodedInt();
                        break;
                    case 2:
                        tag.KeyId = (int) reader.Read7BitEncodedInt();
                        tag.Value = reader.ReadString();
                        break;
                    case 3:
                        tag.Key = reader.ReadString();
                        tag.Value = reader.ReadString();
                        break;
                    default:
                        throw new NotSupportedException($"Tag encoding type '{type}' at position {reader.BaseStream.Position - 1} is not supported.");
                }
            }

            return tags;
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