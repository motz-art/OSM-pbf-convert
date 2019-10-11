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
            // var items = ReadAllItems(); // ????
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
            BoundingRect.Extend(way.MidLat, way.MidLon);

            WriteWay(way);
        }

        public void Add(SRel rel)
        {
            BoundingRect.Extend(rel.MidLat, rel.MidLon);

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

            WriteTags(rel.Tags);
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
            writer.Write7BitEncodedInt((ulong) way.Type);
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
            wayIdReader.Reset();
            wayNodeIdReader.Reset();
            wayLatReader.Reset();
            wayLonReader.Reset();

            var allWays = new List<SWay>(waysCount);

            while (stream.Position < stream.Length)
            {
                var lastStart = stream.Position;
                var id = wayIdReader.ReadZigZag();
                if (id == 0) break;
                var type = (int) reader.Read7BitEncodedInt();
                var nodes = ReadWayNodes();
                var tags = ReadTags();

                var way = new SWay
                {
                    Id = id,
                    Type = type,
                    Nodes = nodes,
                    Tags =  tags
                };

                allWays.Add(way);
            }

            return allWays.ToArray();
        }

        private IEnumerable<IMapObject> ReadAllRels()
        {
            relIdReader.Reset();
            relLatReader.Reset();
            relLonReader.Reset();

            var allRels = new List<SRel>(relsCount);

            while(stream.Position < stream.Length)
            {
                var id = relIdReader.ReadZigZag();
                if (id == 0) break;
                var type = (int) reader.Read7BitEncodedInt();
                var lat = relLatReader.ReadZigZag();
                var lon = relLonReader.ReadZigZag();
                var itemType = (int) reader.Read7BitEncodedInt();
                var itemId = (long) reader.Read7BitEncodedInt();
                var tags = ReadTags();

                allRels.Add(new SRel
                {
                    Id = id,
                    RelType = type,
                    MidLat = (int)lat,
                    MidLon = (int)lon,
                    ItemType = (RelationMemberTypes) itemType,
                    ItemId = itemId,
                    Tags = tags
                });
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

        public SpatialSplitInfo Split(NameGenerator generator, int maxItemsCount)
        {
            if (generator == null) throw new ArgumentNullException(nameof(generator));

            var _allItems = ReadAllItems();

            var info = new SpatialSplitInfoExtended
            {
                Items = _allItems,
                Start = 0,
                End = _allItems.Count - 1,
                BoundingRect = BoundingRect,
            };


            InMemorySplit(info, maxItemsCount);

            Clear();

            var spatialSplitInfo = SaveAllSpatialBlocks(this, info, generator).Result;

            return spatialSplitInfo;
        }

        private static void InMemorySplit(SpatialSplitInfoExtended info, int maxItemsCount)
        {
            var latSize = info.BoundingRect.LatSize;
            var lonSize = info.BoundingRect.LonSize;

            info.SplitByLatitude = latSize >= lonSize;

            Comparison<IMapObject> comparison;
            if (info.SplitByLatitude)
                comparison = (a, b) => a.MidLat - b.MidLat;
            else
                comparison = (a, b) => a.MidLon - b.MidLon;

            var splitter = new QuickSortSplitter<IMapObject>(Comparer<IMapObject>.Create(comparison));
            var position = splitter.Split(info.Items, info.Count / 100, info.Start, info.End);

            info.SplitValue = info.SplitByLatitude ? info.Items[position].MidLat : info.Items[position].MidLon;

            info.First = new SpatialSplitInfoExtended
            {
                Items = info.Items,
                Start = info.Start,
                End = position - 1
            };

            var first = info.First;
            SplitIfRequired(maxItemsCount, first);

            info.Second = new SpatialSplitInfoExtended
            {
                Items = info.Items,
                Start = position,
                End = info.End
            };
            SplitIfRequired(maxItemsCount, info.Second);
        }

        private static void SplitIfRequired(int maxItemsCount, SpatialSplitInfoExtended first)
        {
            if (first.GetSize() > maxItemsCount)
            {
                first.CalcBoundingRect();
                InMemorySplit(first, maxItemsCount);
            }
        }

        private static async Task<SpatialSplitInfo> SaveAllSpatialBlocks(SpatialBlock block, SpatialSplitInfoExtended info, NameGenerator generator)
        {
            if (info.First != null && info.Second != null)
            {
                var first = SaveAllSpatialBlocks(block, info.First, generator);
                var second = SaveAllSpatialBlocks(new SpatialBlock(generator.GetNextFileName()), info.Second,
                    generator);

                return new SpatialSplitInfo{
                    SplitByLatitude = info.SplitByLatitude,
                    SplitValue = info.SplitValue,
                    FirstChild = await first.ConfigureAwait(false),
                    SecondChild = await second.ConfigureAwait(false)
                };
            }

            await Task.Run(
                    () => AddAll(block, info.Items, info.Start, info.End))
                .ConfigureAwait(false);

            return new SpatialSplitInfo
            {
                Block = block
            };
        }

        private List<IMapObject> ReadAllItems()
        {
            var allNodes = ReadAllNodes();
            var allWays = ReadAllWays();
            var allRels = ReadAllRels();
            var allItems = new List<IMapObject>(nodesCount + waysCount + relsCount);
            allItems.AddRange(allNodes);
            allItems.AddRange(allWays);
            allItems.AddRange(allRels);
            return allItems;
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
            var allNodes = new List<SNode>(nodesCount);

            Flush();
            stream.Position = 0;

            var id = 0L;
            var lat = 0;
            var lon = 0;
            while (stream.Position < stream.Length)
            {
                var inc = EncodeHelpers.DecodeZigZag(reader.Read7BitEncodedInt());
                if (inc == 0) break;
                id += inc;
                lat += (int) EncodeHelpers.DecodeZigZag(reader.Read7BitEncodedInt());
                lon += (int) EncodeHelpers.DecodeZigZag(reader.Read7BitEncodedInt());

                var tags = ReadTags();

                allNodes.Add(new SNode
                {
                    Id = id,
                    Lat = lat,
                    Lon = lon,
                    Tags = tags
                });
            }

            return allNodes.ToArray();
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

    public class SpatialSplitInfoExtended
    {
        public List<IMapObject> Items { get; set; }
        public int Start { get; set; }
        public int End { get; set; }
        public int? SplitValue { get; set; }
        public SpatialSplitInfoExtended First { get; set; }
        public SpatialSplitInfoExtended Second { get; set; }
        public int Count => End - Start + 1;
        public bool SplitByLatitude { get; set; }
        public BoundingRect BoundingRect { get; set; }

        public int GetSize()
        {
            var result = 0;

            for (var i = Start; i <= End; i++)
            {
                var item = Items[i];
                result++;
                if (item.ObjectType == RelationMemberTypes.Way)
                {
                    var way = (SWay) item;
                    result += way.Nodes.Count;
                }
            }

            return result;
        }

        public void CalcBoundingRect()
        {
            BoundingRect = new BoundingRect();
            for (var i = Start; i <= End; i++)
            {
                var item = Items[i];

                BoundingRect.Extend(item.MidLat, item.MidLon);
            }
        }
    }
}