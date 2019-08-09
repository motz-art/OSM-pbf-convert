using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using HuffmanCoding;
using OsmReader;
using OsmReader.PbfDataObjects;

namespace OSM_pbf_convert
{
    public struct RelationLocation : IEquatable<RelationLocation>
    {
        public bool Equals(RelationLocation other)
        {
            return Id == other.Id && Offset == other.Offset;
        }

        public override bool Equals(object obj)
        {
            return obj is RelationLocation other && Equals(other);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                return (Id.GetHashCode() * 397) ^ Offset.GetHashCode();
            }
        }

        public static bool operator ==(RelationLocation left, RelationLocation right)
        {
            return left.Equals(right);
        }

        public static bool operator !=(RelationLocation left, RelationLocation right)
        {
            return !left.Equals(right);
        }

        public long Id { get; set; }
        public long Offset { get; set; }
    }

    public class RelationsFile : IDisposable
    {
        private readonly DeltaWriter idWriter;
        private readonly DeltaWriter memberIdWriter;
        private readonly DeltaWriter memberMidLatWriter;
        private readonly DeltaWriter memberMidLonWriter;
        private readonly BinaryReader reader;
        private readonly List<RelationLocation> startPositions = new List<RelationLocation>();
        private readonly Stream stream;

        private readonly Dictionary<string, int> stringIds = new Dictionary<string, int>();
        private readonly BinaryWriter writer;
        private int lastStringId;

        public RelationsFile(string fileName)
        {
            stream = File.Open(fileName, FileMode.OpenOrCreate, FileAccess.ReadWrite);
            stream = new BufferedStream(stream, 65536);
            reader = new BinaryReader(stream, Encoding.UTF8, true);
            writer = new BinaryWriter(stream, Encoding.UTF8, true);

            idWriter = new DeltaWriter(writer);
            memberIdWriter = new DeltaWriter(writer);
            memberMidLatWriter = new DeltaWriter(writer);
            memberMidLonWriter = new DeltaWriter(writer);
        }

        public void Dispose()
        {
            writer?.Dispose();
            reader?.Dispose();
            stream?.Dispose();
        }

        public void WriteStrings(string fileName)
        {
            using (var file = File.Create(fileName))
            using(var writer = new BinaryWriter(file, Encoding.UTF8, true))
            {
                var strings = stringIds.OrderBy(x => x.Value);
                foreach (var pair in strings)
                {
                    writer.Write7BitEncodedInt(pair.Value);
                    writer.Write(pair.Key);
                }
            }
        }

        public void Add(OsmRelation relation, int typeId, IReadOnlyList<RelationItemInfo> itemInfos, BoundingRect rect)
        {
            if (relation == null) throw new ArgumentNullException(nameof(relation));
            if (itemInfos == null) throw new ArgumentNullException(nameof(itemInfos));
            if (rect == null) throw new ArgumentNullException(nameof(rect));

            startPositions.Add(new RelationLocation
            {
                Id = relation.Id,
                Offset = stream.Position
            });

            idWriter.WriteIncrementOnly((ulong) relation.Id);
            writer.Write7BitEncodedInt((ulong) typeId);

            rect.WriteTo(writer);

            writer.Write7BitEncodedInt((ulong) relation.Items.Count);

            memberIdWriter.Reset();
            memberMidLatWriter.Reset();
            memberMidLonWriter.Reset();

            for (var index = 0; index < relation.Items.Count; index++)
            {
                var member = relation.Items[index];
                writer.Write((byte) member.MemberType);
                memberIdWriter.WriteZigZag(member.Id);
                writer.Write7BitEncodedInt(GetRoleId(member.Role));

                var info = itemInfos[index];
                memberMidLatWriter.WriteZigZag(info.MidLat);
                memberMidLonWriter.WriteZigZag(info.MidLon);
            }
        }

        private ulong GetRoleId(string memberRole)
        {
            if (!stringIds.TryGetValue(memberRole, out var id))
            {
                id = lastStringId++;
                stringIds.Add(memberRole, id);
            }

            return (ulong) id;
        }

        public void Flush()
        {
            writer.Flush();
            stream.Flush();
        }
    }

    public class RelationsProcessor : IBlobProcessor<string>, IDisposable
    {
        private readonly NodesIndex nodesIndex;
        private readonly RelationsFile relationsFile;
        private readonly WaysDataFile waysData;
        private string fileName;

        public RelationsProcessor(string fileName)
        {
            this.fileName = fileName;
            waysData = new WaysDataFile(fileName + ".ways.dat");
            nodesIndex = new NodesIndex(fileName, true);
            relationsFile = new RelationsFile(fileName + ".rels");
        }

        public string BlobRead(Blob blob)
        {
            return string.Empty;
        }

        public void ProcessPrimitives(PrimitiveAccessor accessor, string data)
        {
            if (accessor == null) throw new ArgumentNullException(nameof(accessor));

            var nodeIds = accessor.Relations
                .SelectMany(x => x.Items)
                .Where(i => i.MemberType == RelationMemberTypes.Node)
                .Select(i => i.Id)
                .Distinct()
                .OrderBy(x => x);

            var nodes = nodesIndex.ReadAllNodesById(nodeIds).ToDictionary(x => x.Id);

            foreach (var relation in accessor.Relations)
            {
                var rect = new BoundingRect();
                var itemInfos = new List<RelationItemInfo>();

                foreach (var relationItem in relation.Items)
                    switch (relationItem.MemberType)
                    {
                        case RelationMemberTypes.Node:
                            if (!nodes.TryGetValue(relationItem.Id, out var node))
                            {
                                itemInfos.Add(new RelationItemInfo
                                {
                                    Id = (ulong) relationItem.Id,
                                    Type = relationItem.MemberType,
                                    MidLat = int.MinValue,
                                    MidLon = int.MinValue
                                });
                            }
                            else
                            {
                                rect.Extend(node.Lat, node.Lon);
                                itemInfos.Add(new RelationItemInfo
                                {
                                    Id = (ulong) relationItem.Id,
                                    Type = relationItem.MemberType,
                                    MidLat = node.Lat,
                                    MidLon = node.Lon
                                });
                            }

                            break;
                        case RelationMemberTypes.Way:
                            var way = waysData.FindWayInfo((ulong) relationItem.Id);
                            if (way == null)
                            {
                                itemInfos.Add(new RelationItemInfo
                                {
                                    Id = (ulong)relationItem.Id,
                                    Type = relationItem.MemberType,
                                    MidLat = int.MinValue,
                                    MidLon = int.MinValue
                                });
                            }
                            else
                            {
                                rect.Extend(way.Rect);
                                itemInfos.Add(new RelationItemInfo
                                {
                                    Id = (ulong) relationItem.Id,
                                    Type = relationItem.MemberType,
                                    MidLat = int.MinValue,
                                    MidLon = int.MinValue
                                });
                            }
                            break;
                        case RelationMemberTypes.Relation:
                            itemInfos.Add(new RelationItemInfo
                            {
                                Id = (ulong) relationItem.Id,
                                Type = relationItem.MemberType,
                                MidLat = 0,
                                MidLon = 0
                            });
                            // Relations will be assembled recursively.
                            break;
                        default:
                            throw new NotSupportedException("Unknown relation type.");
                    }

                relationsFile.Add(relation, 0, itemInfos, rect); // ToDo: Implement type detection
            }
        }

        public void Finish()
        {
            relationsFile.Flush();
            relationsFile.WriteStrings(fileName + ".rels.strs");
        }

        public void Dispose()
        {
            relationsFile?.Dispose();
        }
    }

    public class RelationItemInfo
    {
        public ulong Id { get; set; }
        public RelationMemberTypes Type { get; set; }
        public int MidLat { get; set; }
        public int MidLon { get; set; }
    }
}