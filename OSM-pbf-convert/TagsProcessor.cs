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
    public class TagsProcessor :
        IBlobProcessor<string>
    {
        private ulong dataSize;

        private ulong fileId;
        private readonly string fileName;

        private readonly Dictionary<string, KeyInfo> keys = new Dictionary<string, KeyInfo>();

        private ulong lastId;
        private readonly Dictionary<string, ValueInfo> values = new Dictionary<string, ValueInfo>();
        private ulong valuesCount;

        public TagsProcessor(string fileName)
        {
            this.fileName = fileName;
        }

        public string BlobRead(Blob blob)
        {
            return string.Empty;
        }

        public void ProcessPrimitives(PrimitiveAccessor accessor, string data)
        {
            foreach (var node in accessor.Nodes)
                if (node.Tags != null)
                    foreach (var tag in node.Tags)
                        AddTag(tag, RelationMemberTypes.Node, node.Id);

            foreach (var way in accessor.Ways)
                if (way.Tags != null)
                    foreach (var tag in way.Tags)
                        AddTag(tag, RelationMemberTypes.Way, way.Id);

            foreach (var relation in accessor.Relations)
                if (relation.Tags != null)
                    foreach (var tag in relation.Tags)
                        AddTag(tag, RelationMemberTypes.Relation, relation.Id);

            Console.Write(
                $"\r\nTag stats: {keys.Count:#,###}. Values: {values.Count:#,###}. Total: {valuesCount:#,###}. Size: {dataSize:#,###}.   ");
            Console.SetCursorPosition(0, 0);

            if (dataSize > 1024 * 1024 * 1024) // 1_073_741_824
                WriteTmpData();
        }

        public void Finish()
        {
            WriteTmpData();
            throw new NotImplementedException();
        }

        private void WriteTmpData()
        {
            fileId++;

            using (var strStream = File.Create(fileName + $".tmp-{fileId}.tag.str"))
            using (var idsStream = File.Create(fileName + $".tmp-{fileId}.tag.id"))
            using (var tagValueIdsStream = File.Create(fileName + $".tmp-{fileId}.tag-val.id"))
            using (var strWriter = new BinaryWriter(strStream, Encoding.UTF8, true))
            using (var idsWriter = new BinaryWriter(idsStream, Encoding.UTF8, true))
            using (var tagValueIdsWriter = new BinaryWriter(tagValueIdsStream, Encoding.UTF8, true))
            {
                var vals = values.Values.ToList();
                values.Clear();

                var cmp = Comparer<string>.Default;
                vals.Sort(Comparer<ValueInfo>.Create((a, b) => cmp.Compare(a.Value, b.Value)));

                var idMap = new Dictionary<ulong, ulong>();
                var i = 0ul;
                
                strWriter.Write7BitEncodedInt((ulong) vals.Count);

                foreach (var valueInfo in vals)
                {
                    idsWriter.Flush();
                    var offset = idsStream.Position;

                    idsWriter.Write7BitEncodedInt((ulong) valueInfo.Refs.Count);
                    foreach (var reference in valueInfo.Refs)
                    {
                        idsWriter.Write7BitEncodedInt(reference.RefId);
                    }

                    strWriter.Write(valueInfo.Value);
                    strWriter.Write7BitEncodedInt((ulong) offset);
                    strWriter.Write7BitEncodedInt(1UL); // Count of value ids.
                    strWriter.Write7BitEncodedInt(valueInfo.Id);

                    idMap.Add(valueInfo.Id, i);
                    i++;
                }

                var tags = keys.Values.ToList();
                keys.Clear();
                
                tags.Sort(Comparer<KeyInfo>.Create((a, b) => cmp.Compare(a.Key, b.Key)));
                
                strWriter.Write7BitEncodedInt((ulong) tags.Count);
                foreach (var keyInfo in tags)
                {
                    idsWriter.Flush();
                    var offset = idsStream.Position;

                    strWriter.Write(keyInfo.Key);
                    strWriter.Write7BitEncodedInt((ulong) offset);

                    var allIds = keyInfo.Values.SelectMany(x => x.Value).Distinct().OrderByDescending(x => x.RefId)
                        .ToList();

                    idsWriter.Write7BitEncodedInt((ulong) allIds.Count);
                    foreach (var reference in allIds)
                    {
                        idsWriter.Write7BitEncodedInt(reference.RefId);
                    }

                    var valIds = keyInfo.Values.OrderBy(x => idMap[x.Key]).ToList();
                    idsWriter.Write7BitEncodedInt((ulong) valIds.Count);
                    
                    foreach (var keyValuePair in valIds)
                    {
                        tagValueIdsWriter.Flush();
                        var offs = tagValueIdsStream.Position;

                        idsWriter.Write7BitEncodedInt(keyValuePair.Key);
                        idsWriter.Write7BitEncodedInt((ulong) offs);

                        foreach (var reference in keyValuePair.Value)
                        {
                            tagValueIdsWriter.Write7BitEncodedInt(reference.RefId);
                        }
                    }
                }
            }

            dataSize = 0;
        }

        private void AddTag(OsmTag tag, RelationMemberTypes type, long id)
        {
            var valueId = 0UL;

            var reference = new Reference(type, (ulong) id);

            if (!values.TryGetValue(tag.Value, out var valueInfo))
            {
                valueId = ++lastId;
                valueInfo.Id = valueId;
                valueInfo.Value = tag.Value;
                valueInfo.Refs = new List<Reference> {reference};

                values.Add(tag.Value, valueInfo);

                dataSize += (ulong) tag.Value.Length * 8 + 512;
            }
            else
            {
                valueId = valueInfo.Id;
                valueInfo.Refs.Add(reference);
            }

            dataSize += 24;

            if (!keys.TryGetValue(tag.Key, out var keyInfo))
            {
                keyInfo.Id = ++lastId;
                keyInfo.Key = tag.Key;
                keyInfo.Values = new Dictionary<ulong, List<Reference>> {{valueId, new List<Reference> {reference}}};
                keys.Add(tag.Key, keyInfo);
                dataSize += (ulong) tag.Key.Length * 8 + 1024;
            }
            else
            {
                if (keyInfo.Values.TryGetValue(valueId, out var valueRefs))
                {
                    valueRefs.Add(reference);
                }
                else
                {
                    keyInfo.Values.Add(valueId, new List<Reference> {reference});
                    dataSize += 128;
                }
            }

            dataSize += 24;
            valuesCount++;
        }

        private struct Reference
        {
            public Reference(ulong refId)
            {
                RefId = refId;
            }

            public Reference(RelationMemberTypes type, ulong id)
            {
                RefId = (id << 2) | (ulong) type;
            }

            public ulong RefId { get; set; }

            public RelationMemberTypes Type
            {
                get => (RelationMemberTypes) (RefId & 0b011);
                set => RefId &= 0xfffffffffffffffc | (ulong) value;
            }

            public ulong Id
            {
                get => RefId >> 2;
                set => RefId = (RefId & 0x03) | (value << 2);
            }
        }

        private struct KeyInfo
        {
            public ulong Id { get; set; }
            public string Key { get; set; }
            public Dictionary<ulong, List<Reference>> Values { get; set; }
        }

        private struct ValueInfo
        {
            public ulong Id { get; set; }
            public string Value { get; set; }
            public List<Reference> Refs { get; set; }
        }
    }
}