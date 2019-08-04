using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using HuffmanCoding;
using OsmReader;
using OsmReader.PbfDataObjects;

namespace OSM_pbf_convert
{
    public struct RelationLocation
    {
        public long Id { get; set; }
        public long Offset { get; set; }
    }

    public class RelationsFile : IDisposable
    {
        private Stream stream;
        private BinaryWriter writer;
        private BinaryReader reader;

        private DeltaWriter idWriter;
        private DeltaWriter memberIdWriter;

        Dictionary<string, int> stringIds = new Dictionary<string, int>();
        private int lastStringId = 0;
        List<RelationLocation> startPositions = new List<RelationLocation>();

        public RelationsFile(string fileName)
        {
            stream = File.Open(fileName, FileMode.OpenOrCreate, FileAccess.ReadWrite);
            stream = new BufferedStream(stream, 65536);
            reader = new BinaryReader(stream, Encoding.UTF8, true);
            writer = new BinaryWriter(stream, Encoding.UTF8, true);

            idWriter = new DeltaWriter(writer);
            memberIdWriter = new DeltaWriter(writer);
        }

        public void Add(OsmRelation relation, int typeId)
        {
            startPositions.Add(new RelationLocation
            {
                Id = relation.Id,
                Offset = stream.Position
            });

            idWriter.WriteIncrementOnly((ulong) relation.Id);
            writer.Write7BitEncodedInt((ulong) typeId);
            writer.Write7BitEncodedInt((ulong) relation.Items.Count);

            memberIdWriter.Reset();

            foreach (var member in relation.Items)
            {
                writer.Write((byte) member.MemberType);
                memberIdWriter.WriteZigZag(member.Id);
                writer.Write7BitEncodedInt(GetRoleId(member.Role));
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

        public void Dispose()
        {
            writer?.Dispose();
            reader?.Dispose();
            stream?.Dispose();
        }
    }

    public class RelationsProcessor : IBlobProcessor<string>, IDisposable
    {
        private RelationsFile relationsFile;
        
        public RelationsProcessor(string fileName)
        {
            relationsFile = new RelationsFile(fileName + ".rels");
        }

        public string BlobRead(Blob blob)
        {
            return string.Empty;
        }

        public void ProcessPrimitives(PrimitiveAccessor accessor, string data)
        {
            foreach (var relation in accessor.Relations)
            {
                relationsFile.Add(relation, 0); // ToDo: Implement type detection
            }
        }

        public void Finish()
        {
            relationsFile.Flush();
        }

        public void Dispose()
        {
            relationsFile?.Dispose();
        }
    }
}