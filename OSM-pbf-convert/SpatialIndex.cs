using System;
using System.IO;
using System.Text;

namespace OSM_pbf_convert
{
    public class NameGenerator
    {
        public int Counter { get; set; }

        public string GetNextFileName()
        {
            Counter++;
            return $"./Blocks/sp-{Counter:0000}.map";
        }
    }

    public class SpatialIndex : IDisposable
    {
        private const int BlockLimit = 20_000_000;
        private const int ReducedBlockLimit = 10_000;
        private const string SplitInfoFileName = "./Blocks/spatial-split-info.dat";

        private readonly NameGenerator generatror = new NameGenerator();

        private readonly SpatialSplitInfo root;

        public SpatialIndex()
        {
            root = new SpatialSplitInfo
            {
                Block = new SpatialBlock(generatror.GetNextFileName())
            };
            root = ReadSplitInfo();
        }

        public void Add(SNode node)
        {
            var split = FindBlock(node);

            split.Block.Add(node);

            if (split.Block.Size >= BlockLimit) SplitBlock(split, BlockLimit / 16);
        }

        public void Add(SWay way)
        {
            var split = FindBlock(way);

            split.Block.Add(way);

            if (split.Block.Size >= BlockLimit) SplitBlock(split, BlockLimit / 16);
        }

        public void Add(SRel rel)
        {
            var split = FindBlock(rel);

            split.Block.Add(rel);

            if (split.Block.Size >= BlockLimit) SplitBlock(split, BlockLimit / 16);
        }

        public void Finish()
        {
            SplitToReducedSize(root);
            WriteSplitInfo();
        }

        private void SplitToReducedSize(SpatialSplitInfo spatialSplitInfo)
        {
            if (spatialSplitInfo.Block == null)
            {
                SplitToReducedSize(spatialSplitInfo.FirstChild);
                SplitToReducedSize(spatialSplitInfo.SecondChild);
            }
            else
            {
                if (spatialSplitInfo.Block.Size > ReducedBlockLimit)
                {
                    SplitBlock(spatialSplitInfo, ReducedBlockLimit);
                    SplitToReducedSize(spatialSplitInfo);
                }
                else
                {
                    spatialSplitInfo.Block.Flush();
                }
            }
        }

        private SpatialSplitInfo ReadSplitInfo()
        {
            if (!File.Exists(SplitInfoFileName)) return root;
            using (var stream = File.Open(SplitInfoFileName, FileMode.Open, FileAccess.Read))
            using (var reader = new BinaryReader(stream, Encoding.UTF8, true))
            {
                return ReadSplitInfo(reader);
            }
        }

        private void WriteSplitInfo()
        {
            using (var stream = File.Open(SplitInfoFileName, FileMode.Create, FileAccess.Write))
            using (var writer = new BinaryWriter(stream, Encoding.UTF8, true))
            {
                WriteSplitInfo(root, writer);
            }

        }

        private SpatialSplitInfo ReadSplitInfo(BinaryReader reader)
        {
            var type = reader.ReadByte();
            var result = new SpatialSplitInfo();
            if (type == 1 || type == 2)
            {
                result.SplitByLatitude = (type == 1);
                result.SplitValue = reader.ReadInt32();
                result.FirstChild = ReadSplitInfo(reader);
                result.SecondChild = ReadSplitInfo(reader);
            }
            else if (type == 0)
            {
                var fileName = reader.ReadString();
                result.Block = new SpatialBlock(fileName);
            }
            else
            {
                throw new InvalidOperationException("Can't read split info. Unknown type.");
            }

            return result;
        }
        private void WriteSplitInfo(SpatialSplitInfo spatialSplitInfo, BinaryWriter writer)
        {
            if (spatialSplitInfo.Block == null)
            {
                writer.Write(spatialSplitInfo.SplitByLatitude ?(byte)1 : (byte)2);

                if (!spatialSplitInfo.SplitValue.HasValue)
                {
                    throw new InvalidOperationException("SpatialSplitInfo must have SplitValue when no Block is defined.");
                }

                writer.Write(spatialSplitInfo.SplitValue.Value);
                WriteSplitInfo(spatialSplitInfo.FirstChild, writer);
                WriteSplitInfo(spatialSplitInfo.SecondChild, writer);
            }
            else
            {
                spatialSplitInfo.Block.Flush();
                writer.Write((byte)0);
                writer.Write(spatialSplitInfo.Block.FileName);
            }
        }

        private void SplitBlock(SpatialSplitInfo spatialSplitInfo, int size)
        {
            var block = spatialSplitInfo.Block;

            var splitInfo = block.Split(generatror, size);

            spatialSplitInfo.SplitByLatitude = splitInfo.SplitByLatitude;
            spatialSplitInfo.SplitValue = splitInfo.SplitValue;
            spatialSplitInfo.FirstChild = splitInfo.FirstChild;
            spatialSplitInfo.SecondChild = splitInfo.SecondChild;
            spatialSplitInfo.Block = null;
        }


        private SpatialSplitInfo FindBlock(IMapObject obj)
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
}