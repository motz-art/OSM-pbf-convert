using System;
using System.IO;
using System.Text;

namespace OSM_pbf_convert
{
    public class SpatialIndex : IDisposable
    {
        private const int BlockLimit = 100_000_000;
        private const int ReducedBlockLimit = 100_000_000;

        private readonly SpatialSplitInfo root = new SpatialSplitInfo
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

        public void Add(SRel rel)
        {
            var split = FindBlock(rel);

            split.Block.Add(rel);

            if (split.Block.Size >= BlockLimit) SplitBlock(split);
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
                    SplitBlock(spatialSplitInfo);
                    SplitToReducedSize(spatialSplitInfo);
                }
            }
        }

        private void WriteSplitInfo()
        {
            using (var stream = File.Open("./Blocks/spatial-split-info.dat", FileMode.Create, FileAccess.Write))
            using (var writer = new BinaryWriter(stream, Encoding.UTF8, true))
            {
                WriteSplitInfo(root, writer);
            }

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

        private void SplitBlock(SpatialSplitInfo spatialSplitInfo)
        {
            var block = spatialSplitInfo.Block;

            lastBlock++;
            var splitInfo = block.Split(GetFileName(lastBlock));

            spatialSplitInfo.SplitByLatitude = splitInfo.SplitByLatitude;
            spatialSplitInfo.SplitValue = splitInfo.SplitValue;
            spatialSplitInfo.FirstChild = splitInfo.FirstChild;
            spatialSplitInfo.SecondChild = splitInfo.SecondChild;
            spatialSplitInfo.Block = null;
        }

        private static string GetFileName(int block)
        {
            return $"./Blocks/sp-{block:0000}.map";
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