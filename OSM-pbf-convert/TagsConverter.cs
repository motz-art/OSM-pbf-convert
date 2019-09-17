using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Newtonsoft.Json;
using OsmReader;

namespace OSM_pbf_convert
{
    public class TagsConverter
    {
        public class TagsConvertSettings
        {
            public IDictionary<string, int> KeyCodes { get; set; }
            public IReadOnlyList<STagInfo> TagCodes { get; set; }
            public IReadOnlyList<string> KeyStopList { get; set; }
        }

        public IDictionary<string, int> keyCodes;
        public IDictionary<OsmTag, int> tagCodes;
        public ISet<string> keyStopList;

        public void LoadSettings(string fileName)
        {
            var text = File.ReadAllText(fileName);

            var settings = JsonConvert.DeserializeObject<TagsConvertSettings>(text);

            keyCodes = settings.KeyCodes;
            tagCodes = settings.TagCodes.ToDictionary(x => new OsmTag(x.Key, x.Value), x => x.TagId.Value);
            keyStopList = new HashSet<string>(settings.KeyStopList);
        }

        public List<STagInfo> ConvertTags(IReadOnlyList<OsmTag> tags)
        {
            if (keyStopList == null) throw new InvalidOperationException("Settings not loaded.");
            if (tags == null) throw new ArgumentNullException(nameof(tags));

            var result = new List<STagInfo>();
            foreach (var tag in tags)
            {
                if (keyStopList.Contains(tag.Key)) continue;
                if (tagCodes.TryGetValue(tag, out var id))
                {
                    result.Add(new STagInfo { TagId = id });
                }
                else if (keyCodes.TryGetValue(tag.Key, out id))
                {
                    result.Add(new STagInfo { KeyId = id, Value = tag.Value });
                }
            }
            return result;
        }
    }
}