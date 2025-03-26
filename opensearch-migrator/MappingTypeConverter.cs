using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace opensearch_migrator
{
    public class MappingTypeConverter
    {
        private readonly ILogger _logger;
        private readonly Dictionary<string, string> _mappingTypeConversions = new Dictionary<string, string>
        {
            { "flattened", "flat_object" } // Add more conversions as needed
        };

        public MappingTypeConverter(ILogger logger)
        {
            _logger = logger;
        }

        public void TransformMappings(JObject mappings)
        {
            if (mappings == null) return;

            var typeTokens = mappings.SelectTokens("..type").ToList();
            foreach (var token in typeTokens)
            {
                if (token.Type == JTokenType.String)
                {
                    string currentType = token.Value<string>();
                    if (_mappingTypeConversions.TryGetValue(currentType, out string newType))
                    {
                        _logger.Log($"Converting mapping type '{currentType}' to '{newType}'");
                        token.Replace(newType);
                    }
                }
            }
        }

        // Optional: Expose the conversions dictionary for external use or modification
        public IReadOnlyDictionary<string, string> GetMappingTypeConversions()
        {
            return _mappingTypeConversions;
        }
    }
}