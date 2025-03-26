using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace opensearch_migrator
{
    // IndexTemplateValidator.cs
    using Newtonsoft.Json;
    using Newtonsoft.Json.Linq;

    public class IndexTemplateValidator
    {
        private readonly IElasticsearchClient _sourceClient;
        private readonly IElasticsearchClient _targetClient;
        private readonly ILogger _logger;
        private readonly string _sourceCluster;
        private readonly string _targetCluster;

        public IndexTemplateValidator(
            IElasticsearchClient sourceClient,
            IElasticsearchClient targetClient,
            ILogger logger,
            string sourceCluster,
            string targetCluster)
        {
            _sourceClient = sourceClient;
            _targetClient = targetClient;
            _logger = logger;
            _sourceCluster = sourceCluster;
            _targetCluster = targetCluster;
        }

        public async Task ValidateAsync()
        {
            _logger.Log("Starting index template validation process");

            try
            {
                // Fetch template names from source and target with filter_path
                string sourceTemplatesJson = await _sourceClient.GetAsync($"{_sourceCluster}/_index_template/default_*?filter_path=index_templates.name");
                string targetTemplatesJson = await _targetClient.GetAsync($"{_targetCluster}/_index_template/default_*?filter_path=index_templates.name");

                // Parse the JSON to extract template names
                var sourceTemplates = ParseTemplateNames(sourceTemplatesJson);
                var targetTemplates = ParseTemplateNames(targetTemplatesJson);

                // Compare templates
                var missingInTarget = sourceTemplates.Except(targetTemplates).ToList();

                if (missingInTarget.Count == 0)
                {
                    _logger.Log("All templates from source are present in target");
                }
                else
                {
                    _logger.Log($"Found {missingInTarget.Count} templates in source that are missing in target:");
                    foreach (var template in missingInTarget)
                    {
                        _logger.Log($"- {template}");
                    }
                }

                _logger.Log("Index template validation completed");
            }
            catch (Exception ex)
            {
                _logger.Log($"Fatal error in template validation: {ex.Message}");
                throw;
            }
        }

        private List<string> ParseTemplateNames(string json)
        {
            try
            {
                if (string.IsNullOrEmpty(json))
                {
                    return new List<string>();
                }

                var jObject = JObject.Parse(json);
                var templatesArray = jObject["index_templates"] as JArray;
                if (templatesArray == null)
                {
                    return new List<string>();
                }

                return templatesArray
                    .Select(t => t["name"]?.ToString())
                    .Where(name => !string.IsNullOrEmpty(name))
                    .ToList();
            }
            catch (JsonException ex)
            {
                _logger.Log($"Error parsing template names: {ex.Message}");
                return new List<string>();
            }
        }
    }
}
