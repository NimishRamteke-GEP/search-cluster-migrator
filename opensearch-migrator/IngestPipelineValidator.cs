using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace opensearch_migrator
{
    // IngestPipelineValidator.cs
    using Newtonsoft.Json;
    using Newtonsoft.Json.Linq;

    public class IngestPipelineValidator
    {
        private readonly IElasticsearchClient _sourceClient;
        private readonly IElasticsearchClient _targetClient;
        private readonly ILogger _logger;
        private readonly string _sourceCluster;
        private readonly string _targetCluster;

        public IngestPipelineValidator(
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
            _logger.Log("Starting ingest pipeline validation process");

            try
            {
                // Fetch pipeline names with filter_path
                string sourcePipelinesJson = await _sourceClient.GetAsync($"{_sourceCluster}/_ingest/pipeline?filter_path=*.id");
                string targetPipelinesJson = await _targetClient.GetAsync($"{_targetCluster}/_ingest/pipeline?filter_path=*.id");

                // Parse pipeline names
                var sourcePipelines = ParsePipelineNames(sourcePipelinesJson);
                var targetPipelines = ParsePipelineNames(targetPipelinesJson);

                // Compare pipelines
                var missingInTarget = sourcePipelines.Except(targetPipelines).ToList();

                if (missingInTarget.Count == 0)
                {
                    _logger.Log("All ingest pipelines from source are present in target");
                }
                else
                {
                    _logger.Log($"Found {missingInTarget.Count} pipelines in source that are missing in target:");
                    foreach (var pipeline in missingInTarget)
                    {
                        _logger.Log($"- {pipeline}");
                    }
                }

                _logger.Log("Ingest pipeline validation completed");
            }
            catch (Exception ex)
            {
                _logger.Log($"Fatal error in pipeline validation: {ex.Message}");
                throw;
            }
        }

        private List<string> ParsePipelineNames(string json)
        {
            try
            {
                if (string.IsNullOrEmpty(json))
                {
                    return new List<string>();
                }

                var jObject = JObject.Parse(json);
                return jObject.Properties()
                    .Select(p => p.Name)
                    .Where(name => !string.IsNullOrEmpty(name))
                    .ToList();
            }
            catch (JsonException ex)
            {
                _logger.Log($"Error parsing pipeline names: {ex.Message}");
                return new List<string>();
            }
        }
    }
}
