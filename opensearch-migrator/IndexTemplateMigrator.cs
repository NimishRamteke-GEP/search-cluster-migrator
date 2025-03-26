// IndexTemplateMigrator.cs
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace opensearch_migrator
{
    public class IndexTemplateMigrator
    {
        private readonly IElasticsearchClient _sourceClient;
        private readonly IElasticsearchClient _targetClient;
        private readonly ILogger _logger;
        private readonly string _sourceCluster;
        private readonly string _targetCluster;

        // Dictionary for mapping type conversions (e.g., for OpenSearch compatibility)
        private readonly Dictionary<string, string> _mappingTypeConversions = new Dictionary<string, string>
        {
            { "flattened", "flat_object" } // Add more conversions as needed
        };

        // Tracking for summary
        private int _totalTemplates;
        private int _successfulMigrations;
        private int _skippedMigrations;
        private readonly Dictionary<string, string> _failedMigrations = new Dictionary<string, string>();

        public IndexTemplateMigrator(
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

        public async Task MigrateAsync(string pattern)
        {
            _logger.Log("Starting index template migration process");

            try
            {
                // Reset tracking variables
                _totalTemplates = 0;
                _successfulMigrations = 0;
                _skippedMigrations = 0;
                _failedMigrations.Clear();

                string templateList = await _sourceClient.GetAsync($"{_sourceCluster}/_index_template/{pattern}");
                var templates = JsonConvert.DeserializeObject<dynamic>(templateList);

                if (templates?.index_templates == null)
                {
                    _logger.Log("No index templates found in source cluster");
                    LogSummary();
                    return;
                }

                _totalTemplates = ((IEnumerable<dynamic>)templates.index_templates).Count();
                _logger.Log($"Found {_totalTemplates} index templates matching pattern 'default_*'");

                foreach (var template in templates.index_templates)
                {
                    await MigrateTemplateAsync(template.name.ToString(), template.index_template.ToString());
                }

                _logger.Log("Index template migration completed");
                LogSummary();
            }
            catch (Exception ex)
            {
                _logger.Log($"Fatal error in template migration: {ex.Message}");
                LogSummary();
                throw;
            }
        }

        private async Task MigrateTemplateAsync(string templateName, string templateDetails)
        {
            try
            {
                _logger.Log($"Checking template: {templateName}");

                // Check if template exists in target
                if (await TemplateExistsInTarget(templateName))
                {
                    _logger.Log($"Template {templateName} already exists in target cluster, skipping migration");
                    _skippedMigrations++;
                    return;
                }

                _logger.Log($"Processing template: {templateName}");

                // Transform mappings in the template details
                var jObject = JObject.Parse(templateDetails);
                var mappings = jObject["template"]["mappings"] as JObject;
                if (mappings != null)
                {
                    TransformMappings(mappings);
                    templateDetails = jObject.ToString(Formatting.None);
                }

                bool success = await _targetClient.PutAsync($"{_targetCluster}/_index_template/{templateName}", templateDetails);
                if (success)
                {
                    _logger.Log($"Successfully migrated template: {templateName}");
                    _successfulMigrations++;
                }
                else
                {
                    _logger.Log($"Failed to migrate template: {templateName}");
                    _failedMigrations[templateName] = "PUT request failed";
                }
            }
            catch (Exception ex)
            {
                _logger.Log($"Error processing template {templateName}: {ex.Message}");
                _failedMigrations[templateName] = ex.Message;
            }
        }

        private async Task<bool> TemplateExistsInTarget(string templateName)
        {
            try
            {
                string response = await _targetClient.GetAsync($"{_targetCluster}/_index_template/{templateName}");
                return !string.IsNullOrEmpty(response);
            }
            catch (HttpRequestException ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
            {
                return false;
            }
            catch (Exception ex)
            {
                _logger.Log($"Error checking existence of template {templateName} in target: {ex.Message}");
                return false;
            }
        }

        private void TransformMappings(JObject mappings)
        {
            var typeTokens = mappings.SelectTokens("..type").ToList();
            foreach (var token in typeTokens)
            {
                if (token.Type == JTokenType.String)
                {
                    string currentType = token.Value<string>();
                    if (_mappingTypeConversions.TryGetValue(currentType, out string newType))
                    {
                        _logger.Log($"Converting mapping type '{currentType}' to '{newType}' in template");
                        token.Replace(newType);
                    }
                }
            }
        }

        private void LogSummary()
        {
            var summary = new StringBuilder();
            summary.AppendLine("╔════════════════════════════════════════════════════╗");
            summary.AppendLine("║        Index Template Migration Summary            ║");
            summary.AppendLine("╠════════════════════════════════════════════════════╣");
            summary.AppendLine($"║ Total Templates Found:        {_totalTemplates,-10}         ║");
            summary.AppendLine($"║ Successfully Migrated:        {_successfulMigrations,-10}         ║");
            summary.AppendLine($"║ Skipped (Already Exist):      {_skippedMigrations,-10}         ║");
            summary.AppendLine($"║ Failed:                      {_failedMigrations.Count,-10}         ║");

            if (_failedMigrations.Any())
            {
                summary.AppendLine("╠════════════════════════════════════════════════════╣");
                summary.AppendLine("║ Failed Templates:                                  ║");
                foreach (var failure in _failedMigrations)
                {
                    summary.AppendLine($"║ - {failure.Key,-20} : {failure.Value,-20} ║");
                }
            }

            summary.AppendLine("╚════════════════════════════════════════════════════╝");
            _logger.Log(summary.ToString());
        }
    }
}