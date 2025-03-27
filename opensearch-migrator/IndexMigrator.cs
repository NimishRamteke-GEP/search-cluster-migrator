// IndexMigrator.cs
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace opensearch_migrator
{
    public class IndexMigrator
    {
        private readonly IElasticsearchClient _sourceClient;
        private readonly IElasticsearchClient _targetClient;
        private readonly ILogger _logger;
        private readonly string _sourceCluster;
        private readonly string _targetCluster;
        private const int BatchSize = 20;
        private readonly MappingTypeConverter _mappingConverter;

        // Tracking for summary
        private int _totalIndices;
        private int _successfulMigrations;
        private int _skippedMigrations;
        private readonly Dictionary<string, string> _failedMigrations = new Dictionary<string, string>();

        public IndexMigrator(
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
            _mappingConverter = new MappingTypeConverter(logger);
        }

        public async Task MigrateAsync(string indexPattern)
        {
            _logger.Log($"Starting index migration process for pattern: {indexPattern}");

            try
            {
                // Reset tracking variables
                _totalIndices = 0;
                _successfulMigrations = 0;
                _skippedMigrations = 0;
                _failedMigrations.Clear();

                // Get matching index names from source
                var indexNames = await GetIndexNamesAsync(indexPattern);
                if (!indexNames.Any())
                {
                    _logger.Log($"No indices found matching pattern '{indexPattern}' in source cluster");
                    LogSummary();
                    return;
                }

                _totalIndices = indexNames.Count;
                _logger.Log($"Found {_totalIndices} indices matching pattern '{indexPattern}'");

                // Process indices in batches
                for (int i = 0; i < indexNames.Count; i += BatchSize)
                {
                    var batch = indexNames.Skip(i).Take(BatchSize).ToList();
                    await ProcessBatchAsync(batch);
                }

                _logger.Log("Index migration completed");
                LogSummary();
            }
            catch (Exception ex)
            {
                _logger.Log($"Fatal error in index migration: {ex.Message}");
                LogSummary();
                throw;
            }
        }

        private async Task<List<string>> GetIndexNamesAsync(string pattern)
        {
            try
            {
                string response = await _sourceClient.GetAsync($"{_sourceCluster}/_cat/indices/{pattern}?format=json");
                var indices = JsonConvert.DeserializeObject<List<Dictionary<string, string>>>(response);
                return indices?.Select(index => index["index"]).Where(name => !string.IsNullOrEmpty(name)).ToList() ?? new List<string>();
            }
            catch (Exception ex)
            {
                _logger.Log($"Error fetching index names for pattern '{pattern}': {ex.Message}");
                return new List<string>();
            }
        }

        private async Task ProcessBatchAsync(List<string> indexNames)
        {
            _logger.Log($"Processing batch of {indexNames.Count} indices: {string.Join(", ", indexNames)}");

            foreach (var indexName in indexNames)
            {
                await MigrateIndexAsync(indexName);
            }
        }

        private async Task MigrateIndexAsync(string indexName)
        {
            try
            {
                _logger.Log($"Checking index: {indexName}");

                // Check if index exists in target
                if (await IndexExistsInTarget(indexName))
                {
                    _logger.Log($"Index {indexName} already exists in target cluster, skipping migration");
                    _skippedMigrations++;
                    return;
                }

                _logger.Log($"Migrating index: {indexName}");

                // Get settings, mappings, and aliases from source
                string settingsAndMappings = await _sourceClient.GetAsync($"{_sourceCluster}/{indexName}/");
                if (string.IsNullOrEmpty(settingsAndMappings))
                {
                    _logger.Log($"Failed to retrieve settings and mappings for index: {indexName}");
                    _failedMigrations[indexName] = "Failed to retrieve settings and mappings";
                    return;
                }

                // Parse and clean the JSON
                var jObject = JObject.Parse(settingsAndMappings);
                var indexData = jObject[indexName] as JObject;

                if (indexData == null)
                {
                    _logger.Log($"No data found for index: {indexName}");
                    _failedMigrations[indexName] = "No data found in response";
                    return;
                }

                var aliases = indexData["aliases"] as JObject;

                // Clean settings (remove metadata)
                var settings = indexData["settings"]?["index"] as JObject;
                if (settings != null)
                {
                    settings.Remove("uuid");
                    settings.Remove("creation_date");
                    settings.Remove("provided_name");
                    settings.Remove("version");
                    settings["number_of_replicas"] = "0"; // Set default replica count
                    settings["refresh_interval"] = "300s"; // Set default refresh interval
                }

                // Transform mappings
                var mappings = indexData["mappings"] as JObject;
                if (mappings != null)
                {
                    _mappingConverter.TransformMappings(mappings);
                }

                // Prepare the cleaned payload
                var payload = new JObject
                {
                    ["aliases"] = aliases ?? new JObject(),
                    ["settings"] = settings != null ? new JObject { ["index"] = settings } : null,
                    ["mappings"] = mappings
                };

                // Migrate to target
                bool success = await _targetClient.PutAsync($"{_targetCluster}/{indexName}", payload.ToString(Formatting.None));
                if (success)
                {
                    _logger.Log($"Successfully migrated index: {indexName}");
                    _successfulMigrations++;
                }
                else
                {
                    _logger.Log($"Failed to migrate index: {indexName}");
                    _failedMigrations[indexName] = "PUT request failed";
                }
            }
            catch (Exception ex)
            {
                _logger.Log($"Error migrating index {indexName}: {ex.Message}");
                _failedMigrations[indexName] = ex.Message;
            }
        }

        private async Task<bool> IndexExistsInTarget(string indexName)
        {
            try
            {
                string response = await _targetClient.GetAsync($"{_targetCluster}/{indexName}/_settings");
                return !string.IsNullOrEmpty(response);
            }
            catch (HttpRequestException ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
            {
                return false;
            }
            catch (Exception ex)
            {
                _logger.Log($"Error checking existence of index {indexName} in target: {ex.Message}");
                return false;
            }
        }

        private void LogSummary()
        {
            var summary = new StringBuilder();
            summary.AppendLine("");

            summary.AppendLine("╔════════════════════════════════════════════════════╗");
            summary.AppendLine("║           Index Migration Summary                  ║");
            summary.AppendLine("╠════════════════════════════════════════════════════╣");
            summary.AppendLine($"║ Total Indices Found:  {_totalIndices,-10}                   ║");
            summary.AppendLine($"║ Successfully Migrated: {_successfulMigrations,-10}                  ║");
            summary.AppendLine($"║ Skipped (Already Exist):  {_skippedMigrations,-10}               ║");
            summary.AppendLine($"║ Failed:     {_failedMigrations.Count,-10}                             ║");

            if (_failedMigrations.Any())
            {
                summary.AppendLine("╠════════════════════════════════════════════════════╣");
                summary.AppendLine("║ Failed Indices:                                    ║1234567890ABCDEFGHIJK");
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