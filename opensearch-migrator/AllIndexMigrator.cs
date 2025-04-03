using Newtonsoft.Json.Linq;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace opensearch_migrator
{
    public class AllIndexMigrator
    {
        private readonly IElasticsearchClient _sourceClient;
        private readonly IElasticsearchClient _targetClient;
        private readonly ILogger _logger;
        private readonly string _sourceCluster;
        private readonly string _targetCluster;
        private readonly MappingTypeConverter _mappingConverter;
        private const int BatchSize = 100;

        // Tracking for summary
        private int _totalSourceIndices;
        private int _totalDestinationIndices;
        private int _successfulMigrations;
        private int _skippedMigrations;
        private readonly Dictionary<string, string> _failedMigrations = new Dictionary<string, string>();

        public AllIndexMigrator(
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

        public async Task MigrateAsync()
        {
            _logger.Log("Starting all index migration process");

            try
            {
                // Reset tracking variables
                _totalSourceIndices = 0;
                _totalDestinationIndices = 0;
                _successfulMigrations = 0;
                _skippedMigrations = 0;
                _failedMigrations.Clear();

                // Fetch all indices from source and destination
                var sourceIndices = await GetAllIndicesAsync(_sourceCluster, "source");
                var destinationIndices = await GetAllIndicesAsync(_targetCluster, "destination");

                sourceIndices.RemoveAll(index => index.Contains("filebeat"));
                sourceIndices.RemoveAll(index => index.Contains("."));
                sourceIndices.RemoveAll(index => index.Contains("metric"));

                destinationIndices.RemoveAll(index => index.Contains("filebeat"));
                destinationIndices.RemoveAll(index => index.Contains("."));
                destinationIndices.RemoveAll(index => index.Contains("metric"));

                _totalSourceIndices = sourceIndices.Count;
                _totalDestinationIndices = destinationIndices.Count;
                _logger.Log($"Found {_totalSourceIndices} indices in source cluster");
                _logger.Log($"Found {_totalDestinationIndices} indices in destination cluster");

                // Identify missing indices
                var missingIndices = sourceIndices.Except(destinationIndices).ToList();
                if (!missingIndices.Any())
                {
                    _logger.Log("No indices missing in destination cluster");
                    LogSummary(missingIndices);
                    return;
                }

                _logger.Log($"Found {missingIndices.Count} indices missing in destination: {string.Join(", ", missingIndices)}");

                Console.WriteLine("Press Enter to proceed with migration");
                Console.ReadLine();

                // Process missing indices in batches
                for (int i = 0; i < missingIndices.Count; i += BatchSize)
                {
                    var batch = missingIndices.Skip(i).Take(BatchSize).ToList();
                    await MigrateBatchAsync(batch);
                }

                _logger.Log("All index migration completed");
                LogSummary(missingIndices);
            }
            catch (Exception ex)
            {
                _logger.Log($"Fatal error in all index migration: {ex.Message}");
                LogSummary(new List<string>());
                throw;
            }
        }

        private async Task<List<string>> GetAllIndicesAsync(string cluster, string clusterType)
        {
            try
            {
                string response = await (clusterType == "source" ? _sourceClient : _targetClient)
                    .GetAsync($"{cluster}/_cat/indices?h=i&format=json");
                var indices = JsonConvert.DeserializeObject<List<Dictionary<string, string>>>(response);
                return indices?.Select(index => index["i"]).Where(name => !string.IsNullOrEmpty(name)).ToList() ?? new List<string>();
            }
            catch (Exception ex)
            {
                _logger.Log($"Error fetching indices from {clusterType} cluster: {ex.Message}");
                return new List<string>();
            }
        }

        private async Task MigrateBatchAsync(List<string> indexNames)
        {
            _logger.Log($"Processing batch of {indexNames.Count} indices: {string.Join(", ", indexNames)}");

            foreach (var indexName in indexNames)
            {
                try
                {
                    _logger.Log($"Migrating index: {indexName}");

                    // Get settings, mappings, and aliases from source
                    string settingsAndMappings = await _sourceClient.GetAsync($"{_sourceCluster}/{indexName}/");
                    if (string.IsNullOrEmpty(settingsAndMappings))
                    {
                        _logger.Log($"Failed to retrieve settings and mappings for index: {indexName}");
                        _failedMigrations[indexName] = "Failed to retrieve settings and mappings";
                        continue;
                    }

                    // Parse and clean the JSON
                    var jObject = JObject.Parse(settingsAndMappings);
                    var indexData = jObject[indexName] as JObject;

                    if (indexData == null)
                    {
                        _logger.Log($"No data found for index: {indexName}");
                        _failedMigrations[indexName] = "No data found in response";
                        continue;
                    }

                    var aliases = indexData["aliases"] as JObject;
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
                    try
                    {
                        bool success = await _targetClient.PutAsync($"{_targetCluster}/{indexName}", payload.ToString(Formatting.None));
                        if (success)
                        {
                            _logger.Log($"Successfully migrated index: {indexName}");
                            _successfulMigrations++;
                        }
                        
                    }
                    catch (Exception ex)
                    {
                        _logger.Log($"Error migrating index {indexName}: {ex.Message}");
                        _failedMigrations[indexName] = ex.Message;
                    }

                }
                catch (Exception ex)
                {
                    _logger.Log($"Error migrating index {indexName}: {ex.Message}");
                    _failedMigrations[indexName] = ex.Message;
                }
            }
        }

        private void LogSummary(List<string> missingIndices)
        {
            var summary = new StringBuilder();
            summary.AppendLine("╔════════════════════════════════════════════════════╗");
            summary.AppendLine("║          All Index Migration Summary               ║");
            summary.AppendLine("╠════════════════════════════════════════════════════╣");
            summary.AppendLine($"║ Total Indices in Source:      {_totalSourceIndices,-10}         ║");
            summary.AppendLine($"║ Total Indices in Destination: {_totalDestinationIndices,-10}         ║");
            summary.AppendLine($"║ Missing Indices:             {missingIndices.Count,-10}         ║");
            summary.AppendLine($"║ Successfully Migrated:        {_successfulMigrations,-10}         ║");
            summary.AppendLine($"║ Skipped (Already Exist):      {_skippedMigrations,-10}         ║");
            summary.AppendLine($"║ Failed:                      {_failedMigrations.Count,-10}         ║");

            if (missingIndices.Any())
            {
                summary.AppendLine("╠════════════════════════════════════════════════════╣");
                summary.AppendLine("║ Missing Indices Identified:                        ║");
                foreach (var index in missingIndices)
                {
                    summary.AppendLine($"║ - {index,-20}                              ║");
                }
            }

            if (_failedMigrations.Any())
            {
                summary.AppendLine("╠════════════════════════════════════════════════════╣");
                summary.AppendLine("║ Failed Indices:                                    ║");
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