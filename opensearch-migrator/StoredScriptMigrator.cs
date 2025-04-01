// StoredScriptMigrator.cs
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using opensearch_migrator;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace opensearch_migrator
{
    public class StoredScriptMigrator
    {
        private readonly IElasticsearchClient _sourceClient;
        private readonly IElasticsearchClient _targetClient;
        private readonly ILogger _logger;
        private readonly string _sourceCluster;
        private readonly string _targetCluster;

        // Tracking for summary
        private int _totalScripts;
        private int _successfulMigrations;
        private int _skippedMigrations;
        private readonly Dictionary<string, string> _failedMigrations = new Dictionary<string, string>();

        public StoredScriptMigrator(
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

        public async Task MigrateAsync(string scriptIds)
        {
            _logger.Log($"Starting stored script migration process for scripts: {scriptIds}");

            try
            {
                // Reset tracking variables
                _totalScripts = 0;
                _successfulMigrations = 0;
                _skippedMigrations = 0;
                _failedMigrations.Clear();

                // Parse comma-separated script IDs
                var scriptIdList = scriptIds.Split(',')
                    .Select(id => id.Trim())
                    .Where(id => !string.IsNullOrEmpty(id))
                    .ToList();

                if (!scriptIdList.Any())
                {
                    _logger.Log("No valid script IDs provided");
                    LogSummary();
                    return;
                }

                _totalScripts = scriptIdList.Count;
                _logger.Log($"Found {_totalScripts} scripts to migrate");

                foreach (var scriptId in scriptIdList)
                {
                    await MigrateScriptAsync(scriptId);
                }

                _logger.Log("Stored script migration completed");
                LogSummary();
            }
            catch (Exception ex)
            {
                _logger.Log($"Fatal error in stored script migration: {ex.Message}");
                LogSummary();
                throw;
            }
        }

        private async Task MigrateScriptAsync(string scriptId)
        {
            try
            {
                _logger.Log($"Checking script: {scriptId}");

                // Check if script exists in target
                if (await ScriptExistsInTarget(scriptId))
                {
                    _logger.Log($"Script {scriptId} already exists in target cluster, skipping migration");
                    _skippedMigrations++;
                    return;
                }

                _logger.Log($"Processing script: {scriptId}");

                // Fetch script details from source
                string scriptDetails = await _sourceClient.GetAsync($"{_sourceCluster}/_scripts/{scriptId}");
                if (string.IsNullOrEmpty(scriptDetails))
                {
                    _logger.Log($"Failed to retrieve details for script: {scriptId}");
                    _failedMigrations[scriptId] = "Failed to retrieve script details";
                    return;
                }

                // Parse and prepare script payload
                var jObject = JObject.Parse(scriptDetails);
                jObject.Remove("_id");
                jObject.Remove("found");

                var scriptPayload = jObject.ToString(Formatting.None);
                if (string.IsNullOrEmpty(scriptPayload))
                {
                    _logger.Log($"No valid script content found for: {scriptId}");
                    _failedMigrations[scriptId] = "No valid script content in response";
                    return;
                }

                // Migrate to target

                try
                {
                    bool success = await _targetClient.PutAsync($"{_targetCluster}/_scripts/{scriptId}", scriptPayload);
                    if (success)
                    {
                        _logger.Log($"Successfully migrated script: {scriptId}");
                        _successfulMigrations++;
                    }

                }
                catch (Exception ex)
                {
                    _logger.Log($"Failed to migrate script: {scriptId}");
                    _failedMigrations[scriptId] = $"PUT request failed {ex.Message}";
                }
            }
            catch (Exception ex)
            {
                _logger.Log($"Error processing script {scriptId}: {ex.Message}");
                _failedMigrations[scriptId] = ex.Message;
            }
        }

        private async Task<bool> ScriptExistsInTarget(string scriptId)
        {
            try
            {
                string response = await _targetClient.GetAsync($"{_targetCluster}/_scripts/{scriptId}");
                return !string.IsNullOrEmpty(response);
            }
            catch (HttpRequestException ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
            {
                return false;
            }
            catch (Exception ex)
            {
                _logger.Log($"Error checking existence of script {scriptId} in target: {ex.Message}");
                return false;
            }
        }

        private void LogSummary()
        {
            var summary = new StringBuilder();
            summary.AppendLine("╔════════════════════════════════════════════════════╗");
            summary.AppendLine("║        Stored Script Migration Summary             ║");
            summary.AppendLine("╠════════════════════════════════════════════════════╣");
            summary.AppendLine($"║ Total Scripts Found:          {_totalScripts,-10}         ║");
            summary.AppendLine($"║ Successfully Migrated:        {_successfulMigrations,-10}         ║");
            summary.AppendLine($"║ Skipped (Already Exist):      {_skippedMigrations,-10}         ║");
            summary.AppendLine($"║ Failed:                      {_failedMigrations.Count,-10}         ║");

            if (_failedMigrations.Any())
            {
                summary.AppendLine("╠════════════════════════════════════════════════════╣");
                summary.AppendLine("║ Failed Scripts:                                    ║");
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