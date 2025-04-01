// IngestPipelineMigrator.cs
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace opensearch_migrator
{
    public class IngestPipelineMigrator
    {
        private readonly IElasticsearchClient _sourceClient;
        private readonly IElasticsearchClient _targetClient;
        private readonly ILogger _logger;
        private readonly string _sourceCluster;
        private readonly string _targetCluster;

        // Tracking for summary
        private int _totalPipelines;
        private int _successfulMigrations;
        private int _skippedMigrations;
        private readonly Dictionary<string, string> _failedMigrations = new Dictionary<string, string>();

        public IngestPipelineMigrator(
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

        public async Task MigrateAsync(string pipelineIds)
        {
            _logger.Log($"Starting ingest pipeline migration process for pipelines: {pipelineIds}");

            try
            {
                // Reset tracking variables
                _totalPipelines = 0;
                _successfulMigrations = 0;
                _skippedMigrations = 0;
                _failedMigrations.Clear();

                // Parse comma-separated pipeline IDs
                var pipelineIdList = pipelineIds.Split(',')
                    .Select(id => id.Trim())
                    .Where(id => !string.IsNullOrEmpty(id))
                    .ToList();

                if (!pipelineIdList.Any())
                {
                    _logger.Log("No valid pipeline IDs provided");
                    LogSummary();
                    return;
                }

                _totalPipelines = pipelineIdList.Count;
                _logger.Log($"Found {_totalPipelines} ingest pipelines to migrate");

                foreach (var pipelineName in pipelineIdList)
                {
                    await MigratePipelineAsync(pipelineName);
                }

                _logger.Log("Ingest pipeline migration completed");
                LogSummary();
            }
            catch (Exception ex)
            {
                _logger.Log($"Fatal error in pipeline migration: {ex.Message}");
                LogSummary();
                throw;
            }
        }

        private async Task MigratePipelineAsync(string pipelineName)
        {
            try
            {
                _logger.Log($"Checking pipeline: {pipelineName}");

                // Check if pipeline exists in target cluster
                bool existsInTarget = await PipelineExistsInTarget(pipelineName);
                if (existsInTarget)
                {
                    _logger.Log($"Pipeline {pipelineName} already exists in target cluster, skipping migration");
                    _skippedMigrations++;
                    return;
                }

                // If it doesn't exist, proceed with migration
                _logger.Log($"Processing pipeline: {pipelineName}");
                string pipelineDetails = await _sourceClient.GetAsync($"{_sourceCluster}/_ingest/pipeline/{pipelineName}");

                if (string.IsNullOrEmpty(pipelineDetails))
                {
                    _logger.Log($"Failed to get details for pipeline: {pipelineName}");
                    _failedMigrations[pipelineName] = "Failed to retrieve pipeline details";
                    return;
                }
                var jbody = JObject.Parse(pipelineDetails);
                var pipelineBody = jbody[pipelineName].ToString(Formatting.None);
                try
                {
                    bool success = await _targetClient.PutAsync($"{_targetCluster}/_ingest/pipeline/{pipelineName}", pipelineBody);
                    if (success)
                    {
                        _logger.Log($"Successfully migrated pipeline: {pipelineName}");
                        _successfulMigrations++;
                    }

                }
                catch (Exception ex)
                {
                    _logger.Log($"Failed to migrate pipeline: {pipelineName}");
                    _failedMigrations[pipelineName] = $"PUT request failed {ex.Message}";
                }

            }
            catch (Exception ex)
            {
                _logger.Log($"Error processing pipeline {pipelineName}: {ex.Message}");
                _failedMigrations[pipelineName] = ex.Message;
            }
        }

        private async Task<bool> PipelineExistsInTarget(string pipelineName)
        {
            try
            {
                string response = await _targetClient.GetAsync($"{_targetCluster}/_ingest/pipeline/{pipelineName}");
                return !string.IsNullOrEmpty(response);
            }
            catch (HttpRequestException ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
            {
                return false;
            }
            catch (Exception ex)
            {
                _logger.Log($"Error checking existence of pipeline {pipelineName} in target: {ex.Message}");
                return false; // Assume it doesn't exist if there's an error, to avoid overwriting
            }
        }

        private void LogSummary()
        {
            var summary = new StringBuilder();
            summary.AppendLine("╔════════════════════════════════════════════════════╗");
            summary.AppendLine("║        Ingest Pipeline Migration Summary           ║");
            summary.AppendLine("╠════════════════════════════════════════════════════╣");
            summary.AppendLine($"║ Total Pipelines Found:        {_totalPipelines,-10}         ║");
            summary.AppendLine($"║ Successfully Migrated:        {_successfulMigrations,-10}         ║");
            summary.AppendLine($"║ Skipped (Already Exist):      {_skippedMigrations,-10}         ║");
            summary.AppendLine($"║ Failed:                      {_failedMigrations.Count,-10}         ║");

            if (_failedMigrations.Any())
            {
                summary.AppendLine("╠════════════════════════════════════════════════════╣");
                summary.AppendLine("║ Failed Pipelines:                                  ║");
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