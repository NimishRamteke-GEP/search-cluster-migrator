using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace opensearch_migrator
{
    public class IndexBatcher
    {
        private readonly IElasticsearchClient _sourceClient;
        private readonly ILogger _logger;
        private const int BatchSize = 400;

        public IndexBatcher(IElasticsearchClient sourceClient, ILogger logger)
        {
            _sourceClient = sourceClient;
            _logger = logger;
        }

        public async Task GenerateIndexBatchesAsync(string cluster)
        {
            try
            {
                _logger.Log("Fetching indices from source cluster...");
                var indices = await GetAllIndicesAsync(cluster);
                indices.RemoveAll(index => index.Contains("filebeat"));
                indices.RemoveAll(index => index.Contains("."));
                indices.RemoveAll(index => index.Contains("metric"));

                if (!indices.Any())
                {
                    _logger.Log("No indices found. Exiting.");
                    return;
                }

                _logger.Log($"Total Indices Retrieved: {indices.Count}");

                // Sort and batch indices
                var sortedIndices = indices.OrderBy(x => x).ToList();
                var batches = CreateBatches(sortedIndices);

                // Generate comma-separated values
                StringBuilder contentBuilder = new StringBuilder();
                foreach (var batch in batches)
                {
                    contentBuilder.AppendLine(string.Join(",", batch));
                    contentBuilder.AppendLine("===========================================");

                }

                // Save to a .txt file with timestamp
                string fileName = $"IndexBatches_{DateTime.Now:yyyyMMdd_HHmmss}.txt";
                await File.WriteAllTextAsync(fileName, contentBuilder.ToString());

                _logger.Log($"Batches generated and saved to {fileName}");
            }
            catch (Exception ex)
            {
                _logger.Log($"Error during index batching: {ex.Message}");
            }
        }

        private async Task<List<string>> GetAllIndicesAsync(string cluster)
        {
            try
            {
                string response = await _sourceClient.GetAsync($"{cluster}/_cat/indices?h=i&format=json");
                var indices = JsonConvert.DeserializeObject<List<Dictionary<string, string>>>(response);
               
                return indices?.Select(index => index["i"]).Where(name => !string.IsNullOrEmpty(name)).ToList() ?? new List<string>();
            }
            catch (Exception ex)
            {
                _logger.Log($"Error fetching indices: {ex.Message}");
                return new List<string>();
            }
        }

        private List<List<string>> CreateBatches(List<string> indices)
        {
            var batches = new List<List<string>>();
            for (int i = 0; i < indices.Count; i += BatchSize)
            {
                batches.Add(indices.Skip(i).Take(BatchSize).ToList());
            }
            _logger.Log($"Created {batches.Count} batches.");
            return batches;
        }
    }
}