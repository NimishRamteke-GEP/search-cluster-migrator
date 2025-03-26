using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace opensearch_migrator
{
    // ElasticsearchClient.cs
    using System.Net.Http;
    using System.Text;

    public class ElasticsearchClient : IElasticsearchClient
    {
        private readonly HttpClient _client;
        private readonly ILogger _logger;

        public ElasticsearchClient(ILogger logger, string username = null, string password = null)
        {
            _logger = logger;
            //HttpClientHandler handler = new HttpClientHandler
            //{
            //    // This bypasses SSL certificate validation
            //    ServerCertificateCustomValidationCallback = (sender, cert, chain, sslPolicyErrors) => true
            //};
            _client = new HttpClient();

            if (!string.IsNullOrEmpty(username) && !string.IsNullOrEmpty(password))
            {
                _client.DefaultRequestHeaders.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue(
                    "Basic",
                    Convert.ToBase64String(Encoding.UTF8.GetBytes($"{username}:{password}")));
            }
        }

        public async Task<string> GetAsync(string endpoint)
        {
            try
            {
                HttpResponseMessage response = await _client.GetAsync(endpoint);
                var ct = response.Content.ReadAsStringAsync();
                response.EnsureSuccessStatusCode();
                return await response.Content.ReadAsStringAsync();
            }
            catch (Exception ex)
            {
                _logger.Log($"Error in GET request to {endpoint}: {ex.Message}");
                throw;
            }
        }

        public async Task<bool> PutAsync(string endpoint, string jsonContent)
        {
            try
            {
                var content = new StringContent(jsonContent, Encoding.UTF8, "application/json");
                HttpResponseMessage response = await _client.PutAsync(endpoint, content);
                var ct = response.Content.ReadAsStringAsync();
                response.EnsureSuccessStatusCode();

                return true;
            }
            catch (Exception ex)
            {
                _logger.Log($"Error in PUT request to {endpoint}: {ex.Message}");
                return false;
            }
        }
    }
}
