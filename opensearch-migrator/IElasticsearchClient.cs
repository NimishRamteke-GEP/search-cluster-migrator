using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace opensearch_migrator
{
    // IElasticsearchClient.cs
    public interface IElasticsearchClient
    {
        Task<string> GetAsync(string endpoint);
        Task<bool> PutAsync(string endpoint, string jsonContent);
    }
}
