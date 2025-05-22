using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace opensearch_migrator
{
    // Configuration.cs (with DotNetEnv)
    using DotNetEnv;

    public class Configuration
    {
        public string SourceCluster { get; }
        public string TargetCluster { get; }
        public string SourceUsername { get; }
        public string SourcePassword { get; }
        public string TargetUsername { get; }
        public string TargetPassword { get; }

        public Configuration(string envFilePath = "..\\..\\..\\.env")
        {
            Env.Load(envFilePath); // Load .env file if it exists
            SourceCluster = Env.GetString("SOURCE_ES_CLUSTER")
                ?? throw new ArgumentNullException("SOURCE_ES_CLUSTER environment variable is not set");
            TargetCluster = Env.GetString("TARGET_ES_CLUSTER")
                ?? throw new ArgumentNullException("TARGET_ES_CLUSTER environment variable is not set");
            SourceUsername = Env.GetString("SOURCE_ES_USERNAME");
            SourcePassword = Env.GetString("SOURCE_ES_PASSWORD");
            TargetUsername = Env.GetString("TARGET_ES_USERNAME");
            TargetPassword = Env.GetString("TARGET_ES_PASSWORD");
        }
    }
}
