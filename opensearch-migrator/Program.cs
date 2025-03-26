// Program.cs
using System;
using System.Threading.Tasks;

namespace opensearch_migrator
{
    class Program
    {
        static async Task Main(string[] args)
        {
            var logger = new FileLogger();
            try
            {
                var config = new Configuration();

                var sourceClient = new ElasticsearchClient(logger, config.SourceUsername, config.SourcePassword);
                var targetClient = new ElasticsearchClient(logger, config.TargetUsername, config.TargetPassword);

                while (true)
                {
                    DisplayMenu();
                    string input = Console.ReadLine()?.Trim();

                    if (string.IsNullOrEmpty(input) || !int.TryParse(input, out int choice))
                    {
                        Console.WriteLine("Invalid input. Please enter a number between 1 and 6.");
                        continue;
                    }

                    switch (choice)
                    {
                        case 1:
                            var templateValidator = new IndexTemplateValidator(sourceClient, targetClient, logger, config.SourceCluster, config.TargetCluster);
                            await templateValidator.ValidateAsync();
                            break;

                        case 2:
                            var templateMigrator = new IndexTemplateMigrator(sourceClient, targetClient, logger, config.SourceCluster, config.TargetCluster);
                            Console.Write("Enter index template pattern (e.g., default_*): ");
                            string indexTemplatePattern = Console.ReadLine()?.Trim();
                            if (string.IsNullOrEmpty(indexTemplatePattern))
                            {
                                Console.WriteLine("Index pattern cannot be empty. Using default 'default_*'.");
                                indexTemplatePattern = "default_*";
                            }
                            await templateMigrator.MigrateAsync(indexTemplatePattern);
                            break;

                        case 3:
                            var pipelineValidator = new IngestPipelineValidator(sourceClient, targetClient, logger, config.SourceCluster, config.TargetCluster);
                            await pipelineValidator.ValidateAsync();
                            break;

                        case 4:
                            Console.Write("Enter comma-separated script IDs (e.g., addElasticTimeStamp,addIndexedAtTimeStamp,clm-attachment): ");
                            string ingestPipelines = Console.ReadLine()?.Trim();
                            if (string.IsNullOrEmpty(ingestPipelines))
                            {
                                Console.WriteLine("Script IDs cannot be empty. Using default: addElasticTimeStamp,addIndexedAtTimeStamp,clm-attachment");
                                ingestPipelines = "addElasticTimeStamp,addIndexedAtTimeStamp,clm-attachment";
                            }
                            var ingestPipelineMigrator = new IngestPipelineMigrator(sourceClient, targetClient, logger, config.SourceCluster, config.TargetCluster);
                            await ingestPipelineMigrator.MigrateAsync(ingestPipelines);
                            break;

                        case 5:
                            Console.Write("Enter index pattern (e.g., dm-idx-contra*): ");
                            string indexPattern = Console.ReadLine()?.Trim();
                            if (string.IsNullOrEmpty(indexPattern))
                            {
                                Console.WriteLine("Index pattern cannot be empty. Using default 'dm-idx-contra*'.");
                                indexPattern = "dm-idx-contra*";
                            }
                            var indexMigrator = new IndexMigrator(sourceClient, targetClient, logger, config.SourceCluster, config.TargetCluster);
                            await indexMigrator.MigrateAsync(indexPattern);
                            break;
                        case 6:
                            Console.Write("Enter comma-separated script IDs (e.g., generic-fields-rename-or-remove,domainmodel-partial-update,domainmodel-partial-delete)");
                            string scriptIds = Console.ReadLine()?.Trim();
                            if (string.IsNullOrEmpty(scriptIds))
                            {
                                Console.WriteLine("Script IDs cannot be empty. Using default: generic-fields-rename-or-remove,domainmodel-partial-update,domainmodel-partial-delete");
                                scriptIds = "generic-fields-rename-or-remove,domainmodel-partial-update,domainmodel-partial-delete";
                            }
                            var scriptMigrator = new StoredScriptMigrator(sourceClient, targetClient, logger, config.SourceCluster, config.TargetCluster);
                            await scriptMigrator.MigrateAsync(scriptIds);
                            break;

                        case 7:
                            Console.WriteLine("Exiting program...");
                            return;
                        case 99:
                            Console.WriteLine("Exiting program...");
                            return;
                        default:
                            Console.WriteLine("Invalid choice. Please select a number between 1 and 6.");
                            break;
                    }

                    Console.WriteLine("\nPress any key to return to the menu...");
                    Console.ReadKey(true);
                    Console.Clear();
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Fatal error: {ex.Message}");
                logger.Log($"Fatal error in program execution: {ex.Message}");
            }
        }

        private static void DisplayMenu()
        {
            Console.WriteLine("╔═══════════════════════════════════════════════════════╗");
            Console.WriteLine("║         Elasticsearch Migration Tool                  ║");
            Console.WriteLine("╠═══════════════════════════════════════════════════════╣");
            Console.WriteLine("║ Select an action:                                     ║");
            Console.WriteLine("║ 1. Validate Index Templates                           ║");
            Console.WriteLine("║ 2. Migrate Index Templates (with pattern)             ║");
            Console.WriteLine("║ 3. Validate Ingest Pipelines (comma-separated IDs)    ║");
            Console.WriteLine("║ 4. Migrate Ingest Pipelines (comma-separated IDs)     ║");
            Console.WriteLine("║ 5. Migrate Indices (with pattern)                     ║");
            Console.WriteLine("║ 6. Migrate Stored Scripts (comma separated values)    ║");
            Console.WriteLine("║ 99. Exit                                              ║");
            Console.WriteLine("╚═══════════════════════════════════════════════════════╝");
            Console.Write("Enter your choice (1-6): ");
        }
    }
}