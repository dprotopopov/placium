using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Loader.Fias.File;
using Loader.Osm.File;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Placium.Common;
using Placium.Route;
using Updater.Addrobx.Sphinx;
using Updater.Addrx.Database;
using Updater.Addrx.Sphinx;
using Updater.Placex.Database;

namespace placium
{
    internal class Program
    {
        private static readonly Type[] Types =
        {
            typeof(FileFiasUploadService),
            typeof(FileOsmUploadService),
            typeof(DatabaseRouteUpdateService),
            typeof(DatabasePlacexUpdateService),
            typeof(DatabaseAddrxUpdateService),
            typeof(SphinxAddrxUpdateService),
            typeof(SphinxAddrobxUpdateService)
        };

        private static async Task Main(string[] args)
        {
            Console.WriteLine("Welcome to Placium!");

            var builder = new ConfigurationBuilder();
            builder.AddCommandLine(args);

            var config = builder.Build();

            var serviceProvider = new ServiceCollection()
                .AddLogging()
                .AddSingleton<IConfiguration>(config)
                .AddSingleton<IConnectionsConfig, ArgsConnectionsConfig>()
                .AddSingleton<IParallelConfig, ArgsParallelConfig>()
                .AddSingleton<IProgressClient, ShellProgressClient>()
                .AddSingleton<FileFiasUploadService>()
                .AddSingleton<FileOsmUploadService>()
                .AddSingleton<DatabaseRouteUpdateService>()
                .AddSingleton<DatabasePlacexUpdateService>()
                .AddSingleton<DatabaseAddrxUpdateService>()
                .AddSingleton<SphinxAddrxUpdateService>()
                .AddSingleton<SphinxAddrobxUpdateService>()
                .BuildServiceProvider();

            try
            {
                var type = Types.FirstOrDefault(t => t.Name == config["service"]);
                if (type == null) throw new Exception($"Unknown service '{config["service"]}'");
                var service = serviceProvider.GetService(type);
                switch (service)
                {
                    case IUploadService uploadService:
                    {
                        using var stream = File.OpenRead(config["file"]);
                        switch (config["action"])
                        {
                            case "install":
                                await uploadService.InstallAsync(stream, new Dictionary<string, string>
                                {
                                    {"region", config["region"]}
                                }, null);
                                break;
                            case "update":
                                await uploadService.UpdateAsync(stream, new Dictionary<string, string>(), null);
                                break;
                            default:
                                throw new Exception($"Unknown action '{config["action"]}");
                        }
                    }
                        break;
                    case IUpdateService updateService:
                        switch (config["action"])
                        {
                            case "update":
                                await updateService.UpdateAsync(null, config["full"] == "yes");
                                break;
                            default:
                                throw new Exception($"Unknown action '{config["action"]}");
                        }

                        break;
                    default:
                        throw new NotSupportedException();
                }

                Console.WriteLine("Complete");
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
        }
    }
}