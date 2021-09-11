using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Loader.Fias;
using Loader.Osm;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Placium.Common;
using Updater.Addrx;
using Updater.Placex;
using Updater.Sphinx;

namespace placium
{
    internal class Program
    {
        private static readonly Type[] _types =
        {
            typeof(FiasUploadService),
            typeof(OsmUploadService),
            typeof(PlacexUpdateService),
            typeof(AddrxUpdateService),
            typeof(SphinxUpdateService),
            typeof(Sphinx1UpdateService),
            typeof(Sphinx2UpdateService),
            typeof(Sphinx3UpdateService)
        };

        private static async Task Main(string[] args)
        {
            Console.WriteLine("Welcome to Placium application!");

            var builder = new ConfigurationBuilder();
            builder.AddCommandLine(args);

            var config = builder.Build();

            var serviceProvider = new ServiceCollection()
                .AddLogging()
                .AddSingleton<IConfiguration>(config)
                .AddSingleton<IConnectionsConfig, ArgsConnectionsConfig>()
                .AddSingleton<IProgressClient, ShellProgressClient>()
                .AddSingleton<FiasUploadService>()
                .AddSingleton<OsmUploadService>()
                .AddSingleton<PlacexUpdateService>()
                .AddSingleton<AddrxUpdateService>()
                .AddSingleton<SphinxUpdateService>()
                .AddSingleton<Sphinx1UpdateService>()
                .AddSingleton<Sphinx2UpdateService>()
                .AddSingleton<Sphinx3UpdateService>()
                .BuildServiceProvider();

            try
            {
                var type = _types.FirstOrDefault(t => t.Name == config["service"]);
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