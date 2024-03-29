﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Loader.Fias.File;
using Loader.Gar.File;
using Loader.Osm.File;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using NetTopologySuite;
using NetTopologySuite.Geometries;
using NetTopologySuite.Geometries.Implementation;
using NLog.Extensions.Logging;
using Placium.Common;
using Placium.Route;
using Updater.Fiasx.Sphinx;
using Updater.Addrx.Database;
using Updater.Addrx.Sphinx;
using Updater.Garx.Sphinx;
using Updater.Placex.Database;

namespace placium
{
    internal class Program
    {
        private static readonly Type[] Types =
        {
            typeof(FileFiasUploadService),
            typeof(FileGarUploadService),
            typeof(FileOsmUploadService),
            typeof(DatabaseRouteUpdateService),
            typeof(DatabasePlacexUpdateService),
            typeof(DatabaseAddrxUpdateService),
            typeof(SphinxAddrxUpdateService),
            typeof(SphinxFiasxUpdateService),
            typeof(SphinxGarxUpdateService)
        };

        private static async Task<int> Main(string[] args)
        {
            Console.WriteLine("Welcome to Placium!");

            Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);

            NtsGeometryServices.Instance = new NtsGeometryServices(
                CoordinateArraySequenceFactory.Instance,
                new PrecisionModel(),
                4326, GeometryOverlay.NG,
                new CoordinateEqualityComparer());

            var builder = new ConfigurationBuilder();
            builder.AddCommandLine(args);

            var config = builder.Build();

            var serviceProvider = new ServiceCollection()
                .AddLogging(logging =>
                {
                    logging.ClearProviders();
                    logging.AddNLog(config);
                })
                .AddSingleton<IConfiguration>(config)
                .AddSingleton<IConnectionsConfig, ArgsConnectionsConfig>()
                .AddSingleton<ISphinxConfig, ArgsSphinxConfig>()
                .AddSingleton<IParallelConfig, ArgsParallelConfig>()
                .AddSingleton<IProgressClient, ShellProgressClient>()
                .AddSingleton<FileFiasUploadService>()
                .AddSingleton<FileGarUploadService>()
                .AddSingleton<FileOsmUploadService>()
                .AddSingleton<DatabaseRouteUpdateService>()
                .AddSingleton<DatabasePlacexUpdateService>()
                .AddSingleton<DatabaseAddrxUpdateService>()
                .AddSingleton<SphinxAddrxUpdateService>()
                .AddSingleton<SphinxFiasxUpdateService>()
                .AddSingleton<SphinxGarxUpdateService>()
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
                        await using var stream = File.OpenRead(config["file"]);
                        switch (config["action"])
                        {
                            case "install":
                                await uploadService.InstallAsync(stream, new Dictionary<string, string>
                                {
                                    { "region", config["region"] }
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
                return 0;
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                return -1;
            }
        }
    }
}