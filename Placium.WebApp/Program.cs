using System;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using Microsoft.AspNetCore;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using NetTopologySuite;
using NetTopologySuite.Geometries;
using NetTopologySuite.Geometries.Implementation;
using NLog.Web;
using OsmSharp.Logging;
using Placium.Common;

namespace Placium.WebApp
{
    public class Program
    {
        private const string SECRET_DIRECTORY = "secrets";
        private const string APPSETTINGS_FILE = "appsettings.json";
        private static readonly string SECRET_APPSETTINGS_PATH = Path.Combine(SECRET_DIRECTORY, APPSETTINGS_FILE);

        public static async Task Main(string[] args)
        {
            // enable logging.
            Logger.LogAction = (o, level, message, parameters) =>
                Console.WriteLine("[{0}] {1} - {2}", o, level, message);

            Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);

            NtsGeometryServices.Instance = new NtsGeometryServices(
                CoordinateArraySequenceFactory.Instance,
                new PrecisionModel(),
                4326, GeometryOverlay.NG,
                new CoordinateEqualityComparer());

            using var host = CreateWebHostBuilder(args).Build();

            await host.StartAsync();
            host.SetSocketPermissions();
            await host.WaitForShutdownAsync();
        }

        public static IWebHostBuilder CreateWebHostBuilder(string[] args)
        {
            return WebHost.CreateDefaultBuilder(args)
                .ConfigureAppConfiguration(ic => ic
                    .AddJsonFile(APPSETTINGS_FILE)
                    .AddJsonFile(SECRET_APPSETTINGS_PATH, true)
                    .AddEnvironmentVariables()
                )
                .ConfigureLogging((_, logging) => { logging.ClearProviders(); })
                .UseNLog()
                .UseUnixSocketCredential()
                .UseIISIntegration()
                .UseStartup<Startup>();
        }
    }
}