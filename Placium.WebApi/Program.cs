using System;
using System.Text;
using System.Threading.Tasks;
using Microsoft.AspNetCore;
using Microsoft.AspNetCore.Hosting;
using NetTopologySuite;
using NetTopologySuite.Geometries;
using NetTopologySuite.Geometries.Implementation;
using OsmSharp.Logging;
using Placium.Common;

namespace Placium.WebApi
{
    public class Program
    {
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
                .UseUnixSocketCredential()
                .UseIISIntegration()
                .UseStartup<Startup>();
        }
    }
}