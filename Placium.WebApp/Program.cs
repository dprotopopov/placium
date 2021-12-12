using System;
using System.Text;
using System.Threading.Tasks;
using Microsoft.AspNetCore;
using Microsoft.AspNetCore.Hosting;
using OsmSharp.Logging;
using Placium.Common;

namespace Placium.WebApp
{
    public class Program
    {
        public static async Task Main(string[] args)
        {
            // enable logging.
            Logger.LogAction = (o, level, message, parameters) =>
                Console.WriteLine("[{0}] {1} - {2}", o, level, message);
            Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);
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