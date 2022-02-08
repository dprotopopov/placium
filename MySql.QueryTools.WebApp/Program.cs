using System.Threading.Tasks;
using Microsoft.AspNetCore;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Logging;

namespace MySql.QueryTools.WebApp
{
    public class Program
    {
        public static async Task Main(string[] args)
        {
            using var host = CreateWebHostBuilder(args).Build();

            await host.StartAsync();
            host.SetSocketPermissions();
            await host.WaitForShutdownAsync();
        }

        public static IWebHostBuilder CreateWebHostBuilder(string[] args)
        {
            return WebHost.CreateDefaultBuilder(args)
                .ConfigureLogging((_, logging) =>
                {
                    logging.ClearProviders();
                    logging.AddSimpleConsole(options => options.IncludeScopes = true);
                    logging.AddEventLog();
                })
                .UseUnixSocketCredential()
                .UseIISIntegration()
                .UseStartup<Startup>();
        }
    }
}