using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using NLog.Web;

namespace MySql.QueryTools.WebApp
{
    public class Program
    {
        private const string SECRET_DIRECTORY = "secrets";
        private const string APPSETTINGS_FILE = "appsettings.json";
        private static readonly string SECRET_APPSETTINGS_PATH = Path.Combine(SECRET_DIRECTORY, APPSETTINGS_FILE);

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