using System.Threading.Tasks;
using Microsoft.AspNetCore;
using Microsoft.AspNetCore.Hosting;

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
                .UseUnixSocketCredential()
                .UseIISIntegration()
                .UseStartup<Startup>();
        }
    }
}