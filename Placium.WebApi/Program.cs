using System.Text;
using System.Threading.Tasks;
using Microsoft.AspNetCore;
using Microsoft.AspNetCore.Hosting;
using Placium.Common;

namespace Placium.WebApi
{
    public class Program
    {
        public static async Task Main(string[] args)
        {
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