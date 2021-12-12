using System;
using System.Collections.Generic;
using System.IO;
using AstraUtils;
using AstraUtils.UnixSocket;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.DependencyInjection;

namespace MySql.QueryTools.WebApp
{
    public static class WebHostBuilderExtensions
    {
        public static void SetSocketPermissions(this IWebHost host)
        {
            var paths = host.Services.GetRequiredService<SocketPathStore>().Paths;
            foreach (var item in paths) AstraLinuxHelper.SetSocketPermissions(item);
        }

        public static IWebHostBuilder UseUnixSocketCredential(this IWebHostBuilder builder)
        {
            builder.ConfigureKestrel(k =>
            {
                k.ConfigureEndpointDefaults(x =>
                {
                    Console.WriteLine("Socket path: {0}", x.SocketPath);
                    if (x.SocketPath != null)
                    {
                        x.ApplicationServices.GetRequiredService<SocketPathStore>().Paths.Add(x.SocketPath);

                        if (File.Exists(x.SocketPath))
                            try
                            {
                                File.Delete(x.SocketPath);
                            }
                            catch (Exception ex)
                            {
                                Console.WriteLine(ex);
                            }
                    }

                    x.AddUnixSocketCredentialFeature();
                });
            });

            builder.ConfigureServices((context, services) => { services.AddSingleton<SocketPathStore>(); });


            return builder;
        }

        private class SocketPathStore
        {
            public List<string> Paths { get; } = new List<string>();
        }
    }
}