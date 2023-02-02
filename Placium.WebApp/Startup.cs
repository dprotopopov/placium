using System.IO;
using Loader.Fias.File;
using Loader.Gar.File;
using Loader.Osm.File;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.DataProtection;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http.Features;
using Microsoft.AspNetCore.HttpOverrides;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using NetTopologySuite.IO.Converters;
using Newtonsoft.Json;
using Placium.Common;
using Placium.Route;
using Placium.Services;
using Placium.WebApp.Filters;
using Updater.Addrobx.Sphinx;
using Updater.Addrx.Database;
using Updater.Addrx.Sphinx;
using Updater.Placex.Database;

namespace Placium.WebApp
{
    public class Startup
    {
        public Startup(IConfiguration configuration, IWebHostEnvironment webHostEnvironment)
        {
            Configuration = configuration;
            WebHostEnvironment = webHostEnvironment;
        }

        private IConfiguration Configuration { get; }
        private IWebHostEnvironment WebHostEnvironment { get; }

        private void RegisterServices(IServiceCollection services)
        {
            services.AddSingleton<IConnectionsConfig, AppsettingsConnectionsConfig>();
            services.AddSingleton<ISphinxConfig, AppsettingsSphinxConfig>();
            services.AddSingleton<IParallelConfig, AppsettingsParallelConfig>();
            services.AddSingleton<IProgressClient, SignalRProgressClient>();

            services.Configure<SphinxConfig>(Configuration.GetSection(nameof(SphinxConfig)));
            services.Configure<UploadConfig>(Configuration.GetSection(nameof(UploadConfig)));
            services.Configure<ServerConfig>(Configuration.GetSection(nameof(ServerConfig)));
            services.Configure<ParallelConfig>(Configuration.GetSection(nameof(ParallelConfig)));

            services.AddSingleton<PlacexService>();
            services.AddSingleton<OsmService>();
            services.AddSingleton<GarService>();
            services.AddSingleton<FiasService>();
            services.AddSingleton<FileFiasUploadService>();
            services.AddSingleton<FileGarUploadService>();
            services.AddSingleton<FileOsmUploadService>();
            services.AddSingleton<DatabaseRouteUpdateService>();
            services.AddSingleton<DatabasePlacexUpdateService>();
            services.AddSingleton<DatabaseAddrxUpdateService>();
            services.AddSingleton<SphinxAddrxUpdateService>();
            services.AddSingleton<SphinxAddrobxUpdateService>();
            services.AddSingleton<ProgressHub>();
        }

        public void ConfigureServices(IServiceCollection services)
        {
            services.AddDataProtection()
                .PersistKeysToFileSystem(
                    new DirectoryInfo(Path.Combine(WebHostEnvironment.ContentRootPath, "ProtectionKeys")));

            RegisterServices(services);

            var config = Configuration.GetSection(nameof(ServerConfig)).Get<ServerConfig>();
            if (config.AddCors)
                services.AddCors(options => options.AddDefaultPolicy(
                    builder => builder
                        .SetIsOriginAllowed(x => true)
                        .AllowAnyHeader()
                        .AllowAnyMethod()));

            services.AddControllersWithViews(options => { options.Filters.Add(typeof(ExceptionFilter)); })
                .AddNewtonsoftJson(options =>
                {
                    options.SerializerSettings.ReferenceLoopHandling = ReferenceLoopHandling.Ignore;
                    options.SerializerSettings.Converters.Add(new GeometryConverter());
                    options.SerializerSettings.Converters.Add(new CoordinateConverter());
                });

            services.Configure<ForwardedHeadersOptions>(options =>
            {
                options.ForwardedHeaders =
                    ForwardedHeaders.XForwardedFor | ForwardedHeaders.XForwardedProto;
            });

            services.AddRazorPages();
            services.AddSignalR();
            services.Configure<IISServerOptions>(options => { options.MaxRequestBodySize = int.MaxValue; });
            services.Configure<KestrelServerOptions>(options =>
            {
                options.Limits.MaxRequestBodySize = int.MaxValue; // if don't set default value is: 30 MB
            });
            services.Configure<FormOptions>(x =>
            {
                x.ValueLengthLimit = int.MaxValue;
                x.MultipartBodyLengthLimit = int.MaxValue; // if don't set default value is: 128 MB
                x.MultipartHeadersLengthLimit = int.MaxValue;
            });
        }

        public void Configure(IApplicationBuilder app, IWebHostEnvironment env)
        {
            var config = Configuration.GetSection(nameof(ServerConfig)).Get<ServerConfig>();

            app.UsePathBase(config.PathBase);

            app.UseForwardedHeaders();

            if (env.IsDevelopment())
            {
                app.UseDeveloperExceptionPage();
            }
            else
            {
                app.UseExceptionHandler("/Home/Error");
                // The default HSTS value is 30 days. You may want to change this for production scenarios, see https://aka.ms/aspnetcore-hsts.
                app.UseHsts();
            }

            app.UseHttpsRedirection();
            app.UseStaticFiles();

            app.UseRouting();

            app.UseEndpoints(endpoints =>
            {
                endpoints.MapRazorPages();
                endpoints.MapDefaultControllerRoute();
                endpoints.MapHub<ProgressHub>("/progress");
            });
        }
    }
}