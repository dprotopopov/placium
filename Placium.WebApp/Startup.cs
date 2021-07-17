using System;
using System.IO;
using Loader.Fias;
using Loader.Osm;
using Microsoft.AspNetCore.Authentication.Cookies;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.DataProtection;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.Features;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using NetTopologySuite.IO.Converters;
using Newtonsoft.Json;
using Placium.Common;
using Placium.Seeker;
using Placium.Services;
using Updater.Addrx;
using Updater.Placex;
using Updater.Sphinx;

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
        public IWebHostEnvironment WebHostEnvironment { get; }

        private void RegisterServices(IServiceCollection services)
        {
            services.Configure<SphinxConfig>(Configuration.GetSection(nameof(SphinxConfig)));
            services.Configure<UploadConfig>(Configuration.GetSection(nameof(UploadConfig)));
            services.Configure<AccountConfig>(Configuration.GetSection(nameof(AccountConfig)));

            services.AddSingleton<DefaultSeeker>();
            services.AddSingleton<PlacexService>();
            services.AddSingleton<OsmService>();
            services.AddSingleton<FiasService>();
            services.AddSingleton<FiasUploadService>();
            services.AddSingleton<OsmUploadService>();
            services.AddSingleton<PlacexUpdateService>();
            services.AddSingleton<AddrxUpdateService>();
            services.AddSingleton<SphinxUpdateService>();
            services.AddSingleton<Sphinx1UpdateService>();
            services.AddSingleton<Sphinx2UpdateService>();
            services.AddSingleton<Sphinx3UpdateService>();
            services.AddSingleton<ProgressHub>();
        }

        public void ConfigureServices(IServiceCollection services)
        {
            RegisterServices(services);

            services.AddControllersWithViews()
                .AddNewtonsoftJson(options =>
                {
                    options.SerializerSettings.ReferenceLoopHandling = ReferenceLoopHandling.Ignore;
                    options.SerializerSettings.Converters.Add(new GeometryConverter());
                    options.SerializerSettings.Converters.Add(new CoordinateConverter());
                });

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

            // установка конфигурации подключения
            services.AddAuthentication(CookieAuthenticationDefaults.AuthenticationScheme)
                .AddCookie(options => //CookieAuthenticationOptions
                {
                    options.LoginPath = new PathString("/Account/Login");
                });

            var keysFolder = Path.Combine(WebHostEnvironment.ContentRootPath, "temp-keys");
            services.AddDataProtection()
                .SetApplicationName("WebApp")
                .PersistKeysToFileSystem(new DirectoryInfo(keysFolder))
                .SetDefaultKeyLifetime(TimeSpan.FromDays(14));
        }

        public void Configure(IApplicationBuilder app, IWebHostEnvironment env)
        {
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

            app.UseAuthentication(); // аутентификация
            app.UseAuthorization(); // авторизация

            app.UseEndpoints(endpoints =>
            {
                endpoints.MapControllerRoute(
                    "default",
                    "{controller=Home}/{action=Index}/{id?}");
            });

            app.UseEndpoints(endpoints => { endpoints.MapHub<ProgressHub>("/progress"); });
        }
    }
}
