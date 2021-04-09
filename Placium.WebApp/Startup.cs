using Loader.Fias;
using Loader.Osm;
using Microsoft.AspNetCore.Authentication.Cookies;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http.Features;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Placium.Common;
using Placium.Seeker;
using Placium.Services;
using Updater.Addr;
using Updater.Placex;
using Updater.Sphinx;

namespace Placium.WebApp
{
    public class Startup
    {
        public Startup(IConfiguration configuration)
        {
            Configuration = configuration;
        }

        private IConfiguration Configuration { get; }

        private void RegisterServices(IServiceCollection services)
        {
            services.Configure<UploadConfig>(Configuration.GetSection(nameof(UploadConfig)));
            services.Configure<AccountConfig>(Configuration.GetSection(nameof(AccountConfig)));

            services.AddTransient<DefaultSeeker>();
            services.AddTransient<PlacexService>();
            services.AddTransient<OsmService>();
            services.AddTransient<FiasService>();
            services.AddSingleton<FiasUploadService>();
            services.AddSingleton<OsmUploadService>();
            services.AddSingleton<PlacexUpdateService>();
            services.AddSingleton<AddrUpdateService>();
            services.AddSingleton<SphinxUpdateService>();
            services.AddSingleton<ProgressHub>();
        }

        public void ConfigureServices(IServiceCollection services)
        {
            RegisterServices(services);

            services.AddControllersWithViews();
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
                    options.LoginPath = new Microsoft.AspNetCore.Http.PathString("/Account/Login");
                });
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

            app.UseAuthentication();    // аутентификация
            app.UseAuthorization();     // авторизация

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