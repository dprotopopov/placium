using System.IO;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.DataProtection;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using MySql.QueryTools.WebApp.Filters;

namespace MySql.QueryTools.WebApp
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

        // This method gets called by the runtime. Use this method to add services to the container.
        public void ConfigureServices(IServiceCollection services)
        {
            services.AddDataProtection()
                .PersistKeysToFileSystem(
                    new DirectoryInfo(Path.Combine(WebHostEnvironment.ContentRootPath, "ProtectionKeys")));

            var config = Configuration.GetSection(nameof(ServerConfig)).Get<ServerConfig>();
            if (config.AddCors)
                services.AddCors(options => options.AddDefaultPolicy(
                    builder => builder
                        .SetIsOriginAllowed(x => true)
                        .AllowAnyHeader()
                        .AllowAnyMethod()));

            services.AddControllersWithViews(options => { options.Filters.Add(typeof(ExceptionFilter)); });
        }

        // This method gets called by the runtime. Use this method to configure the HTTP request pipeline.
        public void Configure(IApplicationBuilder app, IWebHostEnvironment env)
        {
            var config = Configuration.GetSection(nameof(ServerConfig)).Get<ServerConfig>();

            app.UsePathBase(config.PathBase);

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
                endpoints.MapControllerRoute(
                    "default",
                    "{controller=Query}/{action=Index}/{id?}");
            });
        }
    }
}