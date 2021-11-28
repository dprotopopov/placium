using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.HttpOverrides;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.OpenApi.Models;
using NetTopologySuite.IO.Converters;
using Newtonsoft.Json;
using Placium.Common;
using Placium.Seeker;
using Placium.Services;

namespace Placium.WebApi
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
            services.Configure<ServerConfig>(Configuration.GetSection(nameof(ServerConfig)));

            services.AddSingleton<AddressService>();
            services.AddSingleton<PlacexService>();
            services.AddSingleton<OsmService>();
            services.AddSingleton<FiasService>();
        }

        public void ConfigureServices(IServiceCollection services)
        {
            RegisterServices(services);

            //services.AddCors(options => options.AddDefaultPolicy(
            //    builder => builder
            //        .SetIsOriginAllowed((x => true))
            //        .AllowAnyHeader()
            //        .AllowAnyMethod()));

            services.AddControllers()
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

            services.AddSignalR();
            services.AddHealthChecks();

            services.AddSwaggerGen(c =>
            {
                c.SwaggerDoc("v1", new OpenApiInfo {Title = "Placium API", Version = "v1"});
            });
        }

        public void Configure(IApplicationBuilder app, IWebHostEnvironment env)
        {
            var config = Configuration.GetSection(nameof(ServerConfig)).Get<ServerConfig>();

            app.UsePathBase(config.PathBase);

            app.UseForwardedHeaders();

            app.UseDefaultFiles();

            app.UseStaticFiles();

            app.UseCors();

            UseSwagger(app);

            app.UseRouting();

            app.UseEndpoints(endpoints => { endpoints.MapControllers(); });

            app.UseEndpoints(endpoints => { endpoints.MapHealthChecks("/health"); });
        }

        public void UseSwagger(IApplicationBuilder app)
        {
            app.UseSwagger(c =>
                c.RouteTemplate = "swagger/{documentName}/swagger.json"
            );

            app.UseSwaggerUI(c =>
            {
                c.RoutePrefix = "swagger";
                c.SwaggerEndpoint("v1/swagger.json", "Placium API");
            });
        }
    }
}
