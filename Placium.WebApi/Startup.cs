using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.OpenApi.Models;
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
            services.AddTransient<DefaultSeeker>();
            services.AddTransient<PlacexService>();
            services.AddTransient<OsmService>();
            services.AddTransient<FiasService>();
        }

        public void ConfigureServices(IServiceCollection services)
        {
            RegisterServices(services);

            services.AddControllers();
            services.AddSignalR();
            services.AddHealthChecks();

            services.AddSwaggerGen(c =>
            {
                c.SwaggerDoc("v1", new OpenApiInfo {Title = "Placium API", Version = "v1"});
            });
        }

        public void Configure(IApplicationBuilder app, IWebHostEnvironment env)
        {
            app.UseDefaultFiles();

            app.UseStaticFiles();

            app.UseCors();

            UseSwagger(app);

            app.UseRouting();

            app.UseEndpoints(endpoints =>
            {
                endpoints.MapControllers();
            });

            app.UseAuthentication(); // аутентификация
            app.UseAuthorization(); // авторизация

            app.UseEndpoints(endpoints =>
            {
                endpoints.MapHealthChecks("/health");
            });
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