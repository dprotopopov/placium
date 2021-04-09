using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
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
        }

        public void Configure(IApplicationBuilder app, IWebHostEnvironment env)
        {
            app.UseDefaultFiles();

            app.UseStaticFiles();

            app.UseCors();

            app.Map("/api", x =>
            {
                x.UseRouting();

                x.UseEndpoints(endpoints => { endpoints.MapControllers(); });
            });

            app.UseRouting();

            app.UseAuthentication(); // аутентификация
            app.UseAuthorization(); // авторизация

            app.UseEndpoints(endpoints =>
            {
                endpoints.MapControllers();
                endpoints.MapHealthChecks("/health");
            });
        }
    }
}