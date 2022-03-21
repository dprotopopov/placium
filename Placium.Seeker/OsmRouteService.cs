using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Placium.Common;
using Placium.Route;
using Placium.Route.Osm.Vehicles;
using Route.LocalGeo;

namespace Placium.Seeker
{
    public class OsmRouteService : BaseApiService
    {
        private readonly ILogger _logger;

        public OsmRouteService(IConfiguration configuration, ILogger<OsmRouteService> logger) : base(configuration)
        {
            _logger = logger;
        }

        public async Task<string> CalculateAsync(Coordinate source, Coordinate target)
        {
            try
            {
                var routerDb = new RouterDb(Guid.Parse("28662f4a-3d30-464e-9b64-c5e25457b2f1"),
                    GetRouteConnectionString(),
                    new Car());

                // create router.
                var router = new Router(routerDb, "Car", 3.6f / 5f, 3.6f / 120f);

                // calculate route.
                var route = await router.CalculateAsync(source, target);

                return route.ToGeoJson();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex.Message);
                throw;
            }
        }
    }
}