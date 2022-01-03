using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Placium.Common;
using Placium.Route;
using Placium.Route.Osm.Vehicles;
using Route.LocalGeo;

namespace Placium.Seeker
{
    public class OsmRouteService : BaseApiService
    {
        public OsmRouteService(IConfiguration configuration) : base(configuration)
        {
        }

        public async Task<string> CalculateAsync(Coordinate source, Coordinate target)
        {
            var routerDb = new RouterDb(Guid.Parse("28662f4a-3d30-464e-9b64-c5e25457b2f1"), GetRouteConnectionString(),
                new[] { Vehicle.Car });

            // create router.
            var router = new Router(routerDb, "car");

            // calculate route.
            var route = await router.CalculateAsync(source, target);

            return route.ToGeoJson();
        }
    }
}