using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Placium.Common;
using Placium.IO.Osm;
using Route;
using Route.IO.Osm;
using Route.LocalGeo;
using Route.Osm.Vehicles;

namespace Placium.Seeker
{
    public class OsmRouteService : BaseApiService
    {
        public OsmRouteService(IConfiguration configuration) : base(configuration)
        {
        }

        public async Task<string> CalculateAsync(Coordinate source, Coordinate target)
        {
            var routerDb = new RouterDb(GetRouteConnectionString(),GetOsmConnectionString());

            routerDb.LoadOsmDataFromPostgreSQL(GetOsmConnectionString(), Vehicle.Car);

            // get the profile from the routerdb.
            // this is best-practice in Itinero, to prevent mis-matches.
            var car = routerDb.GetSupportedProfile("car");

            // add a contraction hierarchy.
            routerDb.AddContracted(car);

            // create router.
            var router = new Router(routerDb);

            // calculate route.
            var route = router.Calculate(car, source, target);
            return route.ToGeoJson();
        }
    }
}