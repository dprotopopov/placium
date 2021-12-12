using System.Threading.Tasks;
using Itinero;
using Itinero.LocalGeo;
using Itinero.Osm.Vehicles;
using Microsoft.Extensions.Configuration;
using Placium.Common;
using Placium.IO.Osm;

namespace Placium.Seeker
{
    public class OsmRouteService : BaseApiService
    {
        public OsmRouteService(IConfiguration configuration) : base(configuration)
        {
        }

        public async Task<string> CalculateAsync(Coordinate source, Coordinate target, Box box)
        {
            var routerDb = new RouterDb();

            routerDb.LoadOsmDataFromPostgreSQL(GetOsmConnectionString(), box, Vehicle.Car);

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