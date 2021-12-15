using System;
using System.IO;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using OsmSharp.Logging;
using Placium.Common;
using Placium.IO.Osm;
using Route;
using Route.LocalGeo;
using Route.Osm.Vehicles;

namespace Sample.Basic
{
    internal class Program
    {
        private static void Main(string[] args)
        {
            var builder = new ConfigurationBuilder();
            builder.AddCommandLine(args);

            var config = builder.Build();

            var serviceProvider = new ServiceCollection()
                .AddLogging()
                .AddSingleton<IConfiguration>(config)
                .AddSingleton<IConnectionsConfig, ArgsConnectionsConfig>()
                .BuildServiceProvider();

            var connectionsConfig = serviceProvider.GetService<IConnectionsConfig>();

            // enable logging.
            Logger.LogAction = (o, level, message, parameters) =>
            {
                Console.WriteLine("[{0}] {1} - {2}", o, level, message);
            };
            Route.Logging.Logger.LogAction = (o, level, message, parameters) =>
            {
                Console.WriteLine("[{0}] {1} - {2}", o, level, message);
            };

            // load some routing data and create a router.
            var routerDb = new RouterDb();

            routerDb.LoadOsmDataFromPostgreSQL(connectionsConfig.GetConnectionString("OsmConnection"), Vehicle.Car);

            // get the profile from the routerdb.
            // this is best-practice in Itinero, to prevent mis-matches.
            var car = routerDb.GetSupportedProfile("car");

            // add a contraction hierarchy.
            routerDb.AddContracted(car);

            // create router.
            var router = new Router(routerDb);

            // calculate route.
            // this should be the result: http://geojson.io/#id=gist:dprotopopov/34df4ce18b6e974bb2ee9123b29d46c4&map=16/55.8223/37.6331
            var route = router.Calculate(car, new Coordinate(55.821861f, 37.626996f),
                new Coordinate(55.820427f, 37.639986f));
            var routeGeoJson = route.ToGeoJson();
            File.WriteAllText("route1.geojson", routeGeoJson);

            // calculate a sequence.
            // http://geojson.io/#id=gist:dprotopopov/75184f6877efa96121f12811a95ad191&map=12/55.7770/37.6445
            var locations = new[]
            {
                new Coordinate(55.820223f, 37.627041f),
                new Coordinate(55.771024f, 37.633490f),
                new Coordinate(55.750117f, 37.658210f),
                new Coordinate(55.732686f, 37.639842f),
                new Coordinate(55.723780f, 37.654816f)
            };
            route = router.Calculate(car, locations);
            routeGeoJson = route.ToGeoJson();
            File.WriteAllText("sequence1-undirected.geojson", routeGeoJson);

            // calculate a directed sequence with a turn penalty of 120 secs.
            // this should be the result: http://geojson.io/#id=gist:xivk/49f5d843c16adb68c740f8fc0b4d8583&map=16/49.5881/6.1115
            route = router.Calculate(car, locations, 120, preferredDirections: null);
            routeGeoJson = route.ToGeoJson();
            File.WriteAllText("sequence2-turn-penalty-120.geojson", routeGeoJson);

            // calculate a directed sequence without turn penalty but with a departure angle.
            // this should be the result: http://geojson.io/#id=gist:xivk/c93be9a18072a78ea931dbc5a772f34f&map=16/49.5881/6.1111
            var angles = new float?[]
            {
                -90, // leave west.
                null, // don't-care
                null, // don't-care
                null, // don't-care
                null // don't-care
            };
            route = router.Calculate(car, locations, preferredDirections: angles);
            routeGeoJson = route.ToGeoJson();
            File.WriteAllText("sequence3-preferred-directions.geojson", routeGeoJson);

            // calculate a direction with a turn penalty of 120 secs and more preferred departure/arrival angles.
            angles = new float?[]
            {
                -90, // leave west.
                -90, // pass in western direction.
                null, // don't-care
                null, // don't-care
                -45 // arrive in north-west direction.
            };
            route = router.Calculate(car, locations, 120, angles);
            routeGeoJson = route.ToGeoJson();
            File.WriteAllText("sequence4-turn-penalty-120-preferred-directions.geojson", routeGeoJson);
        }
    }
}