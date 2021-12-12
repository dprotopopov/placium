using System;
using System.IO;
using Itinero;
using Itinero.Attributes;
using Itinero.LocalGeo;
using Itinero.Profiles;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using OsmSharp.Logging;
using Placium.Common;
using Placium.IO.Osm;
using Attribute = Itinero.Attributes.Attribute;

namespace Sample.DynamicProfiles
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
            Logger.LogAction = (o, level, message, parameters) => { Console.WriteLine($"[{o}] {level} - {message}"); };
            Itinero.Logging.Logger.LogAction = (o, level, message, parameters) =>
            {
                Console.WriteLine($"[{o}] {level} - {message}");
            };

            // load some routing data and create a router.
            var routerDb = new RouterDb();

            var customCar = DynamicVehicle.Load(File.ReadAllText("custom-car.lua"));

            routerDb.LoadOsmDataFromPostgreSQL(connectionsConfig.GetConnectionString("OsmConnection"), new Box(),
                customCar);

            // add custom profiles.
            var speed10 = routerDb.EdgeProfiles.Add(new AttributeCollection(
                new Attribute("highway", "residential"),
                new Attribute("custom-speed", "10")));
            var speed20 = routerDb.EdgeProfiles.Add(new AttributeCollection(
                new Attribute("highway", "residential"),
                new Attribute("custom-speed", "20")));
            var speed30 = routerDb.EdgeProfiles.Add(new AttributeCollection(
                new Attribute("highway", "residential"),
                new Attribute("custom-speed", "30")));
            var speed40 = routerDb.EdgeProfiles.Add(new AttributeCollection(
                new Attribute("highway", "residential"),
                new Attribute("custom-speed", "40")));

            // define locations, profile and router.
            var location1 = new Coordinate(49.88826851632804f, 5.815232992172241f);
            var location2 = new Coordinate(49.88775699771737f, 5.8133286237716675f);
            var router = new Router(routerDb);

            // calculate route before.
            var routeBefore = router.Calculate(customCar.Fastest(), location1, location2);
            var routeBeforeGeoJson = routeBefore.ToGeoJson();

            // resolve an edge.
            var edgeLocation = new Coordinate(49.888040407347006f, 5.8142513036727905f);
            var resolved = router.Resolve(customCar.Fastest(), edgeLocation);

            // update the speed profile of this edge.
            var edgeData = routerDb.Network.GetEdge(resolved.EdgeId).Data;
            edgeData.Profile = (ushort) speed10;
            routerDb.Network.UpdateEdgeData(resolved.EdgeId, edgeData);

            // calculate route.
            var routeAfter = router.Calculate(customCar.Fastest(), location1, location2);
            var routeAfterGeoJson = routeAfter.ToGeoJson();

            // calculate route to middle of edge.
            var location3 = new Coordinate(49.888035223039466f, 5.814205706119537f);
            var routeAfter13 = router.Calculate(customCar.Fastest(), location1, location3);
            var routeAfter13GeoJson = routeAfter13.ToGeoJson();
        }
    }
}