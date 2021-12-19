using System;
using System.IO;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using OsmSharp.Logging;
using Placium.Common;
using Route;
using Route.Attributes;
using Route.IO.Osm;
using Route.LocalGeo;
using Route.Profiles;
using Attribute = Route.Attributes.Attribute;

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
            Route.Logging.Logger.LogAction = (o, level, message, parameters) =>
            {
                Console.WriteLine($"[{o}] {level} - {message}");
            };

            // load some routing data and create a router.
            var routerDb = new RouterDb(connectionsConfig.GetConnectionString("RouteConnection"), connectionsConfig.GetConnectionString("OsmConnection"));

            var customCar = DynamicVehicle.Load(File.ReadAllText("custom-car.lua"));

            routerDb.LoadOsmDataFromPlacium(connectionsConfig.GetConnectionString("OsmConnection"), customCar);

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
            var location1 = new Coordinate(55.821861f, 37.626996f);
            var location2 = new Coordinate(55.820427f, 37.639986f);
            var router = new Router(routerDb);

            // calculate route before.
            var routeBefore = router.Calculate(customCar.Fastest(), location1, location2);
            var routeBeforeGeoJson = routeBefore.ToGeoJson();

            // resolve an edge.
            var edgeLocation = new Coordinate(55.750117f, 37.658210f);
            var resolved = router.Resolve(customCar.Fastest(), edgeLocation);

            // update the speed profile of this edge.
            var edgeData = routerDb.Network.GetEdge(resolved.EdgeId).Data;
            edgeData.Profile = (ushort) speed10;
            routerDb.Network.UpdateEdgeData(resolved.EdgeId, edgeData);

            // calculate route.
            var routeAfter = router.Calculate(customCar.Fastest(), location1, location2);
            var routeAfterGeoJson = routeAfter.ToGeoJson();

            // calculate route to middle of edge.
            var location3 = new Coordinate(55.732686f, 37.639842f);
            var routeAfter13 = router.Calculate(customCar.Fastest(), location1, location3);
            var routeAfter13GeoJson = routeAfter13.ToGeoJson();
        }
    }
}