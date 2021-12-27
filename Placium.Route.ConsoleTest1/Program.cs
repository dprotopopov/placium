using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using OsmSharp.Logging;
using Placium.Common;
using Placium.Route.Osm.Vehicles;
using Route.LocalGeo;

namespace Placium.Route.ConsoleTest1
{
    internal class Program
    {
        private static async Task Main(string[] args)
        {
            var builder = new ConfigurationBuilder();
            builder.AddCommandLine(args);

            var config = builder.Build();

            var serviceProvider = new ServiceCollection()
                .AddLogging()
                .AddSingleton<IConfiguration>(config)
                .AddSingleton<IConnectionsConfig, ArgsConnectionsConfig>()
                .AddSingleton<IProgressClient, ShellProgressClient>()
                .BuildServiceProvider();

            var connectionsConfig = serviceProvider.GetService<IConnectionsConfig>();
            var progressClient = serviceProvider.GetService<IProgressClient>();

            // enable logging.
            Logger.LogAction = (o, level, message, parameters) =>
            {
                Console.WriteLine("[{0}] {1} - {2}", o, level, message);
            };

            var routerDb = new RouterDb(Guid.Parse("28662f4a-3d30-464e-9b64-c5e25457b2f1"), connectionsConfig.GetConnectionString("RouteConnection"),
                new[] { Vehicle.Car });
                //await routerDb.LoadFromOsmAsync(connectionsConfig.GetConnectionString("OsmConnection"), progressClient);

            // create router.
            var router = new Router(routerDb);

            // calculate route.
            // this should be the result: http://geojson.io/#id=gist:dprotopopov/34df4ce18b6e974bb2ee9123b29d46c4&map=16/55.8223/37.6331
            var route = await router.CalculateAsync(new Coordinate(55.823680f, 37.608577f),
                new Coordinate(55.820427f, 37.639986f), "car.shortest");
            var routeGeoJson = route.ToGeoJson();
            File.WriteAllText("route1.geojson", routeGeoJson);
        }
    }
}