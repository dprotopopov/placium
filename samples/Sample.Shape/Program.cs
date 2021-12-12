using System;
using System.IO;
using Itinero;
using Itinero.Data.Edges;
using Itinero.LocalGeo;
using Itinero.Logging;
using Itinero.Profiles;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Placium.Common;
using Placium.IO.Osm;

namespace Sample.Shape
{
    public class Program
    {
        public static void Main(string[] args)
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

            Logger.LogAction = (o, level, message, parameters) =>
            {
                Console.WriteLine("[{0}] {1} - {2}", o, level, message);
            };

            // create a new router db and load the shapefile.
            var vehicle = DynamicVehicle.LoadFromStream(File.OpenRead(
                Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "car.lua")));
            var routerDb = new RouterDb(EdgeDataSerializer.MAX_DISTANCE);
            routerDb.LoadOsmDataFromPostgreSQL(connectionsConfig.GetConnectionString("OsmConnection"), new Box(),
                vehicle);

            // OPTIONAL: build a contracted version of the routing graph.
            // routerDb.AddContracted(vehicle.Fastest());

            // write the router db to disk for later use.
            routerDb.Serialize(File.OpenWrite(Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "nwb.routerdb")));

            // calculate a test route.
            var router = new Router(routerDb);
            var route = router.Calculate(vehicle.Fastest(), new Coordinate(52.954718f, 6.338811f),
                new Coordinate(52.95359f, 6.337916f));
            route = router.Calculate(vehicle.Fastest(), new Coordinate(51.57060821506861f, 5.46792984008789f),
                new Coordinate(51.58711643524425f, 5.4957228899002075f));

            // generate instructions based on lua profile.
            var instructions = route.GenerateInstructions(routerDb);
        }
    }
}