using System;
using System.IO;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using OsmSharp.Logging;
using Placium.Common;
using Placium.IO.Osm;
using Route;
using Route.Elevation;
using Route.LocalGeo;
using Route.LocalGeo.Elevation;
using Route.Osm.Vehicles;
using SRTM;

namespace Sample.Elevation
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
            var router = new Router(routerDb);

            routerDb.LoadOsmDataFromPostgreSQL(connectionsConfig.GetConnectionString("OsmConnection"), Vehicle.Car);

            // create a new srtm data instance.
            // it accepts a folder to download and cache data into.
            var srtmCache = new DirectoryInfo("srtm-cache");
            if (!srtmCache.Exists) srtmCache.Create();
            var srtmData = new SRTMData("srtm-cache");
            ElevationHandler.GetElevation = (lat, lon) => { return (short) srtmData.GetElevation(lat, lon); };

            // add elevation.
            routerDb.AddElevation();

            // calculate route.
            // this should be the result: http://geojson.io/#id=gist:anonymous/c944cb9741f1fd511c8213b2dd83d58d&map=17/49.75454/6.09571
            var route = router.Calculate(Vehicle.Car.Fastest(), new Coordinate(49.75635954613685f, 6.095362901687622f),
                new Coordinate(49.75263039062888f, 6.098860502243042f));
            var routeGeoJson = route.ToGeoJson();
        }
    }
}