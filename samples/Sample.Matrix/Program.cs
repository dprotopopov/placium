using System.Collections.Generic;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Placium.Common;
using Placium.IO.Osm;
using Route;
using Route.Algorithms.Matrices;
using Route.IO.Osm;
using Route.LocalGeo;
using Route.Osm.Vehicles;

namespace Sample.Matrix
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

            var routerDb = new RouterDb(connectionsConfig.GetConnectionString("RouteConnection"), connectionsConfig.GetConnectionString("OsmConnection"));
            var router = new Router(routerDb);
            routerDb.LoadOsmDataFromPostgreSQL(connectionsConfig.GetConnectionString("OsmConnection"), Vehicle.Car);

            var locations = new List<Coordinate>(new[]
            {
                new Coordinate(51.270453873703080f, 4.8008108139038080f),
                new Coordinate(51.264197451065370f, 4.8017120361328125f),
                new Coordinate(51.267446600889850f, 4.7830009460449220f),
                new Coordinate(51.260733228426076f, 4.7796106338500980f),
                new Coordinate(51.256489871317920f, 4.7884941101074220f),
                new Coordinate(51.270964016530680f, 4.7894811630249020f)
            });

            // METHOD1: quick and easy for high-quality data already on the road network.
            // calculate drive time in seconds between all given locations.
            var resolved = router.Resolve(Vehicle.Car.Fastest(), locations.ToArray(), 150);
            var invalidPoints = new HashSet<int>();
            var matrix = router.CalculateWeight(Vehicle.Car.Fastest(), resolved, invalidPoints);

            // METHOD2: most datasets contain large numbers of unconfirmed locations that may be too far from the road network or contain errors.
            //          this method can handle coordinates sets that contains errors.

            // let's add a location that's in the middle of nowhere.
            var invalidLocation = new Coordinate(51.275689280878694f, 4.7779369354248040f);
            locations.Insert(3, invalidLocation);

            // for advanced applications there is a helper class
            var matrixCalculator = new WeightMatrixAlgorithm(router, Vehicle.Car.Fastest(), locations.ToArray());
            matrixCalculator.Run();

            // there is some usefull output data here now.
            var weights = matrixCalculator.Weights; // the weights, in this case seconds travel time.
            var errors =
                matrixCalculator
                    .Errors; // some locations could be unreachable, this contains details about those locations.
            resolved = matrixCalculator.RouterPoints
                .ToArray(); // the resolved routerpoints, you can use these later without the need to resolve again.

            // when there are failed points, the weight matrix is smaller, use these functions to map locations from the original array to succeeded points.
            var newIndex =
                matrixCalculator.MassResolver
                    .ResolvedIndexOf(4); // returns the index of the original location in the weight matrix.
            var oldIndex =
                matrixCalculator.MassResolver
                    .LocationIndexOf(
                        5); // returns the index of the weight matrix point in the original locations array.
        }
    }
}