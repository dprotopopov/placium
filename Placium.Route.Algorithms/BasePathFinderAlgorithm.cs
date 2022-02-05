using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Placium.Route.Common;

namespace Placium.Route.Algorithms;

public abstract class BasePathFinderAlgorithm : BaseDatabaseAlgorithm
{
    protected BasePathFinderAlgorithm(Guid guid, string connectionString, string vehicleType, string profile,
        float minFactor, float maxFactor) : base(guid, connectionString, profile)
    {
        VehicleType = vehicleType;
        MinFactor = minFactor;
        MaxFactor = maxFactor;
    }

    public string VehicleType { get; }
    public float MinFactor { get; }
    public float MaxFactor { get; }

    public abstract Task<PathFinderResult> FindPathAsync(RouterPoint source,
        RouterPoint target, float maxWeight = float.MaxValue);
}

public class PathFinderResult
{
    public List<long> Edges { get; set; }
    public float? Weight { get; set; }
}