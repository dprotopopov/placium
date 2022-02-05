using System;
using System.Threading.Tasks;
using Placium.Route.Common;
using Route.LocalGeo;

namespace Placium.Route.Algorithms;

public abstract class BaseResolveRouterPointAlgorithm : BaseDatabaseAlgorithm
{
    public BaseResolveRouterPointAlgorithm(Guid guid, string connectionString, string profile) : base(guid,
        connectionString,
        profile)
    {
    }

    public abstract Task<RouterPoint> ResolveRouterPointAsync(Coordinate coordinate);
}