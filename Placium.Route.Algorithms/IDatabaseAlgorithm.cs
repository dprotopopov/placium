using System;

namespace Placium.Route.Algorithms
{
    public interface IDatabaseAlgorithm
    {
        Guid Guid { get; }
        string ConnectionString { get; }
    }
}