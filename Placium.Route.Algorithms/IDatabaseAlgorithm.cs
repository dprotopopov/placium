using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Placium.Route.Algorithms
{
    public interface IDatabaseAlgorithm
    {
        Guid Guid { get; }
        string ConnectionString { get; }

    }
}
