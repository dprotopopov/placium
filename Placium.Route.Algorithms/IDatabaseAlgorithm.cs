using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Placium.Route.Algorithms
{
    public interface IDatabaseAlgorithm<T>
    {
        Guid Guid { get; }
        string ConnectionString { get; }

        Task<T> DoRunAsync();
    }
}
