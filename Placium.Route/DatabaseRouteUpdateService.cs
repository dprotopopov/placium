using System;
using System.Threading.Tasks;
using Placium.Common;
using Placium.Route.Osm.Vehicles;

namespace Placium.Route
{
    public class DatabaseRouteUpdateService : BaseAppService, IUpdateService
    {
        private readonly IProgressClient _progressClient;

        public DatabaseRouteUpdateService(IConnectionsConfig configuration, IProgressClient progressClient) : base(
            configuration)
        {
            _progressClient = progressClient;
        }

        public async Task UpdateAsync(string session, bool full)
        {
            var routerDb = new RouterDb(Guid.Parse("28662f4a-3d30-464e-9b64-c5e25457b2f1"), GetRouteConnectionString(),
                new[] {Vehicle.Car});
            await routerDb.LoadFromOsmAsync(GetOsmConnectionString(), _progressClient, session);
        }
    }
}