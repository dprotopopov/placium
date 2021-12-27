using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.Configuration;
using Placium.Common;
using Placium.Route;

namespace Placium.WebApp.Controllers.Update
{
    public class DatabaseRouteUpdateController : UpdateController<DatabaseRouteUpdateService>
    {
        public DatabaseRouteUpdateController(IConfiguration configuration, DatabaseRouteUpdateService updateService,
            IHubContext<ProgressHub, IProgressHubClient> progressHub) : base(configuration,
            updateService, progressHub)
        {
        }

        protected override UpdateFormInfo GetUpdateFormInfo()
        {
            return new UpdateFormInfo
            {
                Title = "Наполнение Route",
                Label = "Добавление новых записей OSM в Route"
            };
        }

        protected override string GetSession()
        {
            return nameof(DatabaseRouteUpdateController);
        }
    }
}