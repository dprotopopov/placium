using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.Configuration;
using Placium.Common;
using Updater.Placex.Database;

namespace Placium.WebApp.Controllers.Update
{
    public class DatabasePlacexUpdateController : UpdateController<DatabasePlacexUpdateService>
    {
        public DatabasePlacexUpdateController(IConfiguration configuration, DatabasePlacexUpdateService updateService,
            IHubContext<ProgressHub, IProgressHubClient> progressHub) : base(configuration,
            updateService, progressHub)
        {
        }

        protected override UpdateFormInfo GetUpdateFormInfo()
        {
            return new UpdateFormInfo
            {
                Title = "Наполнение Placex",
                Label = "Добавление новых записей OSM в Placex"
            };
        }

        protected override string GetSession()
        {
            return nameof(DatabasePlacexUpdateController);
        }
    }
}