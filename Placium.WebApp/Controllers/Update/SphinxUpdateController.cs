using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.Configuration;
using Placium.Common;
using Updater.Sphinx;

namespace Placium.WebApp.Controllers.Update
{
    [Authorize]
    public class SphinxUpdateController : UpdateController<SphinxUpdateService>
    {
        public SphinxUpdateController(IConfiguration configuration, SphinxUpdateService updateService, IHubContext<ProgressHub, IProgressHubClient> progressHub) : base(configuration,
            updateService, progressHub)
        {
        }

        protected override UpdateFormInfo GetUpdateFormInfo()
        {
            return new UpdateFormInfo
            {
                Title = "Наполнение Sphinx (OSM)",
                Label = "Добавление новых записей в Sphinx"
            };
        }
        protected override string GetSession() => nameof(SphinxUpdateController);

    }
}
