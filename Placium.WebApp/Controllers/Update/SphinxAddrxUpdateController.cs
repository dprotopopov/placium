using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.Configuration;
using Placium.Common;
using Updater.Addrx.Sphinx;

namespace Placium.WebApp.Controllers.Update
{
    public class SphinxAddrxUpdateController : UpdateController<SphinxAddrxUpdateService>
    {
        public SphinxAddrxUpdateController(IConfiguration configuration, SphinxAddrxUpdateService updateService,
            IHubContext<ProgressHub, IProgressHubClient> progressHub) : base(configuration,
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

        protected override string GetSession()
        {
            return nameof(SphinxAddrxUpdateController);
        }
    }
}