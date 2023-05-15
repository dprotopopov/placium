using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.Configuration;
using Placium.Common;
using Updater.Garx.Sphinx;

namespace Placium.WebApp.Controllers.Update
{
    public class SphinxGarxUpdateController : UpdateController<SphinxGarxUpdateService>
    {
        public SphinxGarxUpdateController(IConfiguration configuration, SphinxGarxUpdateService updateService,
            IHubContext<ProgressHub, IProgressHubClient> progressHub) : base(configuration,
            updateService, progressHub)
        {
        }

        protected override UpdateFormInfo GetUpdateFormInfo()
        {
            return new UpdateFormInfo
            {
                Title = "Наполнение Sphinx (ГАР)",
                Label = "Добавление новых записей в Sphinx"
            };
        }

        protected override string GetSession()
        {
            return nameof(SphinxGarxUpdateController);
        }
    }
}