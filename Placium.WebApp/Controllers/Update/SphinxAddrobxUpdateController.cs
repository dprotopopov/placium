using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.Configuration;
using Placium.Common;
using Updater.Addrobx.Sphinx;

namespace Placium.WebApp.Controllers.Update
{
    public class SphinxAddrobxUpdateController : UpdateController<SphinxAddrobxUpdateService>
    {
        public SphinxAddrobxUpdateController(IConfiguration configuration, SphinxAddrobxUpdateService updateService,
            IHubContext<ProgressHub, IProgressHubClient> progressHub) : base(configuration,
            updateService, progressHub)
        {
        }

        protected override UpdateFormInfo GetUpdateFormInfo()
        {
            return new UpdateFormInfo
            {
                Title = "Наполнение Sphinx (ФИАС)",
                Label = "Добавление новых записей в Sphinx"
            };
        }

        protected override string GetSession()
        {
            return nameof(SphinxAddrobxUpdateController);
        }
    }
}