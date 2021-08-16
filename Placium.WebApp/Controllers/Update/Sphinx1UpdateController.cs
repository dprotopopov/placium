using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.Configuration;
using Placium.Common;
using Updater.Sphinx;

namespace Placium.WebApp.Controllers.Update
{
    [Authorize]
    public class Sphinx1UpdateController : UpdateController<Sphinx1UpdateService>
    {
        public Sphinx1UpdateController(IConfiguration configuration, Sphinx1UpdateService updateService, IHubContext<ProgressHub, IProgressHubClient> progressHub) : base(configuration,
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
        protected override string GetSession() => nameof(Sphinx1UpdateController);

    }
}
