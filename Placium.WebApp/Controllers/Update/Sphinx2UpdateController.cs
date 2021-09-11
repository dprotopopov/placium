using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.Configuration;
using Placium.Common;
using Updater.Sphinx;

namespace Placium.WebApp.Controllers.Update
{
    public class Sphinx2UpdateController : UpdateController<Sphinx2UpdateService>
    {
        public Sphinx2UpdateController(IConfiguration configuration, Sphinx2UpdateService updateService,
            IHubContext<ProgressHub, IProgressHubClient> progressHub) : base(configuration,
            updateService, progressHub)
        {
        }

        protected override UpdateFormInfo GetUpdateFormInfo()
        {
            return new UpdateFormInfo
            {
                Title = "Наполнение Sphinx (ФИАС-Поиск)",
                Label = "Добавление новых записей в Sphinx"
            };
        }

        protected override string GetSession()
        {
            return nameof(Sphinx2UpdateController);
        }
    }
}