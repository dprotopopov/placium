using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.Configuration;
using Placium.Common;
using Updater.Fias;

namespace Placium.WebApp.Controllers.Update
{
    public class Fias3UpdateController : UpdateController<Fias3UpdateService>
    {
        public Fias3UpdateController(IConfiguration configuration, Fias3UpdateService updateService,
            IHubContext<ProgressHub, IProgressHubClient> progressHub) : base(configuration,
            updateService, progressHub)
        {
        }

        protected override UpdateFormInfo GetUpdateFormInfo()
        {
            return new UpdateFormInfo
            {
                Title = "Наполнение Fias (Visary)",
                Label = "Добавление новых записей в Fias"
            };
        }

        protected override string GetSession()
        {
            return nameof(Fias3UpdateController);
        }
    }
}