using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.Configuration;
using Placium.Common;
using Updater.Fias;

namespace Placium.WebApp.Controllers.Update
{
    public class FiasUpdateController : UpdateController<FiasUpdateService>
    {
        public FiasUpdateController(IConfiguration configuration, FiasUpdateService updateService,
            IHubContext<ProgressHub, IProgressHubClient> progressHub) : base(configuration,
            updateService, progressHub)
        {
        }

        protected override UpdateFormInfo GetUpdateFormInfo()
        {
            return new UpdateFormInfo
            {
                Title = "Наполнение Fias (OSM)",
                Label = "Добавление новых записей в Fias"
            };
        }

        protected override string GetSession()
        {
            return nameof(FiasUpdateController);
        }
    }
}