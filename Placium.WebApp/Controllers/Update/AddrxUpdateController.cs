using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.Configuration;
using Placium.Common;
using Updater.Addrx;

namespace Placium.WebApp.Controllers.Update
{
    public class AddrxUpdateController : UpdateController<AddrxUpdateService>
    {
        public AddrxUpdateController(IConfiguration configuration, AddrxUpdateService updateService,
            IHubContext<ProgressHub, IProgressHubClient> progressHub) : base(configuration,
            updateService, progressHub)
        {
        }

        protected override UpdateFormInfo GetUpdateFormInfo()
        {
            return new UpdateFormInfo
            {
                Title = "Наполнение Addrx",
                Label = "Добавление записей Placex в Addrx"
            };
        }

        protected override string GetSession()
        {
            return nameof(AddrxUpdateController);
        }
    }
}