using Microsoft.AspNetCore.Authorization;
using Microsoft.Extensions.Configuration;
using Placium.Common;
using Updater.Addrx;

namespace Placium.WebApp.Controllers.Update
{
    [Authorize]
    public class AddrxUpdateController : UpdateController<AddrxUpdateService>
    {
        public AddrxUpdateController(IConfiguration configuration, AddrxUpdateService updateService, ProgressHub progressHub) : base(configuration,
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

        protected override string GetSession() => nameof(AddrxUpdateController);
    }
}