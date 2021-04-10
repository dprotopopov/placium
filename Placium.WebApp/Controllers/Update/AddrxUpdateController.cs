using Microsoft.AspNetCore.Authorization;
using Microsoft.Extensions.Configuration;
using Updater.Addrx;

namespace Placium.WebApp.Controllers.Update
{
    [Authorize]
    public class AddrxUpdateController : UpdateController<AddrxUpdateService>
    {
        public AddrxUpdateController(IConfiguration configuration, AddrxUpdateService updateService) : base(configuration,
            updateService)
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
    }
}