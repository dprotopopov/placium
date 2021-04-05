using Microsoft.Extensions.Configuration;
using Updater.Addr;

namespace Placium.WebApp.Controllers.Update
{
    public class AddrUpdateController : UpdateController<AddrUpdateService>
    {
        public AddrUpdateController(IConfiguration configuration, AddrUpdateService updateService) : base(configuration,
            updateService)
        {
        }

        protected override UpdateFormInfo GetUpdateFormInfo()
        {
            return new UpdateFormInfo
            {
                Title = "Наполнение Addr",
                Label = "Добавление записей Placex в Addr"
            };
        }
    }
}