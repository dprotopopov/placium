using Microsoft.AspNetCore.Authorization;
using Microsoft.Extensions.Configuration;
using Updater.Sphinx;

namespace Placium.WebApp.Controllers.Update
{
    [Authorize]
    public class SphinxUpdateController : UpdateController<SphinxUpdateService>
    {
        public SphinxUpdateController(IConfiguration configuration, SphinxUpdateService updateService) : base(configuration,
            updateService)
        {
        }

        protected override UpdateFormInfo GetUpdateFormInfo()
        {
            return new UpdateFormInfo
            {
                Title = "Наполнение Sphinx",
                Label = "Добавление новых записей в Sphinx"
            };
        }
    }
}