using Microsoft.Extensions.Configuration;
using Updater.Sphinx;

namespace Placium.WebApp.Controllers.Update
{
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
                Title = "Обработка Sphinx",
                Label = "Обработка новых записей Sphinx"
            };
        }
    }
}