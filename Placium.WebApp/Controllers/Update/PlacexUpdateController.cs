using Microsoft.AspNetCore.Authorization;
using Microsoft.Extensions.Configuration;
using Updater.Placex;

namespace Placium.WebApp.Controllers.Update
{
    [Authorize]
    public class PlacexUpdateController : UpdateController<PlacexUpdateService>
    {
        public PlacexUpdateController(IConfiguration configuration, PlacexUpdateService updateService) : base(configuration,
            updateService)
        {
        }

        protected override UpdateFormInfo GetUpdateFormInfo()
        {
            return new UpdateFormInfo
            {
                Title = "Наполнение Placex",
                Label = "Добавление новых записей OSM в Placex"
            };
        }
        protected override string GetSession() => nameof(PlacexUpdateController);
    }
}