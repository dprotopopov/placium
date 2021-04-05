using Microsoft.Extensions.Configuration;
using Updater.Placex;

namespace Placium.WebApp.Controllers.Update
{
    public class PlaceUpdateController : UpdateController<PlacexUpdateService>
    {
        public PlaceUpdateController(IConfiguration configuration, PlacexUpdateService updateService) : base(configuration,
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
    }
}