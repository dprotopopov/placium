using Microsoft.Extensions.Configuration;
using Updater.Place;

namespace Placium.WebApp.Controllers.Update
{
    public class PlaceUpdateController : UpdateController<PlaceUpdateService>
    {
        public PlaceUpdateController(IConfiguration configuration, PlaceUpdateService updateService) : base(configuration,
            updateService)
        {
        }

        protected override UpdateFormInfo GetUpdateFormInfo()
        {
            return new UpdateFormInfo
            {
                Title = "Наполнение Place",
                Label = "Добавление новых записей OSM в Place"
            };
        }
    }
}