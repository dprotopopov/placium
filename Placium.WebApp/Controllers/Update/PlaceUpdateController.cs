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

        protected override string GetConnectionString()
        {
            return Configuration.GetConnectionString("OsmConnection");
        }

        protected override UpdateFormInfo GetUpdateFormInfo()
        {
            return new UpdateFormInfo
            {
                Title = "Обработка OSM",
                Label = "Обработка новых записей OSM"
            };
        }
    }
}