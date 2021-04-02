using Loader.Osm;
using Microsoft.Extensions.Configuration;

namespace Placium.WebApp.Controllers.Upload
{
    public class OsmUploadController : UploadController<OsmUploadService>
    {
        public OsmUploadController(IConfiguration configuration, OsmUploadService uploadService) : base(configuration,
            uploadService)
        {
        }

        protected override UploadFormInfo GetInstallFormInfo()
        {
            return new UploadFormInfo
            {
                Title = "Загрузка полной базы OSM",
                Label = "База данных OSM"
            };
        }

        protected override UploadFormInfo GetUpdateFormInfo()
        {
            return new UploadFormInfo
            {
                Title = "Загрузка добавления базы OSM",
                Label = "База данных OSM"
            };
        }
    }
}