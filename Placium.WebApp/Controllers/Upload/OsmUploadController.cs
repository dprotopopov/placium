using Loader.Osm;
using Microsoft.AspNetCore.Authorization;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Options;

namespace Placium.WebApp.Controllers.Upload
{
    [Authorize]
    public class OsmUploadController : UploadController<OsmUploadService>
    {
        public OsmUploadController(IConfiguration configuration, OsmUploadService uploadService,
            IOptions<UploadConfig> uploadConfig) : base(configuration, uploadService, uploadConfig)
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
        protected override string GetSession() => nameof(OsmUploadController);
    }
}