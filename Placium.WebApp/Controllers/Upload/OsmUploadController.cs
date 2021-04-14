using Loader.Osm;
using Microsoft.AspNetCore.Authorization;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Options;
using Placium.Common;

namespace Placium.WebApp.Controllers.Upload
{
    [Authorize]
    public class OsmUploadController : UploadController<OsmUploadService>
    {
        public OsmUploadController(IConfiguration configuration, OsmUploadService uploadService,
            IOptions<UploadConfig> uploadConfig, ProgressHub progressHub) : base(configuration, uploadService, uploadConfig, progressHub)
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