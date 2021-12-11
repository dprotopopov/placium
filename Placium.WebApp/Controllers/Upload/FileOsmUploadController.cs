using Loader.Osm.File;
using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Options;
using Placium.Common;

namespace Placium.WebApp.Controllers.Upload
{
    public class FileOsmUploadController : UploadController<FileOsmUploadService>
    {
        public FileOsmUploadController(IConfiguration configuration, FileOsmUploadService uploadService,
            IOptions<UploadConfig> uploadConfig, IHubContext<ProgressHub, IProgressHubClient> progressHub) : base(
            configuration, uploadService,
            uploadConfig, progressHub)
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

        protected override string GetSession()
        {
            return nameof(FileOsmUploadController);
        }
    }
}