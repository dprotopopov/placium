using Loader.Gar.File;
using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Options;
using Placium.Common;

namespace Placium.WebApp.Controllers.Upload
{
    public class FileGarUploadController : UploadController<FileGarUploadService>
    {
        public FileGarUploadController(IConfiguration configuration, FileGarUploadService uploadService,
            IOptions<UploadConfig> uploadConfig, IHubContext<ProgressHub, IProgressHubClient> progressHub) : base(
            configuration, uploadService,
            uploadConfig, progressHub)
        {
        }

        protected override UploadFormInfo GetInstallFormInfo()
        {
            return new UploadFormInfo
            {
                Title = "Загрузка полной базы ГАР",
                Label = "Полная БД ГАР (gar_xml.zip)"
            };
        }

        protected override UploadFormInfo GetUpdateFormInfo()
        {
            return new UploadFormInfo
            {
                Title = "Загрузка обновления базы ГАР",
                Label = "Обновление БД ГАР (gar_delta_xml.zip)"
            };
        }

        protected override string GetSession()
        {
            return nameof(FileGarUploadController);
        }
    }

}