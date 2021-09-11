using Loader.Fias;
using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Options;
using Placium.Common;

namespace Placium.WebApp.Controllers.Upload
{
    public class FiasUploadController : UploadController<FiasUploadService>
    {
        public FiasUploadController(IConfiguration configuration, FiasUploadService uploadService,
            IOptions<UploadConfig> uploadConfig, IHubContext<ProgressHub, IProgressHubClient> progressHub) : base(
            configuration, uploadService,
            uploadConfig, progressHub)
        {
        }

        protected override UploadFormInfo GetInstallFormInfo()
        {
            return new UploadFormInfo
            {
                Title = "Загрузка полной базы ФИАС",
                Label = "Полная БД ФИАС (fias_dbf.zip)"
            };
        }

        protected override UploadFormInfo GetUpdateFormInfo()
        {
            return new UploadFormInfo
            {
                Title = "Загрузка обновления базы ФИАС",
                Label = "Обновление БД ФИАС (fias_delta_dbf.zip)"
            };
        }

        protected override string GetSession()
        {
            return nameof(FiasUploadController);
        }
    }
}