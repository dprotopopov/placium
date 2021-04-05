using Loader.Fias;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Options;

namespace Placium.WebApp.Controllers.Upload
{
    public class FiasUploadController : UploadController<FiasUploadService>
    {
        public FiasUploadController(IConfiguration configuration, FiasUploadService uploadService,
            IOptions<UploadConfig> uploadConfig) : base(configuration, uploadService, uploadConfig)
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
    }
}