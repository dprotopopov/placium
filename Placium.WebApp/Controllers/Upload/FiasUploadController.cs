using Loader.Fias;
using Microsoft.Extensions.Configuration;

namespace Placium.WebApp.Controllers.Upload
{
    public class FiasUploadController : UploadController<FiasUploadService>
    {
        public FiasUploadController(IConfiguration configuration, FiasUploadService uploadService) : base(configuration,
            uploadService)
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