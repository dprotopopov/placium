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

        protected override string GetConnectionString()
        {
            return Configuration.GetConnectionString("FiasConnection");
        }

        protected override UploadFormInfo GetInstallFormInfo()
        {
            return new UploadFormInfo
            {
                Title = "Установка ФИАС",
                Label = "Полная БД ФИАС (zip-архив dbf-файлов)"
            };
        }

        protected override UploadFormInfo GetUpdateFormInfo()
        {
            return new UploadFormInfo
            {
                Title = "Установка ФИАС",
                Label = "Обновление БД ФИАС (zip-архив dbf-файлов)"
            };
        }
    }
}