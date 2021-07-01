using Microsoft.AspNetCore.Authorization;
using Microsoft.Extensions.Configuration;
using Placium.Common;
using Updater.Sphinx;

namespace Placium.WebApp.Controllers.Update
{
    [Authorize]
    public class Sphinx3UpdateController : UpdateController<Sphinx3UpdateService>
    {
        public Sphinx3UpdateController(IConfiguration configuration, Sphinx3UpdateService updateService, ProgressHub progressHub) : base(configuration,
            updateService, progressHub)
        {
        }

        protected override UpdateFormInfo GetUpdateFormInfo()
        {
            return new UpdateFormInfo
            {
                Title = "Наполнение Sphinx (Visary)",
                Label = "Добавление новых записей в Sphinx"
            };
        }
        protected override string GetSession() => nameof(Sphinx3UpdateController);

    }
}