﻿using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.Configuration;
using Placium.Common;
using Updater.Fiasx.Sphinx;

namespace Placium.WebApp.Controllers.Update
{
    public class SphinxFiasxUpdateController : UpdateController<SphinxFiasxUpdateService>
    {
        public SphinxFiasxUpdateController(IConfiguration configuration, SphinxFiasxUpdateService updateService,
            IHubContext<ProgressHub, IProgressHubClient> progressHub) : base(configuration,
            updateService, progressHub)
        {
        }

        protected override UpdateFormInfo GetUpdateFormInfo()
        {
            return new UpdateFormInfo
            {
                Title = "Наполнение Sphinx (ФИАС)",
                Label = "Добавление новых записей в Sphinx"
            };
        }

        protected override string GetSession()
        {
            return nameof(SphinxFiasxUpdateController);
        }
    }
}