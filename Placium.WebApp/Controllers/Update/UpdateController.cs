using System;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.Configuration;
using Placium.Common;

namespace Placium.WebApp.Controllers.Update
{
    public abstract class UpdateController<TService> : Controller where TService : IUpdateService
    {
        protected readonly IConfiguration Configuration;
        protected readonly IHubContext<ProgressHub, IProgressHubClient> ProgressHub;
        protected readonly TService UpdateService;

        protected UpdateController(IConfiguration configuration, TService updateService,
            IHubContext<ProgressHub, IProgressHubClient> progressHub)
        {
            Configuration = configuration;
            UpdateService = updateService;
            ProgressHub = progressHub;
        }

        protected abstract UpdateFormInfo GetUpdateFormInfo();
        protected abstract string GetSession();

        public async Task<IActionResult> Update()
        {
            var actionName = ControllerContext.RouteData.Values["action"].ToString();
            var controllerName = ControllerContext.RouteData.Values["controller"].ToString();
            ViewBag.UploadLink = $"/{controllerName}/{actionName}";
            var info = GetUpdateFormInfo();
            ViewBag.Title = info.Title;
            ViewBag.Label = info.Label;
            ViewBag.Session = GetSession();

            return await Task.FromResult(View("~/Views/_Update.cshtml"));
        }

        [HttpPost]
        [DisableRequestSizeLimit]
        public async Task<IActionResult> Update(string session, bool full = false)
        {
            try
            {
                await UpdateService.UpdateAsync(session, full);
                await ProgressHub.Clients.All.Complete(session);

                return Content("complete");
            }
            catch (Exception ex)
            {
                await ProgressHub.Clients.All.Error(ex.Message, session);
                throw;
            }
        }

        public class UpdateFormInfo
        {
            public string Title { get; set; }
            public string Label { get; set; }
        }
    }
}