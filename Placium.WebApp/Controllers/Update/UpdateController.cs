﻿using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Placium.Common;

namespace Placium.WebApp.Controllers.Update
{
    public abstract class UpdateController<TService> : Controller where TService : IUpdateService
    {
        protected readonly IConfiguration Configuration;
        protected readonly TService UpdateService;

        protected UpdateController(IConfiguration configuration, TService updateService)
        {
            Configuration = configuration;
            UpdateService = updateService;
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

            return View("~/Views/_Update.cshtml");
        }

        [HttpPost]
        [DisableRequestSizeLimit]
        public async Task<IActionResult> Update(string session, bool full = false)
        {
            await UpdateService.UpdateAsync(session, full);

            return Content("complete");
        }

        public class UpdateFormInfo
        {
            public string Title { get; set; }
            public string Label { get; set; }
        }
    }
}