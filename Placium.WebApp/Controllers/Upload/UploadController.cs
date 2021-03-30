using System.IO;
using System.Net;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Placium.Common;

namespace Placium.WebApp.Controllers.Upload
{
    public abstract class UploadController<TService> : Controller where TService : IUploadService
    {
        protected readonly IConfiguration Configuration;
        protected readonly TService UploadService;

        public UploadController(IConfiguration configuration, TService uploadService)
        {
            Configuration = configuration;
            UploadService = uploadService;
        }

        public async Task<IActionResult> InstallFromDisk()
        {
            var actionName = ControllerContext.RouteData.Values["action"].ToString();
            var controllerName = ControllerContext.RouteData.Values["controller"].ToString();
            ViewBag.UploadLink = $"/{controllerName}/{actionName}";
            var info = GetInstallFormInfo();
            ViewBag.Title = info.Title;
            ViewBag.Label = info.Label;
            return View("~/Views/_UploadFromDisk.cshtml");
        }

        protected abstract UploadFormInfo GetInstallFormInfo();
        protected abstract UploadFormInfo GetUpdateFormInfo();

        [HttpPost]
        [DisableRequestSizeLimit]
        public async Task<IActionResult> InstallFromDisk(string fileName, string session)
        {
            using (var stream = System.IO.File.OpenRead(fileName))
            {
                await UploadService.InstallAsync(stream, session);
            }

            return Content(fileName);
        }

        public async Task<IActionResult> UpdateFromDisk()
        {
            var actionName = ControllerContext.RouteData.Values["action"].ToString();
            var controllerName = ControllerContext.RouteData.Values["controller"].ToString();
            ViewBag.UploadLink = $"/{controllerName}/{actionName}";
            var info = GetUpdateFormInfo();
            ViewBag.Title = info.Title;
            ViewBag.Label = info.Label;

            return View("~/Views/_UploadFromDisk.cshtml");
        }

        [HttpPost]
        [DisableRequestSizeLimit]
        public async Task<IActionResult> UpdateFromDisk(string fileName, string session)
        {
            using (var stream = System.IO.File.OpenRead(fileName))
            {
                await UploadService.UpdateAsync(stream, session);
            }

            return Content(fileName);
        }

        public async Task<IActionResult> InstallFromWeb()
        {
            var actionName = ControllerContext.RouteData.Values["action"].ToString();
            var controllerName = ControllerContext.RouteData.Values["controller"].ToString();
            ViewBag.UploadLink = $"/{controllerName}/{actionName}";
            var info = GetInstallFormInfo();
            ViewBag.Title = info.Title;
            ViewBag.Label = info.Label;
            return View("~/Views/_UploadFromWeb.cshtml");
        }

        [HttpPost]
        public async Task<IActionResult> InstallFromWeb(string url, string session)
        {
            using (var tempFileStream = new FileStream(Path.GetTempFileName(), FileMode.Create,
                FileAccess.ReadWrite, FileShare.None,
                4096, FileOptions.DeleteOnClose))
            {
                using (var webClient = new WebClient())
                {
                    using (var streamFile = webClient.OpenRead(url))
                    {
                        await streamFile.CopyToAsync(tempFileStream);
                    }
                }

                tempFileStream.Position = 0;

                await UploadService.InstallAsync(tempFileStream, session);
            }

            return Content(url);
        }

        public async Task<IActionResult> UpdateFromWeb()
        {
            var actionName = ControllerContext.RouteData.Values["action"].ToString();
            var controllerName = ControllerContext.RouteData.Values["controller"].ToString();
            ViewBag.UploadLink = $"/{controllerName}/{actionName}";
            var info = GetUpdateFormInfo();
            ViewBag.Title = info.Title;
            ViewBag.Label = info.Label;

            return View("~/Views/_UploadFromWeb.cshtml");
        }

        [HttpPost]
        public async Task<IActionResult> UpdateFromWeb(string url, string session)
        {
            using (var tempFileStream = new FileStream(Path.GetTempFileName(), FileMode.Create,
                FileAccess.ReadWrite, FileShare.None,
                4096, FileOptions.DeleteOnClose))
            {
                using (var webClient = new WebClient())
                {
                    using (var streamFile = webClient.OpenRead(url))
                    {
                        await streamFile.CopyToAsync(tempFileStream);
                    }
                }

                tempFileStream.Position = 0;

                await UploadService.UpdateAsync(tempFileStream, session);
            }

            return Content(url);
        }

        public class UploadFormInfo
        {
            public string Title { get; set; }
            public string Label { get; set; }
        }
    }
}