using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Options;
using Placium.Common;

namespace Placium.WebApp.Controllers.Upload
{
    public abstract class UploadController<TService> : Controller where TService : IUploadService
    {
        protected readonly ProgressHub ProgressHub;
        protected readonly IConfiguration Configuration;
        protected readonly UploadConfig UploadConfig;
        protected readonly TService UploadService;

        public UploadController(IConfiguration configuration, TService uploadService,
            IOptions<UploadConfig> uploadConfig, ProgressHub progressHub)
        {
            Configuration = configuration;
            UploadService = uploadService;
            ProgressHub = progressHub;
            UploadConfig = uploadConfig.Value;
        }

        public async Task<IActionResult> InstallFromDisk()
        {
            var actionName = ControllerContext.RouteData.Values["action"].ToString();
            var controllerName = ControllerContext.RouteData.Values["controller"].ToString();
            ViewBag.UploadLink = $"/{controllerName}/{actionName}";
            var info = GetInstallFormInfo();
            ViewBag.Title = info.Title;
            ViewBag.Label = info.Label;
            ViewBag.Session = GetSession();
            ViewBag.Files = Directory.GetFiles(UploadConfig.Path).Select(x => Path.GetFileName(x)).ToArray();
            return View("~/Views/_UploadFromDisk.cshtml");
        }

        protected abstract UploadFormInfo GetInstallFormInfo();
        protected abstract UploadFormInfo GetUpdateFormInfo();
        protected abstract string GetSession();

        [HttpPost]
        [DisableRequestSizeLimit]
        public async Task<IActionResult> InstallFromDisk(string fileName, string region, string session)
        {
            try
            {
                using (var stream =
                    System.IO.File.OpenRead(Path.Combine(UploadConfig.Path, Path.GetFileName(fileName))))
                {
                    await UploadService.InstallAsync(stream, new Dictionary<string, string>
                    {
                        {"region", region}
                    }, session);
                }

                await ProgressHub.CompleteAsync(session);

                return Content("complete");
            }
            catch (Exception ex)
            {
                await ProgressHub.ErrorAsync(ex.Message, session);
                throw;
            }
        }

        public async Task<IActionResult> UpdateFromDisk()
        {
            var actionName = ControllerContext.RouteData.Values["action"].ToString();
            var controllerName = ControllerContext.RouteData.Values["controller"].ToString();
            ViewBag.UploadLink = $"/{controllerName}/{actionName}";
            var info = GetUpdateFormInfo();
            ViewBag.Title = info.Title;
            ViewBag.Label = info.Label;
            ViewBag.Session = GetSession();
            ViewBag.Files = Directory.GetFiles(UploadConfig.Path).Select(x => Path.GetFileName(x)).ToArray();
            return View("~/Views/_UploadFromDisk.cshtml");
        }

        [HttpPost]
        [DisableRequestSizeLimit]
        public async Task<IActionResult> UpdateFromDisk(string fileName, string region, string session)
        {
            try
            {
                using (var stream =
                    System.IO.File.OpenRead(Path.Combine(UploadConfig.Path, Path.GetFileName(fileName))))
                {
                    await UploadService.UpdateAsync(stream, new Dictionary<string, string>
                    {
                        {"region", region}
                    }, session);
                }

                await ProgressHub.CompleteAsync(session);

                return Content("complete");
            }
            catch (Exception ex)
            {
                await ProgressHub.ErrorAsync(ex.Message, session);
                throw;
            }
        }

        public async Task<IActionResult> InstallFromWeb()
        {
            var actionName = ControllerContext.RouteData.Values["action"].ToString();
            var controllerName = ControllerContext.RouteData.Values["controller"].ToString();
            ViewBag.UploadLink = $"/{controllerName}/{actionName}";
            var info = GetInstallFormInfo();
            ViewBag.Title = info.Title;
            ViewBag.Label = info.Label;
            ViewBag.Session = GetSession();
            return View("~/Views/_UploadFromWeb.cshtml");
        }

        [HttpPost]
        public async Task<IActionResult> InstallFromWeb(string url, string session)
        {
            try
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

                    await UploadService.InstallAsync(tempFileStream, new Dictionary<string, string>(), session);
                }

                await ProgressHub.CompleteAsync(session);

                return Content("complete");
            }
            catch (Exception ex)
            {
                await ProgressHub.ErrorAsync(ex.Message, session);
                throw;
            }
        }

        public async Task<IActionResult> UpdateFromWeb()
        {
            var actionName = ControllerContext.RouteData.Values["action"].ToString();
            var controllerName = ControllerContext.RouteData.Values["controller"].ToString();
            ViewBag.UploadLink = $"/{controllerName}/{actionName}";
            var info = GetUpdateFormInfo();
            ViewBag.Title = info.Title;
            ViewBag.Label = info.Label;
            ViewBag.Session = GetSession();
            return View("~/Views/_UploadFromWeb.cshtml");
        }

        [HttpPost]
        public async Task<IActionResult> UpdateFromWeb(string url, string session)
        {
            try
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

                    await UploadService.UpdateAsync(tempFileStream, new Dictionary<string, string>(), session);
                }

                await ProgressHub.CompleteAsync(session);

                return Content("complete");
            }
            catch (Exception ex)
            {
                await ProgressHub.ErrorAsync(ex.Message, session);
                throw;
            }
        }

        public class UploadFormInfo
        {
            public string Title { get; set; }
            public string Label { get; set; }
        }
    }
}
