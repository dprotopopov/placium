using System.IO;
using System.Net;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Npgsql;
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

        public async Task<IActionResult> InstallFromFile()
        {
            var actionName = ControllerContext.RouteData.Values["action"].ToString();
            var controllerName = ControllerContext.RouteData.Values["controller"].ToString();
            ViewBag.UploadLink = $"/{controllerName}/{actionName}";
            var info = GetInstallFormInfo();
            ViewBag.Title = info.Title;
            ViewBag.Label = info.Label;
            return View("~/Views/_UploadFromFile.cshtml");
        }

        protected abstract string GetConnectionString();
        protected abstract UploadFormInfo GetInstallFormInfo();
        protected abstract UploadFormInfo GetUpdateFormInfo();

        [HttpPost]
        [DisableRequestSizeLimit]
        public async Task<IActionResult> InstallFromFile(IFormFile file, string session)
        {
            var connectionString = GetConnectionString();

            using (var connection = new NpgsqlConnection(connectionString))
            using (var stream = file.OpenReadStream())
            {
                connection.Open();
                await UploadService.InstallAsync(stream, connection, session);
                connection.Close();
            }

            return Content(file.FileName);
        }

        public async Task<IActionResult> UpdateFromFile()
        {
            var actionName = ControllerContext.RouteData.Values["action"].ToString();
            var controllerName = ControllerContext.RouteData.Values["controller"].ToString();
            ViewBag.UploadLink = $"/{controllerName}/{actionName}";
            var info = GetUpdateFormInfo();
            ViewBag.Title = info.Title;
            ViewBag.Label = info.Label;

            return View("~/Views/_UploadFromFile.cshtml");
        }

        [HttpPost]
        [DisableRequestSizeLimit]
        public async Task<IActionResult> UpdateFromFile(IFormFile file, string session)
        {
            var connectionString = GetConnectionString();

            using (var connection = new NpgsqlConnection(connectionString))
            using (var stream = file.OpenReadStream())
            {
                connection.Open();
                await UploadService.UpdateAsync(stream, connection, session);
                connection.Close();
            }

            return Content(file.FileName);
        }

        public async Task<IActionResult> InstallFromUrl()
        {
            var actionName = ControllerContext.RouteData.Values["action"].ToString();
            var controllerName = ControllerContext.RouteData.Values["controller"].ToString();
            ViewBag.UploadLink = $"/{controllerName}/{actionName}";
            var info = GetInstallFormInfo();
            ViewBag.Title = info.Title;
            ViewBag.Label = info.Label;
            return View("~/Views/_UploadFromUrl.cshtml");
        }

        [HttpPost]
        public async Task<IActionResult> InstallFromUrl(string url, string session)
        {
            var connectionString = GetConnectionString();
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

                using (var connection = new NpgsqlConnection(connectionString))
                {
                    connection.Open();
                    await UploadService.InstallAsync(tempFileStream, connection, session);
                    connection.Close();
                }
            }

            return Content(url);
        }

        public async Task<IActionResult> UpdateFromUrl()
        {
            var actionName = ControllerContext.RouteData.Values["action"].ToString();
            var controllerName = ControllerContext.RouteData.Values["controller"].ToString();
            ViewBag.UploadLink = $"/{controllerName}/{actionName}";
            var info = GetUpdateFormInfo();
            ViewBag.Title = info.Title;
            ViewBag.Label = info.Label;

            return View("~/Views/_UploadFromUrl.cshtml");
        }

        [HttpPost]
        public async Task<IActionResult> UpdateFromUrl(string url, string session)
        {
            var connectionString = GetConnectionString();
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

                using (var connection = new NpgsqlConnection(connectionString))
                {
                    connection.Open();
                    await UploadService.UpdateAsync(tempFileStream, connection, session);
                    connection.Close();
                }
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