using System;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Newtonsoft.Json;
using Placium.Types;
using Placium.WebApi.Services;

namespace Placium.WebApp.Controllers
{
    public class OsmController : Controller
    {
        private readonly OsmApiService _osmApiService;

        public OsmController(OsmApiService osmApiService)
        {
            _osmApiService = osmApiService;
        }

        public IActionResult Index()
        {
            return View();
        }

        [HttpPost]
        public async Task<IActionResult> Index(string osm_id, string osm_type)
        {
            long.TryParse(osm_id, out var id);

            var type = (OsmType) Enum.Parse(typeof(OsmType), osm_type, true);

            return Content(JsonConvert.SerializeObject(await _osmApiService.GetByIdAsync(id, type)));
        }
    }
}