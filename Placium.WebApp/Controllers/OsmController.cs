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
        private readonly OsmService _osmService;

        public OsmController(OsmService osmService)
        {
            _osmService = osmService;
        }

        public IActionResult ById()
        {
            return View();
        }

        [HttpPost]
        public async Task<IActionResult> ById(string osm_id, string osm_type)
        {
            long.TryParse(osm_id, out var id);

            var type = (OsmType) Enum.Parse(typeof(OsmType), osm_type, true);

            return Content(JsonConvert.SerializeObject(await _osmService.GetByIdAsync(id, type)));
        }
    }
}