using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using NetTopologySuite.IO.Converters;
using Newtonsoft.Json;
using Placium.Services;
using Placium.Types;

namespace Placium.WebApp.Controllers
{
    public class OsmController : Controller
    {
        private readonly OsmService _osmService;

        public OsmController(OsmService osmService)
        {
            _osmService = osmService;
        }

        public async Task<IActionResult> ById()
        {
            return await Task.FromResult(View());
        }

        [HttpPost]
        public async Task<IActionResult> ById(string osm_id, string osm_type)
        {
            long.TryParse(osm_id, out var id);

            var type = (OsmType)Enum.Parse(typeof(OsmType), osm_type, true);

            var serializerSettings = new JsonSerializerSettings
            {
                ReferenceLoopHandling = ReferenceLoopHandling.Ignore,
                Converters = new List<JsonConverter>
                {
                    new GeometryConverter(),
                    new CoordinateConverter()
                }
            };

            return Content(JsonConvert.SerializeObject(await _osmService.GetByIdAsync(id, type), serializerSettings));
        }
    }
}