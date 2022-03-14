using System.Collections.Generic;
using System.Globalization;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using NetTopologySuite.IO.Converters;
using Newtonsoft.Json;
using Placium.Services;

namespace Placium.WebApp.Controllers
{
    public class PlacexController : Controller
    {
        private readonly PlacexService _placexService;

        public PlacexController(PlacexService placexService)
        {
            _placexService = placexService;
        }

        public async Task<IActionResult> ByName()
        {
            return await Task.FromResult(View());
        }

        [HttpPost]
        public async Task<IActionResult> ByName(string pattern)
        {
            var serializerSettings = new JsonSerializerSettings
            {
                ReferenceLoopHandling = ReferenceLoopHandling.Ignore,
                Converters = new List<JsonConverter>
                {
                    new GeometryConverter(),
                    new CoordinateConverter()
                }
            };
            return Content(
                JsonConvert.SerializeObject(await _placexService.GetByNameAsync(pattern), serializerSettings));
        }

        public async Task<IActionResult> ByCoords()
        {
            return await Task.FromResult(View());
        }

        [HttpPost]
        public async Task<IActionResult> ByCoords(string coords)
        {
            var arr = coords.Split(",");
            var latitude = double.Parse(arr[0].Trim(), NumberStyles.Any, CultureInfo.InvariantCulture);
            var longitude = double.Parse(arr[1].Trim(), NumberStyles.Any, CultureInfo.InvariantCulture);
            var serializerSettings = new JsonSerializerSettings
            {
                ReferenceLoopHandling = ReferenceLoopHandling.Ignore,
                Converters = new List<JsonConverter>
                {
                    new GeometryConverter(),
                    new CoordinateConverter()
                }
            };
            return Content(JsonConvert.SerializeObject(await _placexService.GetByCoordsAsync(latitude, longitude),
                serializerSettings));
        }
    }
}