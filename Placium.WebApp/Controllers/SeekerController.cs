using System.Collections.Generic;
using System.Globalization;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using NetTopologySuite.IO.Converters;
using Newtonsoft.Json;
using Placium.Seeker;

namespace Placium.WebApp.Controllers
{
    public class SeekerController : Controller
    {
        private readonly DefaultSeeker _seeker;

        public SeekerController(DefaultSeeker seeker)
        {
            _seeker = seeker;
        }

        public IActionResult ByCoords()
        {
            return View();
        }

        [HttpPost]
        public async Task<IActionResult> ByCoords(string coords)
        {
            var arr = coords.Split(",");
            var latitude = double.Parse(arr[0].Trim(), NumberStyles.Any, CultureInfo.InvariantCulture);
            var longitude = double.Parse(arr[1].Trim(), NumberStyles.Any, CultureInfo.InvariantCulture);
            var addr = await _seeker.GetAddrByCoordsAsync(latitude, longitude);
            var fias = await _seeker.GetFiasByAddrAsync(addr);
            var serializerSettings = new JsonSerializerSettings
            {
                ReferenceLoopHandling = ReferenceLoopHandling.Ignore,
                Converters = new List<JsonConverter>
                {
                    new GeometryConverter(),
                    new CoordinateConverter()
                }
            };
            return Content(JsonConvert.SerializeObject(new
            {
                addr,
                fias
            }, serializerSettings));
        }

        public IActionResult ByAddr()
        {
            return View();
        }

        [HttpPost]
        public async Task<IActionResult> ByAddr(string addr, string housenumber)
        {
            var arr = addr.Split(",");
            var fias = await _seeker.GetFiasByAddrAsync(arr, housenumber);
            var osm = await _seeker.GetOsmByAddrAsync(arr, housenumber);
            var serializerSettings = new JsonSerializerSettings
            {
                ReferenceLoopHandling = ReferenceLoopHandling.Ignore,
                Converters = new List<JsonConverter>
                {
                    new GeometryConverter(),
                    new CoordinateConverter()
                }
            };
            return Content(JsonConvert.SerializeObject(new
            {
                fias,
                osm
            }, serializerSettings));
        }

        public IActionResult OsmSuggest()
        {
            return View();
        }

        [HttpPost]
        public async Task<IActionResult> OsmSuggest(string search, int limit = 20)
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
            return Content(JsonConvert.SerializeObject(await _seeker.GetOsmSuggestAsync(search, limit),
                serializerSettings));
        }

        public IActionResult FiasSuggest()
        {
            return View();
        }

        [HttpPost]
        public async Task<IActionResult> FiasSuggest(string search, int limit = 20)
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
            return Content(JsonConvert.SerializeObject(await _seeker.GetFiasSuggestAsync(search, limit),
                serializerSettings));
        }
    }
}