using System.Globalization;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Newtonsoft.Json;
using Placium.WebApi.Services;

namespace Placium.WebApp.Controllers
{
    public class PlacexController : Controller
    {
        private readonly PlacexService _placexService;

        public PlacexController(PlacexService placexService)
        {
            _placexService = placexService;
        }

        public IActionResult ByName()
        {
            return View();
        }

        [HttpPost]
        public async Task<IActionResult> ByName(string pattern)
        {
            return Content(JsonConvert.SerializeObject(await _placexService.GetByNameAsync(pattern)));
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
            return Content(JsonConvert.SerializeObject(await _placexService.GetByCoordsAsync(latitude, longitude)));
        }
    }
}