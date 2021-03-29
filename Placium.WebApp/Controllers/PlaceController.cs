using System.Globalization;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Newtonsoft.Json;
using Placium.WebApi.Services;

namespace Placium.WebApp.Controllers
{
    public class PlaceController : Controller
    {
        private readonly PlaceApiService _placeApiService;

        public PlaceController(PlaceApiService placeApiService)
        {
            _placeApiService = placeApiService;
        }

        public IActionResult ByName()
        {
            return View();
        }

        [HttpPost]
        public async Task<IActionResult> ByName(string pattern)
        {
            return Content(JsonConvert.SerializeObject(await _placeApiService.GetByNameAsync(pattern)));
        }
        public IActionResult ByPoint()
        {
            return View();
        }

        [HttpPost]
        public async Task<IActionResult> ByPoint(string coords)
        {
            var arr = coords.Split(",");
            var latitude = double.Parse(arr[0].Trim(), NumberStyles.Any, CultureInfo.InvariantCulture);
            var longitude = double.Parse(arr[1].Trim(), NumberStyles.Any, CultureInfo.InvariantCulture);
            return Content(JsonConvert.SerializeObject(await _placeApiService.GetByPointAsync(latitude, longitude)));
        }
    }
}