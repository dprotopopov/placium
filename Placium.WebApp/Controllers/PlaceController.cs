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

        public IActionResult Index()
        {
            return View();
        }

        [HttpPost]
        public async Task<IActionResult> Index(string pattern)
        {
            return Content(JsonConvert.SerializeObject(await _placeApiService.GetByNameAsync(pattern)));
        }
    }
}