using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Placium.Services;

namespace Placium.WebApi.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class MapController : ControllerBase
    {
        private readonly MapService _mapService;

        public MapController(MapService mapService)
        {
            _mapService = mapService;
        }

        [HttpGet]
        [ProducesResponseType(200, Type = typeof(string))]
        public async Task<IActionResult> GetMap(string name = "Moscow",
            string keys = "R446092,R162903,R226149,R1252558,R1278703,R1282181,R1304596,R1320234,R2162196,R2263058,R1988678",
            int width = 1200, int height = 800)
        {
            return Ok(await _mapService.GetMap(name, keys.Split(',').Select(x => x.Trim()).ToList(), width,
                height));
        }
    }
}