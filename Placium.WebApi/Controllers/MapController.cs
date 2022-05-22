using System;
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
            string osm_ids =
                "R2162196,R162903,R1252558,R1320234,R1278703,R1282181,R1304596,R226149,R446092,R1320358,R2263058,R2263059",
            int width = 1200, int height = 800, bool json = false)
        {
            return Ok(await _mapService.GetMap(name,
                osm_ids.Split(',').Select(x => x.Trim()).Distinct(StringComparer.InvariantCultureIgnoreCase).ToList(),
                width, height, json));
        }
    }
}