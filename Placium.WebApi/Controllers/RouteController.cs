using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Placium.Common;
using Placium.Seeker;

namespace Placium.WebApi.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class RouteController : ControllerBase
    {
        private readonly OsmRouteService _osmRouteService;

        public RouteController(OsmRouteService osmRouteService)
        {
            _osmRouteService = osmRouteService;
        }

        [HttpGet]
        public async Task<IActionResult> Calculate(string source, string target)
        {
            return Ok(await _osmRouteService.CalculateAsync(source.ToCoordinate(), target.ToCoordinate()));
        }
    }
}