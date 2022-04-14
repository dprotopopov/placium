using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Placium.Seeker;

namespace Placium.WebApi.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class NominatimController : ControllerBase
    {
        private readonly OsmNominatimService _osmNominatimService;

        public NominatimController(OsmNominatimService osmNominatimService)
        {
            _osmNominatimService = osmNominatimService;
        }

        [HttpGet("search")]
        [ProducesResponseType(200, Type = typeof(List<NominatimEntry>))]
        [ProducesResponseType(404)]
        public async Task<IActionResult> Search(string q, int limit = 20, bool raw = false, bool custom = false)
        {
            return Ok(await _osmNominatimService.GetByNameAsync(q, limit, raw, custom));
        }

        [Route("reverse")]
        [HttpGet]
        [ProducesResponseType(200, Type = typeof(List<NominatimEntry>))]
        [ProducesResponseType(404)]
        public async Task<IActionResult> Reverse(float lat, float lon, int limit = 20, bool raw = false, bool custom = false)
        {
            return Ok(await _osmNominatimService.GetByCoordsAsync(lat, lon, limit, raw, custom));
        }
    }
}