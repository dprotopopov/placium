using System.Collections.Generic;
using System.Globalization;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Placium.Models;
using Placium.Services;

namespace Placium.WebApi.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class PlacexController : ControllerBase
    {
        private readonly PlacexService _placexService;

        public PlacexController(PlacexService placexService)
        {
            _placexService = placexService;
        }

        [HttpGet("by_name")]
        [ProducesResponseType(200, Type = typeof(List<Placex>))]
        public async Task<IActionResult> GetByNameAsync(string pattern)
        {
            return Ok(await _placexService.GetByNameAsync(pattern));
        }

        [HttpGet("by_coords")]
        [ProducesResponseType(200, Type = typeof(List<Placex>))]
        public async Task<IActionResult> GetByCoordsAsync(string coords)
        {
            var arr = coords.Split(",");
            var latitude = double.Parse(arr[0].Trim(), NumberStyles.Any, CultureInfo.InvariantCulture);
            var longitude = double.Parse(arr[1].Trim(), NumberStyles.Any, CultureInfo.InvariantCulture);
            return Ok(await _placexService.GetByCoordsAsync(latitude, longitude));
        }
    }
}