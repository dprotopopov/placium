using System.Collections.Generic;
using System.Globalization;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Placium.WebApi.Models;
using Placium.WebApi.Services;

namespace Placium.WebApi.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class PlaceController : ControllerBase
    {
        private readonly PlaceService _placeService;

        public PlaceController(PlaceService placeService)
        {
            _placeService = placeService;
        }

        [HttpGet("by_name")]
        [ProducesResponseType(200, Type = typeof(List<Place>))]
        public async Task<IActionResult> GetByNameAsync(string pattern)
        {
            return Ok(await _placeService.GetByNameAsync(pattern));
        }

        [HttpGet("by_coords")]
        [ProducesResponseType(200, Type = typeof(List<Place>))]
        public async Task<IActionResult> GetByCoordsAsync(string coords)
        {
            var arr = coords.Split(",");
            var latitude = double.Parse(arr[0].Trim(), NumberStyles.Any, CultureInfo.InvariantCulture);
            var longitude = double.Parse(arr[1].Trim(), NumberStyles.Any, CultureInfo.InvariantCulture);
            return Ok(await _placeService.GetByCoordsAsync(latitude, longitude));
        }
    }
}