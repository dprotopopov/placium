using System.Collections.Generic;
using System.Globalization;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Placium.Seeker;

namespace Placium.WebApi.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class AddressController : ControllerBase
    {
        private readonly OsmAddressService _osmAddressService;

        public AddressController(OsmAddressService osmAddressService)
        {
            _osmAddressService = osmAddressService;
        }

        [HttpGet]
        [ProducesResponseType(200, Type = typeof(List<AddressEntry>))]
        [ProducesResponseType(404)]
        public async Task<IActionResult> Get(string searchString, int limit = 20)
        {
            return Ok(await _osmAddressService.GetByNameAsync(searchString, limit));
        }

        [Route("by_coords")]
        [HttpGet]
        [ProducesResponseType(200, Type = typeof(List<AddressEntry>))]
        [ProducesResponseType(404)]
        public async Task<IActionResult> GetByCoords(string coords, int limit = 20)
        {
            var arr = coords.Split(",");
            var latitude = double.Parse(arr[0].Trim(), NumberStyles.Any, CultureInfo.InvariantCulture);
            var longitude = double.Parse(arr[1].Trim(), NumberStyles.Any, CultureInfo.InvariantCulture);
            return Ok(await _osmAddressService.GetByCoordsAsync(latitude, longitude, limit));
        }
    }
}
