using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Placium.Common;
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
        public async Task<IActionResult> Get(string searchString, int limit = 20, bool raw = false, int field = 0)
        {
            return Ok(await _osmAddressService.GetByNameAsync(searchString, limit, raw, field));
        }

        [Route("by_coords")]
        [HttpGet]
        [ProducesResponseType(200, Type = typeof(List<AddressEntry>))]
        [ProducesResponseType(404)]
        public async Task<IActionResult> GetByCoords(string coords, int limit = 20, bool raw = false)
        {
            return Ok(await _osmAddressService.GetByCoordsAsync(coords.ToCoordinate(), limit, raw));
        }
    }
}