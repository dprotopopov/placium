using System.Collections.Generic;
using System.Globalization;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Placium.Models;
using Placium.Seeker;

namespace Placium.WebApi.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class SeekerController : ControllerBase
    {
        private readonly DefaultSeeker _seeker;

        public SeekerController(DefaultSeeker seeker)
        {
            _seeker = seeker;
        }

        [HttpGet("addr_by_coords")]
        [ProducesResponseType(200, Type = typeof(Dictionary<string, string>))]
        public async Task<IActionResult> GetAddrByCoordsAsync(string coords)
        {
            var arr = coords.Split(",");
            var latitude = double.Parse(arr[0].Trim(), NumberStyles.Any, CultureInfo.InvariantCulture);
            var longitude = double.Parse(arr[1].Trim(), NumberStyles.Any, CultureInfo.InvariantCulture);
            return Ok(await _seeker.GetAddrByCoordsAsync(latitude, longitude));
        }

        [HttpGet("fias_by_addr")]
        [ProducesResponseType(200, Type = typeof(List<string>))]
        public async Task<IActionResult> GetFiasByAddrAsync(string addr, string housenumber, string roomnumber)
        {
            return Ok(await _seeker.GetFiasByAddrAsync(addr.Split(","), housenumber, roomnumber));
        }

        [HttpGet("fias_by_addr2")]
        [ProducesResponseType(200, Type = typeof(List<string>))]
        public async Task<IActionResult> GetFiasByAddr2Async(string addr, string housenumber, string roomnumber)
        {
            return Ok(await _seeker.GetFiasByAddr2Async(addr.Split(","), housenumber, roomnumber));
        }

        [HttpGet("osm_by_addr")]
        [ProducesResponseType(200, Type = typeof(List<Placex>))]
        public async Task<IActionResult> GetOsmByAddrAsync(string addr, string housenumber)
        {
            return Ok(await _seeker.GetOsmByAddrAsync(addr.Split(","), housenumber));
        }

        [HttpGet("osm_suggest")]
        [ProducesResponseType(200, Type = typeof(List<string>))]
        public async Task<IActionResult> GetOsmSuggestAsync(string search, int limit = 20)
        {
            return Ok(await _seeker.GetOsmSuggestAsync(search, limit));
        }

        [HttpGet("fias_suggest")]
        [ProducesResponseType(200, Type = typeof(List<string>))]
        public async Task<IActionResult> GetFiasSuggestAsync(string search, int limit = 20)
        {
            return Ok(await _seeker.GetFiasSuggestAsync(search, limit));
        }
    }
}