using System.Collections.Generic;
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
        private readonly PlaceApiService _placeApiService;

        public PlaceController(PlaceApiService placeApiService)
        {
            _placeApiService = placeApiService;
        }

        [HttpGet("by_name")]
        [ProducesResponseType(200, Type = typeof(List<Place>))]
        public async Task<IActionResult> GetByNameAsync(string pattern)
        {
            return Ok(await _placeApiService.GetByNameAsync(pattern));
        }
    }
}