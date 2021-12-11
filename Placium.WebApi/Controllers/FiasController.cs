using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Placium.Seeker;
using Placium.Services;

namespace Placium.WebApi.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class FiasController : ControllerBase
    {
        private readonly FiasAddressService _fiasAddressService;
        private readonly FiasService _fiasService;

        public FiasController(FiasService fiasService, FiasAddressService fiasAddressService)
        {
            _fiasService = fiasService;
            _fiasAddressService = fiasAddressService;
        }

        [HttpGet("{guid}/details")]
        [ProducesResponseType(200, Type = typeof(List<object>))]
        [ProducesResponseType(404)]
        public async Task<IActionResult> GetDetails(string guid, bool formal = false, bool socr = true)
        {
            return Ok(await _fiasService.GetDetailsAsync(guid, formal, socr));
        }

        [HttpGet("{guid}/text")]
        [ProducesResponseType(200, Type = typeof(string))]
        [ProducesResponseType(404)]
        public async Task<IActionResult> GetText(string guid, bool formal = false, bool socr = true)
        {
            return Ok(string.Join(", ",
                (await _fiasService.GetDetailsAsync(guid, formal, socr)).Select(x => x.ToString())));
        }

        [HttpGet("{guid}/children")]
        [ProducesResponseType(200, Type = typeof(List<object>))]
        [ProducesResponseType(404)]
        public async Task<IActionResult> GetChildren(string guid, bool formal = false, bool socr = true)
        {
            return Ok(await _fiasService.GetChildrenAsync(guid, formal, socr));
        }


        [HttpGet("roots")]
        [ProducesResponseType(200, Type = typeof(List<object>))]
        [ProducesResponseType(404)]
        public async Task<IActionResult> GetRoots(bool formal = false, bool socr = true)
        {
            return Ok(await _fiasService.GetRootsAsync(formal, socr));
        }

        [HttpGet]
        [ProducesResponseType(200, Type = typeof(List<AddressEntry>))]
        [ProducesResponseType(404)]
        public async Task<IActionResult> Get(string searchString, int limit = 20)
        {
            return Ok(await _fiasAddressService.GetByNameAsync(searchString, limit));
        }
    }
}