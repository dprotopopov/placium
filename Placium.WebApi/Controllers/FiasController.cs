using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Placium.WebApi.Models;
using Placium.WebApi.Services;

namespace Placium.WebApi.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class FiasController : ControllerBase
    {
        private readonly FiasApiService _fiasApiService;

        public FiasController(FiasApiService fiasApiService)
        {
            _fiasApiService = fiasApiService;
        }

        [HttpGet("{guid}/details")]
        [ProducesResponseType(200, Type = typeof(List<Element>))]
        [ProducesResponseType(404)]
        public async Task<IActionResult> GetDetails(string guid, bool formal = false, bool socr = false)
        {
            return Ok(await _fiasApiService.GetDetailsAsync(guid, formal, socr));
        }

        [HttpGet("{guid}/text")]
        [ProducesResponseType(200, Type = typeof(string))]
        [ProducesResponseType(404)]
        public async Task<IActionResult> GetText(string guid, bool formal = false, bool socr = false)
        {
            return Ok(string.Join(", ", (await _fiasApiService.GetDetailsAsync(guid, formal, socr)).Select(x => x.title)));
        }

        [HttpGet("{guid}/children")]
        [ProducesResponseType(200, Type = typeof(List<Element>))]
        [ProducesResponseType(404)]
        public async Task<IActionResult> GetChildren(string guid, bool formal = false, bool socr = false)
        {
            return Ok(await _fiasApiService.GetChildrenAsync(guid, formal, socr));
        }


        [HttpGet("roots")]
        [ProducesResponseType(200, Type = typeof(List<Element>))]
        [ProducesResponseType(404)]
        public async Task<IActionResult> GetRoots(bool formal = false, bool socr = false)
        {
            return Ok(await _fiasApiService.GetRootsAsync(formal, socr));
        }
    }
}