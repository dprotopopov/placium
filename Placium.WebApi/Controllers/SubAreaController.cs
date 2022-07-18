using System;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Placium.Services;

namespace Placium.WebApi.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class SubAreaController : ControllerBase
    {
        private readonly SubAreaService _subAreaService;

        public SubAreaController(SubAreaService subAreaService)
        {
            _subAreaService = subAreaService;
        }


        [HttpGet]
        [ProducesResponseType(200, Type = typeof(string))]
        public async Task<IActionResult> GetSubArea(
            string osm_ids = "R102269",
            int level = 1)
        {
            return Ok(await _subAreaService.GetSubArea(
                osm_ids.Split(',').Select(x => x.Trim()).Distinct(StringComparer.InvariantCultureIgnoreCase).ToList(),
                level));
        }
    }
}