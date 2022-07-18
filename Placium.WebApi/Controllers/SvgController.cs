using System;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using NLog.LayoutRenderers;
using Placium.Services;

namespace Placium.WebApi.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class SvgController : ControllerBase
    {
        private readonly SvgService _svgService;

        public SvgController(SvgService svgService)
        {
            _svgService = svgService;
        }

        [HttpGet]
        [ProducesResponseType(200, Type = typeof(string))]
        public async Task<IActionResult> GetSvg(
            string osm_ids =
                "R2162196,R162903,R1252558,R1320234,R1278703,R1282181,R1304596,R226149,R446092,R1320358,R2263058,R2263059",
            int width = 1200, int height = 800)
        {
            return Ok(await _svgService.GetSvg(
                osm_ids.Split(',').Select(x => x.Trim()).Distinct(StringComparer.InvariantCultureIgnoreCase).ToList(),
                width, height));
        }
    }
}