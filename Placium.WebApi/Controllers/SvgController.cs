using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
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
            string keys = "R446092,R162903,R226149,R1252558,R1278703,R1282181,R1304596,R1320234,R2162196,R2263058,R1988678",
            int width = 1200, int height = 800)
        {
            return Ok(await _svgService.GetSvg(keys.Split(',').Select(x => x.Trim()).ToList(), width, height));
        }
    }
}