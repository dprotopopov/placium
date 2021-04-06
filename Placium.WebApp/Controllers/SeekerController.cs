﻿using System.Globalization;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Newtonsoft.Json;
using Placium.Seeker;

namespace Placium.WebApp.Controllers
{
    public class SeekerController : Controller
    {
        private readonly DefaultSeeker _seeker;

        public SeekerController(DefaultSeeker seeker)
        {
            _seeker = seeker;
        }

        public IActionResult ByCoords()
        {
            return View();
        }

        [HttpPost]
        public async Task<IActionResult> ByCoords(string coords)
        {
            var arr = coords.Split(",");
            var latitude = double.Parse(arr[0].Trim(), NumberStyles.Any, CultureInfo.InvariantCulture);
            var longitude = double.Parse(arr[1].Trim(), NumberStyles.Any, CultureInfo.InvariantCulture);
            var addr = await _seeker.GetAddrByCoordsAsync(latitude, longitude);
            var fias = await _seeker.GetFiasByAddrAsync(addr);
            return Content(JsonConvert.SerializeObject(new
            {
                addr,
                fias
            }));
        }

        public IActionResult ByAddr()
        {
            return View();
        }

        [HttpPost]
        public async Task<IActionResult> ByAddr(string addr, string housenumber)
        {
            var arr = addr.Split(",");
            var fias = await _seeker.GetFiasByAddrAsync(arr, housenumber);
            var osm = await _seeker.GetOsmByAddrAsync(arr, housenumber);
            return Content(JsonConvert.SerializeObject(new
            {
                fias,
                osm
            }));
        }

    }
}