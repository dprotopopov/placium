﻿using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Placium.Models;
using Placium.Services;

namespace Placium.WebApi.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class GarController : ControllerBase
    {
        private readonly GarService _garService;

        public GarController(GarService garService)
        {
            _garService = garService;
        }

        [HttpGet("{objectid}/details")]
        [ProducesResponseType(200, Type = typeof(List<Element>))]
        [ProducesResponseType(404)]
        public async Task<IActionResult> GetDetails(long objectid, DateTime? dateTime = null)
        {
            return Ok(await _garService.GetDetailsAsync(objectid, dateTime));
        }

        [HttpGet("{objectid}/children")]
        [ProducesResponseType(200, Type = typeof(List<Element>))]
        [ProducesResponseType(404)]
        public async Task<IActionResult> GetChildren(long objectid, DateTime? dateTime = null)
        {
            return Ok(await _garService.GetChildrenAsync(objectid, dateTime));
        }


        [HttpGet("roots")]
        [ProducesResponseType(200, Type = typeof(List<Element>))]
        [ProducesResponseType(404)]
        public async Task<IActionResult> GetRoots(DateTime? dateTime = null)
        {
            return Ok(await _garService.GetRootsAsync(dateTime));
        }
    }
}