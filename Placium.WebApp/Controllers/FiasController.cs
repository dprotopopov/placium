using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.Rendering;
using Placium.WebApi.Services;
using Placium.WebApp.Models;

namespace Placium.WebApp.Controllers
{
    public class FiasController : Controller
    {
        private readonly FiasApiService _fiasApiService;

        public FiasController(FiasApiService fiasApiService)
        {
            _fiasApiService = fiasApiService;
        }


        public async Task<IActionResult> Index(string guid = null)
        {
            var model = new FiasSelectModel();


            if (string.IsNullOrEmpty(guid))
            {
                model.PreviousItems = new List<SelectListItem>();

                model.NextItems = (await _fiasApiService.GetRootsAsync(socr: true)).Select(x => new SelectListItem
                {
                    Text = x.title,
                    Value = x.guid.ToString()
                }).OrderBy(x => x.Text).ToList();
            }
            else
            {
                model.PreviousItems = (await _fiasApiService.GetDetailsAsync(guid, socr: true)).Select(x =>
                    new SelectListItem
                    {
                        Text = x.title,
                        Value = x.guid.ToString()
                    }).ToList();

                model.NextItems = (await _fiasApiService.GetChildrenAsync(guid, socr: true)).Select(x => new SelectListItem
                {
                    Text = x.title,
                    Value = x.guid.ToString()
                }).OrderBy(x => x.Text).ToList();
            }

            return View(model);
        }
    }
}