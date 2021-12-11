using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.Rendering;
using Placium.Models;
using Placium.Services;
using Placium.WebApp.Models;

namespace Placium.WebApp.Controllers
{
    public class FiasController : Controller
    {
        private readonly FiasService _fiasService;

        public FiasController(FiasService fiasService)
        {
            _fiasService = fiasService;
        }


        public async Task<IActionResult> Index(string guid = null)
        {
            var model = new FiasSelectModel();


            if (string.IsNullOrEmpty(guid))
            {
                model.PreviousItems = new List<SelectListItem>();

                model.NextItems = (await _fiasService.GetRootsAsync()).Cast<Element>().Select(x => new SelectListItem
                {
                    Text = x.title,
                    Value = x.guid.ToString()
                }).OrderBy(x => x.Text).ToList();
            }
            else
            {
                model.PreviousItems = (await _fiasService.GetDetailsAsync(guid)).Cast<Element>().Select(x =>
                    new SelectListItem
                    {
                        Text = x.title,
                        Value = x.guid.ToString()
                    }).ToList();

                model.NextItems = (await _fiasService.GetChildrenAsync(guid)).Cast<Element>().Select(x =>
                    new SelectListItem
                    {
                        Text = x.title,
                        Value = x.guid.ToString()
                    }).OrderBy(x => x.Text).ToList();
            }

            return View(model);
        }
    }
}