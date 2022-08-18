using System.Collections.Generic;
using Microsoft.AspNetCore.Mvc.Rendering;

namespace Placium.WebApp.Models
{
    public class GarSelectModel
    {
        public List<SelectListItem> PreviousItems { get; set; }
        public List<SelectListItem> NextItems { get; set; }
    }
}