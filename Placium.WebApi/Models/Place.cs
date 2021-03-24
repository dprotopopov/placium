using System.Collections.Generic;
using GeoJSON.Net;

namespace Placium.WebApi.Models
{
    public class Place
    {
        public string title { get; set; }
        public Dictionary<string, string> tags { get; set; }
        public GeoJSONObject location { get; set; }
    }
}