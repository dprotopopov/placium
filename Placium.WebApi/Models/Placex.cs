using System.Collections.Generic;
using GeoJSON.Net;

namespace Placium.WebApi.Models
{
    public class Placex
    {
        public long id { get; set; }
        public Dictionary<string, string> tags { get; set; }
        public GeoJSONObject location { get; set; }
    }
}