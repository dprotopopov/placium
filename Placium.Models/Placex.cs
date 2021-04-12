using System.Collections.Generic;
using GeoJSON.Net;

namespace Placium.Models
{
    public class Placex
    {
        public long id { get; set; }
        public Dictionary<string, string> tags { get; set; }
        public object location { get; set; }
    }
}