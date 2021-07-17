using System.Collections.Generic;
using NetTopologySuite.Geometries;

namespace Placium.Models
{
    public class Placex
    {
        public long id { get; set; }
        public Dictionary<string, string> tags { get; set; }
        public Geometry location { get; set; }
    }
}
