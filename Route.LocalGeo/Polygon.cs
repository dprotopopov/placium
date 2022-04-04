using System.Collections.Generic;

namespace Route.LocalGeo
{
    /// <summary>
    ///     Represents a polygon.
    /// </summary>
    public class Polygon
    {
        /// <summary>
        ///     The exterior ring.
        /// </summary>
        public List<Coordinate> ExteriorRing { get; set; } = new List<Coordinate>();

        /// <summary>
        ///     The interior rings.
        /// </summary>
        public List<List<Coordinate>> InteriorRings { get; set; } = new List<List<Coordinate>>();
    }
}