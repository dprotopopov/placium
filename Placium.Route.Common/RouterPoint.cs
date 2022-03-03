using Route.LocalGeo;

namespace Placium.Route.Common
{
    /// <summary>
    ///     Represents a resolved point. A hook for the router to route on.
    /// </summary>
    public class RouterPoint
    {
        /// <summary>
        ///     Gets the edge id.
        /// </summary>
        public long EdgeId { get; set; }

        /// <summary>
        ///     Gets the offset.
        /// </summary>
        public int Offset { get; set; }

        public long FromNode { get; set; }
        public long ToNode { get; set; }
        public Coordinate[] Coordinates { get; set; }
        public Coordinate Coordinate { get; set; }
        public short Direction { get; set; }
        public float Weight { get; set; }
    }
}