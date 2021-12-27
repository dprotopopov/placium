namespace Placium.Route.Profiles
{
    /// <summary>
    /// A speed returned by a routing profile to influence routing.
    /// </summary>
    public struct Speed
    {
        /// <summary>
        /// Gets or sets the value in m/s.
        /// </summary>
        public float Value { get; set; }

        /// <summary>
        /// Gets or sets the direction.
        /// </summary>
        /// 0=bidirectional, 1=forward, 2=backward.
        public short Direction { get; set; }

        /// <summary>
        /// Returns a default speed represent a non-value.
        /// </summary>
        public static Speed NoSpeed { get { return new Speed() { Direction = 0, Value = 0 }; } }
    }
}
