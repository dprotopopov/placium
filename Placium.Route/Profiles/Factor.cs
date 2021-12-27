namespace Placium.Route.Profiles
{
    /// <summary>
    /// A factor returned by a routing profile to influence routing.
    /// </summary>
    public struct Factor
    {
        /// <summary>
        /// Gets or sets the actual factor.
        /// </summary>
        public float Value { get; set; }

        /// <summary>
        /// Gets or sets the direction.
        /// </summary>
        /// 0=bidirectional, 1=forward, 2=backward.
        public short Direction { get; set; }

        /// <summary>
        /// Returns a non-value.
        /// </summary>
        public static Factor NoFactor { get { return new Factor() { Direction = 0, Value = 0 }; } }
    }
}