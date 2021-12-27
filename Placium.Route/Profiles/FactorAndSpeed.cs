namespace Placium.Route.Profiles
{
    /// <summary>
    /// A factor returned by a routing profile to influence routing augmented with the speed.
    /// </summary>
    public struct FactorAndSpeed
    {
        /// <summary>
        /// Gets or sets the actual factor.
        /// </summary>
        public float Value { get; set; }

        /// <summary>
        /// Gets or sets the speed (1/m/s).
        /// </summary>
        public float SpeedFactor { get; set; }

        /// <summary>
        /// Gets or sets the direction.
        /// </summary>
        /// 0=bidirectional, 1=forward, 2=backward.
        /// 3=bidirectional, 4=forward, 5=backward but without stopping abilities.
        public short Direction { get; set; }

        /// <summary>
        /// Gets or sets the constraint values.
        /// </summary>
        public float[] Constraints { get; set; }
        
        /// <summary>
        /// Returns a non-value.
        /// </summary>
        public static FactorAndSpeed NoFactor { get { return new FactorAndSpeed() { Direction = 0, Value = 0, SpeedFactor = 0, Constraints = null }; } }
    }
}