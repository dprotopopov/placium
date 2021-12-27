namespace Placium.Route.Profiles
{
    /// <summary>
    /// Represents different profile metrics.
    /// </summary>
    public enum ProfileMetric
    {
        /// <summary>
        /// A profile that uses time in seconds.
        /// </summary>
        /// <remarks>Means that Factor() = 1/Speed().</remarks>
        TimeInSeconds,
        /// <summary>
        /// A profile that uses distance in meters.
        /// </summary>
        /// <remarks>Means that Factor() is constant, Speed() returns the actual speed.</remarks>
        DistanceInMeters,
        /// <summary>
        /// A profile that uses a custom metric.
        /// </summary>
        /// <remarks>Means that Factor() can be anything, Speed() returns the actual speed.</remarks>
        Custom
    }
}