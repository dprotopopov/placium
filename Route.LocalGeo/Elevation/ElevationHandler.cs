namespace Route.LocalGeo.Elevation
{
    /// <summary>
    ///     An elevation handler.
    /// </summary>
    public static class ElevationHandler
    {
        /// <summary>
        ///     A delegate to get elevation.
        /// </summary>
        public delegate short? GetElevationDelegate(float latitude, float longitude);

        /// <summary>
        ///     Gets or sets the delegate to get elevation.
        /// </summary>
        public static GetElevationDelegate GetElevation = null;

        /// <summary>
        ///     Add elevation to the given coordinate.
        /// </summary>
        public static short? Elevation(this Coordinate coordinate)
        {
            if (GetElevation != null) return GetElevation(coordinate.Latitude, coordinate.Longitude);
            return null;
        }
    }
}