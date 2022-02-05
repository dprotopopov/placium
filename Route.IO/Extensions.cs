using System;
using System.Globalization;

namespace Route.IO;

public static class Extensions
{
    /// <summary>
    ///     Returns a string representing the object in a culture invariant way.
    /// </summary>
    public static string ToInvariantString(this object obj)
    {
        return obj is IConvertible ? ((IConvertible)obj).ToString(CultureInfo.InvariantCulture)
            : obj is IFormattable ? ((IFormattable)obj).ToString(null, CultureInfo.InvariantCulture)
            : obj.ToString();
    }
}