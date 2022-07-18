using System;

namespace Placium.Common
{
    public static class OsmExtensions
    {
        public static string ToOsmType(this int i)
        {
            return i switch
            {
                0 => "node",
                1 => "way", 
                2 => "relation",
                _ => throw new ArgumentOutOfRangeException(nameof(i), i, null)
            };
        }
    }
}