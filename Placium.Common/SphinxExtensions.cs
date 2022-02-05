using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;

namespace Placium.Common;

public static class SphinxExtensions
{
    private static readonly Regex _spaceRegex = new(@"\s+", RegexOptions.IgnoreCase);

    public static string ToMatch(this List<string> list, int near = 199)
    {
        return string.Join("<<",
            list.Where(x => !string.IsNullOrWhiteSpace(x)).Select(x =>
                $"({string.Join($" NEAR/{near} ", _spaceRegex.Split(x.Trim()).Select(y => $"\"{y.Yo().ToLower().Escape()}\""))})"));
    }
}