using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;

namespace Placium.Common
{
    public static class SphinxExtensions
    {
        private static readonly Regex _spaceRegex = new Regex(@"\s+", RegexOptions.IgnoreCase);

        public static string ToMatch(this List<string> list)
        {
            return string.Join("<<",
                list.Where(x => !string.IsNullOrWhiteSpace(x)).Select(x =>
                    $"({string.Join(" NEAR/9 ", _spaceRegex.Split(x.Trim()).Select(y => $"\"{y.Yo().ToLower().Escape()}\""))})"));
        }
    }
}