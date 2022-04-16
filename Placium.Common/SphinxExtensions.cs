using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;

namespace Placium.Common
{
    public static class SphinxExtensions
    {
        private static readonly Regex SpaceRegex = new Regex(@"\s+", RegexOptions.IgnoreCase);
        private static readonly MatchEvaluator ZeroPadLeftEvaluator = ZeroPadLeft;

        public static string ToMatch(this List<string> list, int near = 199)
        {
            return string.Join("<<",
                list.Where(x => !string.IsNullOrWhiteSpace(x)).Select(x =>
                    $"({string.Join($" NEAR/{near} ", SpaceRegex.Split(x.Trim()).Select(y => $"\"{y.Trim('*').Yo().ToLower().Escape()}*\""))})"));
        }

        public static string ToSorting(this string s)
        {
            return Regex.Replace(s, @"\d+", ZeroPadLeftEvaluator);
        }

        private static string ZeroPadLeft(Match match)
        {
            return match.Value.PadLeft(12, '0');
        }
    }
}