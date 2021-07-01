using System.Collections.Generic;
using System.Text.RegularExpressions;
using Npgsql;
using Placium.Common;

namespace Updater.Sphinx
{
    public static class PgExtensions
    {
        private static readonly Regex _pointRegex = new Regex(
            @"POINT\s*\(\s*(?<lon>\d+(\.\d+)?)\s+(?<lat>\d+(\.\d+)?)\s*\)",
            RegexOptions.IgnoreCase);

        public static List<Doc> ReadDocs(this NpgsqlDataReader reader, int take)
        {
            var result = new List<Doc>(take);
            for (var i = 0; i < take && reader.Read(); i++)
                result.Add(new Doc
                {
                    id = reader.GetInt64(0),
                    text = reader.SafeGetString(1)
                });
            return result;
        }

        public static List<Doc1> ReadDocs1(this NpgsqlDataReader reader, int take)
        {
            var result = new List<Doc1>(take);
            for (var i = 0; i < take && reader.Read(); i++)
                result.Add(new Doc1
                {
                    id = reader.GetInt64(0),
                    text = reader.SafeGetString(1),
                    parentguid = reader.SafeGetString(2)
                });
            return result;
        }

        public static List<Doc3> ReadDocs3(this NpgsqlDataReader reader, int take)
        {
            var keys = new[]
            {
                "addr:region",
                "addr:district",
                "addr:city",
                "addr:town",
                "addr:village",
                "addr:subdistrict",
                "addr:suburb",
                "addr:hamlet",
                "addr:place",
                "addr:street",
                "addr:housenumber"
            };

            var result = new List<Doc3>(take);
            for (var i = 0; i < take && reader.Read(); i++)
            {
                var dictionary = (Dictionary<string, string>) reader.GetValue(1);

                var priority = keys.Length;
                for (; priority > 0 && !dictionary.ContainsKey(keys[priority - 1]); priority--) ;

                var list = new List<string>(priority);

                var skipCity = dictionary.ContainsKey("addr:region") && dictionary.ContainsKey("addr:city") &&
                               dictionary["addr:region"] == dictionary["addr:city"];

                var skipTown = dictionary.ContainsKey("addr:city") && dictionary.ContainsKey("addr:town") &&
                               dictionary["addr:city"] == dictionary["addr:town"];

                var skipVillage = dictionary.ContainsKey("addr:city") && dictionary.ContainsKey("addr:village") &&
                                  dictionary["addr:city"] == dictionary["addr:village"];

                for (var k = 0; k < priority; k++)
                {
                    var key = keys[k];
                    if (dictionary.ContainsKey(key) && (key != "addr:city" || !skipCity) &&
                        (key != "addr:town" || !skipTown) &&
                        (key != "addr:village" || !skipVillage))
                        list.Add(dictionary[key].Yo());
                }

                var doc = new Doc3
                {
                    id = reader.GetInt64(0),
                    text = string.Join(", ", list),
                    priority = priority
                };

                var match = _pointRegex.Match(reader.SafeGetString(2));
                if (match.Success)
                {
                    doc.lon = match.Groups["lon"].Value;
                    doc.lat = match.Groups["lat"].Value;
                }

                result.Add(doc);
            }

            return result;
        }
    }
}