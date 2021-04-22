using System.Collections.Generic;
using Npgsql;
using Placium.Common;

namespace Updater.Sphinx
{
    public static class PgExtensions
    {
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
                "addr:subdistrict",
                "addr:city",
                "addr:town",
                "addr:village",
                "addr:place",
                "addr:suburb",
                "addr:hamlet",
                "addr:street",
                "addr:housenumber"
            };

            var result = new List<Doc3>(take);
            for (var i = 0; i < take && reader.Read(); i++)
            {
                var tags = (Dictionary<string, string>) reader.GetValue(1);
                var priority = keys.Length;
                for (; priority > 0 && !tags.ContainsKey(keys[priority - 1]); priority--) ;
                var list = new List<string>(priority);
                for (var k = 0; k< priority; k++)
                    if (tags.ContainsKey(keys[k]))
                        list.Add(tags[keys[k]].Yo());

                result.Add(new Doc3
                {
                    id = reader.GetInt64(0),
                    text = string.Join(", ", list),
                    priority = priority
                });
            }

            return result;
        }
    }
}