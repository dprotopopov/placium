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
    }
}