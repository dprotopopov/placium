using System.Collections.Generic;
using Npgsql;

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
                    text = reader.GetString(1)
                });
            return result;
        }
    }
}