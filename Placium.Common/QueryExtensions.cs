using System.Collections.Generic;
using Npgsql;

namespace Placium.Common
{
    public static class QueryExtensions
    {
        public static void Fill(this List<string> list, string sql, NpgsqlConnection connection)
        {
            using (var command = new NpgsqlCommand(sql, connection))
            using (var reader = command.ExecuteReader())
            {
                while (reader.Read())
                    list.Add(reader.GetString(0));
            }
        }
    }
}