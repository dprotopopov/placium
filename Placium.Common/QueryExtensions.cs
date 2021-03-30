using System.Collections.Generic;
using MySql.Data.MySqlClient;
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
                list.Fill(reader);
            }
        }

        public static void Fill(this List<string> list, NpgsqlDataReader reader)
        {
            while (reader.Read())
                list.Add(reader.GetString(0));
        }
        public static void Fill(this List<long> list, string sql, MySqlConnection connection)
        {
            using (var command = new MySqlCommand(sql, connection))
            using (var reader = command.ExecuteReader())
            {
                list.Fill(reader);
            }
        }
        public static void Fill(this List<long> list, MySqlDataReader reader)
        {
            while (reader.Read())
                list.Add(reader.GetInt64(0));
        }
    }
}