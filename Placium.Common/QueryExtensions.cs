using System;
using System.Collections.Generic;
using MySql.Data.MySqlClient;
using Npgsql;

namespace Placium.Common
{
    public static class QueryExtensions
    {
        public static int Fill(this List<string> list, string sql, NpgsqlConnection connection)
        {
            using (var command = new NpgsqlCommand(sql, connection))
            {
                command.Prepare();

                using (var reader = command.ExecuteReader())
                {
                    return list.Fill(reader);
                }
            }
        }

        public static int Fill(this List<string> list, NpgsqlDataReader reader)
        {
            var count = 0;

            while (reader.Read())
            {
                list.Add(reader.GetString(0));
                count++;
            }

            return count;
        }

        public static int FillAll(this List<long> list, string sql, MySqlConnection connection, int take = 100)
        {
            var total = 0;
            for (var skip = 0;; skip += take)
            {
                try
                {
                    var count = list.Fill($"{sql} LIMIT {skip},{take}", connection);
                    total += count;
                    if (count < take) return total;
                }
                catch (Exception)
                {
                    return total;
                }
            }
        }

        public static int Fill(this List<long> list, string sql, MySqlConnection connection)
        {
            connection.TryOpen();
            using (var command = new MySqlCommand(sql, connection))
            {
                using (var reader = command.ExecuteReader())
                {
                    return list.Fill(reader);
                }
            }
        }

        public static int Fill(this List<long> list, MySqlDataReader reader)
        {
            var count = 0;
            while (reader.Read())
            {
                list.Add(reader.GetInt64(0));
                count++;
            }

            return count;
        }
    }
}