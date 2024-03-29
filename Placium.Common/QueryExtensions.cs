﻿using System;
using System.Collections.Generic;
using MySql.Data.MySqlClient;
using Npgsql;

namespace Placium.Common
{
    public static class QueryExtensions
    {
        public static int Fill(this List<string> list, string sql, NpgsqlConnection connection)
        {
            using var command = new NpgsqlCommand(sql, connection);
            command.Prepare();
            using var reader = command.ExecuteReader();
            return list.Fill(reader);
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


        public static int FillAll(this List<long> list, string sql, Dictionary<string, object> dictionary,
            MySqlConnection connection, int take = 100, int limit = 1000)
        {
            var total = 0;
            for (var skip = 0;; skip += take)
                try
                {
                    dictionary["skip"] = skip;
                    dictionary["take"] = take;
                    var count = list.Fill($"{sql} LIMIT @skip,@take", dictionary, connection, limit);
                    total += count;
                    limit -= count;
                    if (count < take || limit == 0) return total;
                }
                catch (Exception)
                {
                    return total;
                }
        }

        public static int FillAll(this List<string> list, string sql, Dictionary<string, object> dictionary,
            MySqlConnection connection, int take = 100, int limit = 1000)
        {
            var total = 0;
            for (var skip = 0;; skip += take)
                try
                {
                    dictionary["skip"] = skip;
                    dictionary["take"] = take;
                    var count = list.Fill($"{sql} LIMIT @skip,@take", dictionary, connection, limit);
                    total += count;
                    limit -= count;
                    if (count < take || limit == 0) return total;
                }
                catch (Exception)
                {
                    return total;
                }
        }

        public static int Fill(this List<long> list, string sql, Dictionary<string, object> dictionary,
            MySqlConnection connection, int limit)
        {
            connection.TryOpen();
            using var command = new MySqlCommand(sql, connection);
            foreach (var (key, value) in dictionary) command.Parameters.AddWithValue(key, value);
            using var reader = command.ExecuteReader();
            return Fill(list, reader, limit);
        }

        public static int Fill(this List<string> list, string sql, Dictionary<string, object> dictionary,
            MySqlConnection connection, int limit)
        {
            connection.TryOpen();
            using var command = new MySqlCommand(sql, connection);
            foreach (var (key, value) in dictionary) command.Parameters.AddWithValue(key, value);
            using var reader = command.ExecuteReader();
            return Fill(list, reader, limit);
        }

        public static int Fill(this List<long> list, MySqlDataReader reader, int limit)
        {
            var count = 0;
            for (var i = 0; i < limit && reader.Read(); i++)
            {
                list.Add(reader.GetInt64(0));
                count++;
            }

            return count;
        }

        public static int Fill(this List<string> list, MySqlDataReader reader, int limit)
        {
            var count = 0;
            for (var i = 0; i < limit && reader.Read(); i++)
            {
                list.Add(reader.GetString(0));
                count++;
            }

            return count;
        }
    }
}