﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using MySql.Data.MySqlClient;
using Npgsql;
using Placium.Types;

namespace Placium.Common
{
    public class BaseAppService
    {
        private readonly IConnectionsConfig _configuration;

        public BaseAppService(IConnectionsConfig configuration)
        {
            _configuration = configuration;
        }

        protected string GetSphinxConnectionString()
        {
            return _configuration.GetConnectionString("SphinxConnection");
        }

        protected string GetGarConnectionString()
        {
            return _configuration.GetConnectionString("GarConnection");
        }

        protected string GetFiasConnectionString()
        {
            return _configuration.GetConnectionString("FiasConnection");
        }

        protected string GetOsmConnectionString()
        {
            return _configuration.GetConnectionString("OsmConnection");
        }

        protected string GetRouteConnectionString()
        {
            return _configuration.GetConnectionString("RouteConnection");
        }

        protected void SelectAndExecute(string[][] sqls, NpgsqlConnection conn, string connectionString)
        {
            foreach (var sql in sqls)
            {
                var cmds = new List<string>();

                cmds.Fill(string.Join("\nUNION ALL\n", sql), conn);

                Parallel.ForEach(cmds, new ParallelOptions
                {
                    MaxDegreeOfParallelism = 4
                }, cmd =>
                {
                    using var connection = new NpgsqlConnection(connectionString);
                    connection.Open();
                    using (var command = new NpgsqlCommand(cmd, connection))
                    {
                        command.Prepare();

                        command.ExecuteNonQuery();
                    }

                    connection.Close();
                });
            }
        }

        protected void ExecuteNonQueries(string[][] sqls, NpgsqlConnection conn)
        {
            foreach (var sql in sqls)
            {
                using var command = new NpgsqlCommand(string.Join(";", sql), conn);

                command.Prepare();

                command.ExecuteNonQuery();
            }
        }


        protected async Task ExecuteResourceAsync(Assembly assembly, string resource, NpgsqlConnection connection)
        {
            await using var stream = assembly.GetManifestResourceStream(resource);
            using var sr = new StreamReader(stream, Encoding.UTF8);
            await using var command = new NpgsqlCommand(await sr.ReadToEndAsync(), connection);

            await command.PrepareAsync();

            command.ExecuteNonQuery();
        }

        protected bool TableIsExists(string tableName, NpgsqlConnection conn)
        {
            using var command = new NpgsqlCommand(
                $"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema='public' AND table_name='{tableName}');"
                , conn);

            command.Prepare();

            return (bool)command.ExecuteScalar()!;
        }

        protected long GetNextLastRecordNumber(NpgsqlConnection connection)
        {
            using var command = new NpgsqlCommand(
                "SELECT last_value FROM record_number_seq"
                , connection);

            command.Prepare();

            return (long)command.ExecuteScalar()!;
        }

        protected long GetLastRecordNumber<ServiceType>(NpgsqlConnection connection, ServiceType service_type, bool full)
        {
            if (full) return 0;

            using var command = new NpgsqlCommand(
                "SELECT last_record_number FROM service_history WHERE service_type=@service_type LIMIT 1"
                , connection);
            command.Parameters.AddWithValue("service_type", service_type);

            command.Prepare();

            using var reader = command.ExecuteReader();

            if (reader.Read())
                return reader.GetInt64(0);

            return 0;
        }

        protected void SetLastRecordNumber<ServiceType>(NpgsqlConnection connection, ServiceType service_type,
            long last_record_number)
        {
            using var command = new NpgsqlCommand(
                "INSERT INTO service_history(service_type,last_record_number) VALUES (@service_type, @last_record_number) ON CONFLICT (service_type) DO UPDATE SET last_record_number=EXCLUDED.last_record_number"
                , connection);

            command.Parameters.AddWithValue("service_type", service_type);
            command.Parameters.AddWithValue("last_record_number", last_record_number);

            command.Prepare();

            command.ExecuteNonQuery();
        }

        protected int ExecuteNonQueryWithRepeatOnError(string sql, MySqlConnection connection)
        {
            for (;;)
                try
                {
                    connection.TryOpen();
                    using var mySqlCommand = new MySqlCommand(sql, connection);
                    return mySqlCommand.ExecuteNonQuery();
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"error execute mysql command ({ex.Message}). waiting.");
                    connection.TryClose();
                    Thread.Sleep(2000);
                }
        }

        protected void TryExecuteNonQueries(string[] sqls, MySqlConnection connection)
        {
            foreach (var sql in sqls)
            {
                connection.TryOpen();
                using var command = new MySqlCommand(sql, connection);
                command.TryExecuteNonQuery();
            }
        }
    }
}