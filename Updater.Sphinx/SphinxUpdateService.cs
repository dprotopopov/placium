using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using MySql.Data.MySqlClient;
using Npgsql;
using Placium.Common;
using Placium.Types;

namespace Updater.Sphinx
{
    public class SphinxUpdateService : IUpdateService
    {
        private readonly IConfiguration _configuration;
        private readonly ProgressHub _progressHub;

        public SphinxUpdateService(ProgressHub progressHub, IConfiguration configuration)
        {
            _progressHub = progressHub;
            _configuration = configuration;
        }

        public async Task UpdateAsync(string connectionString, string session)
        {
            using (var connection = new MySqlConnection(connectionString))
            {
                connection.TryOpen();

                using (var command = new MySqlCommand("CREATE TABLE addrob(text field)", connection))
                {
                    command.TryExecuteNonQuery();
                }

                using (var command = new MySqlCommand("CREATE TABLE placex(text field)", connection))
                {
                    command.TryExecuteNonQuery();
                }

                using (var npgsqlConnection = new NpgsqlConnection(GetFiasConnectionString()))
                {
                    var current = 0L;
                    var total = 0L;

                    var id = Guid.NewGuid().ToString();
                    await _progressHub.Init(id, session);

                    await npgsqlConnection.OpenAsync();

                    npgsqlConnection.TypeMapper.MapEnum<FiasServiceType>("service_type");

                    var last_record_number = GetLastRecordNumber(npgsqlConnection, FiasServiceType.Addrob);
                    var next_last_record_number = NextLastRecordNumber(npgsqlConnection);

                    var list = new List<string>();
                    list.Fill(
                        @"SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' and table_name similar to 'addrob\d+'",
                        npgsqlConnection);

                    var sql1 = string.Join(" UNION ",
                        list.Select(x =>
                            $"SELECT COUNT(*) FROM {x} WHERE {x}.actstatus=1 AND {x}.record_number>@last_record_number AND {x}.record_number<=@next_last_record_number"));

                    using (var npgsqlCommand = new NpgsqlCommand(sql1, npgsqlConnection))
                    {
                        npgsqlCommand.Parameters.AddWithValue("last_record_number", last_record_number);
                        npgsqlCommand.Parameters.AddWithValue("next_last_record_number", next_last_record_number);

                        using (var reader = npgsqlCommand.ExecuteReader())
                        {
                            while (reader.Read()) total += reader.GetInt64(0);
                        }
                    }

                    var sql = string.Join(" UNION ",
                        list.Select(x =>
                            $"SELECT {x}.record_number,CASE WHEN {x}.aolevel>1 THEN CONCAT (socrbase.socrname,' ', {x}.offname) ELSE {x}.offname END FROM {x} JOIN socrbase ON {x}.shortname=socrbase.scname AND {x}.aolevel=socrbase.level WHERE {x}.actstatus=1 AND {x}.record_number>@last_record_number AND {x}.record_number<=@next_last_record_number"));

                    using (var npgsqlCommand = new NpgsqlCommand(sql, npgsqlConnection))
                    {
                        npgsqlCommand.Parameters.AddWithValue("last_record_number", last_record_number);
                        npgsqlCommand.Parameters.AddWithValue("next_last_record_number", next_last_record_number);

                        var take = 1000;

                        using (var reader = npgsqlCommand.ExecuteReader())
                        {
                            while (true)
                            {
                                var docs = reader.ReadDocs(take);

                                if (docs.Any())
                                {
                                    var sb = new StringBuilder("REPLACE INTO addrob(id,text) VALUES ");
                                    sb.Append(string.Join(",", docs.Select(x => $"({x.id},'{x.text.TextEscape()}')")));
                                    using (var mySqlCommand = new MySqlCommand(sb.ToString(), connection))
                                    {
                                        mySqlCommand.ExecuteNonQuery();
                                    }

                                    current += docs.Count;

                                    await _progressHub.Progress(100f * current / total, id, session);
                                }


                                if (docs.Count < take) break;
                            }
                        }
                    }

                    SetLastRecordNumber(npgsqlConnection, FiasServiceType.Addrob, next_last_record_number);
                    await _progressHub.Progress(100f, id, session);
                }

                using (var npgsqlConnection = new NpgsqlConnection(GetOsmConnectionString()))
                {
                    var current = 0L;
                    var total = 0L;

                    var id = Guid.NewGuid().ToString();
                    await _progressHub.Init(id, session);

                    await npgsqlConnection.OpenAsync();

                    npgsqlConnection.TypeMapper.MapEnum<OsmServiceType>("service_type");

                    var last_record_number = GetLastRecordNumber(npgsqlConnection, OsmServiceType.Place);
                    var next_last_record_number = NextLastRecordNumber(npgsqlConnection);

                    var sql1 =
                        "SELECT COUNT(*) FROM place WHERE record_number>@last_record_number AND record_number<=@next_last_record_number";

                    using (var npgsqlCommand = new NpgsqlCommand(sql1, npgsqlConnection))
                    {
                        npgsqlCommand.Parameters.AddWithValue("last_record_number", last_record_number);
                        npgsqlCommand.Parameters.AddWithValue("next_last_record_number", next_last_record_number);

                        total = (long) npgsqlCommand.ExecuteScalar();
                    }

                    var sql =
                        "SELECT record_number,tags->'name' FROM place WHERE record_number>@last_record_number AND record_number<=@next_last_record_number";

                    using (var npgsqlCommand = new NpgsqlCommand(sql, npgsqlConnection))
                    {
                        npgsqlCommand.Parameters.AddWithValue("last_record_number", last_record_number);
                        npgsqlCommand.Parameters.AddWithValue("next_last_record_number", next_last_record_number);

                        var take = 1000;

                        using (var reader = npgsqlCommand.ExecuteReader())
                        {
                            while (true)
                            {
                                var docs = reader.ReadDocs(take);

                                if (docs.Any())
                                {
                                    var sb = new StringBuilder("REPLACE INTO placex(id,text) VALUES ");
                                    sb.Append(string.Join(",", docs.Select(x => $"({x.id},'{x.text.TextEscape()}')")));
                                    using (var mySqlCommand = new MySqlCommand(sb.ToString(), connection))
                                    {
                                        mySqlCommand.ExecuteNonQuery();
                                    }

                                    current += docs.Count;

                                    await _progressHub.Progress(100f * current / total, id, session);
                                }

                                if (docs.Count < take) break;
                            }
                        }
                    }

                    SetLastRecordNumber(npgsqlConnection, OsmServiceType.Place, next_last_record_number);
                    await _progressHub.Progress(100f, id, session);
                }
            }
        }

        private string GetFiasConnectionString()
        {
            return _configuration.GetConnectionString("FiasConnection");
        }

        private string GetOsmConnectionString()
        {
            return _configuration.GetConnectionString("OsmConnection");
        }

        private long GetLastRecordNumber(NpgsqlConnection connection, OsmServiceType service_type)
        {
            using (var command = new NpgsqlCommand(
                "SELECT last_record_number FROM service_history WHERE service_type=@service_type LIMIT 1"
                , connection))
            {
                command.Parameters.AddWithValue("service_type", service_type);
                using (var reader = command.ExecuteReader())
                {
                    if (reader.Read())
                        return reader.GetInt64(0);
                }
            }

            return 0;
        }

        public void SetLastRecordNumber(NpgsqlConnection connection, OsmServiceType service_type,
            long last_record_number)
        {
            using (var command = new NpgsqlCommand(
                "INSERT INTO service_history(service_type,last_record_number) VALUES (@service_type, @last_record_number) ON CONFLICT (service_type) DO UPDATE SET last_record_number=EXCLUDED.last_record_number"
                , connection))
            {
                command.Parameters.AddWithValue("service_type", service_type);
                command.Parameters.AddWithValue("last_record_number", last_record_number);
                command.ExecuteNonQuery();
            }
        }

        public void SetLastRecordNumber(NpgsqlConnection connection, FiasServiceType service_type,
            long last_record_number)
        {
            using (var command = new NpgsqlCommand(
                "INSERT INTO service_history(service_type,last_record_number) VALUES (@service_type, @last_record_number) ON CONFLICT (service_type) DO UPDATE SET last_record_number=EXCLUDED.last_record_number"
                , connection))
            {
                command.Parameters.AddWithValue("service_type", service_type);
                command.Parameters.AddWithValue("last_record_number", last_record_number);
                command.ExecuteNonQuery();
            }
        }

        private long GetLastRecordNumber(NpgsqlConnection connection, FiasServiceType service_type)
        {
            using (var command = new NpgsqlCommand(
                "SELECT last_record_number FROM service_history WHERE service_type=@service_type LIMIT 1"
                , connection))
            {
                command.Parameters.AddWithValue("service_type", service_type);
                using (var reader = command.ExecuteReader())
                {
                    if (reader.Read())
                        return reader.GetInt64(0);
                }
            }

            return 0;
        }

        private long NextLastRecordNumber(NpgsqlConnection connection)
        {
            using (var command = new NpgsqlCommand(
                "SELECT last_value FROM record_number_seq"
                , connection))
            {
                using (var reader = command.ExecuteReader())
                {
                    if (reader.Read())
                        return reader.GetInt64(0);
                }
            }

            return 0;
        }
    }
}