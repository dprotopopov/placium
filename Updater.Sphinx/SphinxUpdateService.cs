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
    public class SphinxUpdateService : BaseService, IUpdateService
    {
        private readonly ProgressHub _progressHub;

        public SphinxUpdateService(ProgressHub progressHub, IConfiguration configuration) : base(configuration)
        {
            _progressHub = progressHub;
        }

        public async Task UpdateAsync(string session)
        {
            using (var connection = new MySqlConnection(GetSphinxConnectionString()))
            {
                TryExecuteNonQueries(new[]
                {
                    "CREATE TABLE addrob(text field)",
                    "CREATE TABLE house(text field)",
                    "CREATE TABLE stead(text field)",
                    "CREATE TABLE placex(text field)"
                }, connection);

                await UpdateAddrobAsync(connection, session);
                await UpdateHouseAsync(connection, session);
                await UpdateSteadAsync(connection, session);
                await UpdatePlacexAsync(connection, session);
            }
        }


        private void TryExecuteNonQueries(string[] sqls, MySqlConnection connection)
        {
            connection.TryOpen();
            foreach (var sql in sqls)
                using (var command = new MySqlCommand(sql, connection))
                {
                    command.TryExecuteNonQuery();
                }
        }

        private async Task UpdateAddrobAsync(MySqlConnection connection, string session)
        {
            var socr = true;
            var formal = false;

            using (var npgsqlConnection = new NpgsqlConnection(GetFiasConnectionString()))
            {
                var current = 0L;
                var total = 0L;

                var id = Guid.NewGuid().ToString();
                await _progressHub.InitAsync(id, session);

                await npgsqlConnection.OpenAsync();

                npgsqlConnection.ReloadTypes();
                npgsqlConnection.TypeMapper.MapEnum<FiasServiceType>("service_type");

                var last_record_number = GetLastRecordNumber(npgsqlConnection, FiasServiceType.Addrob);
                var next_last_record_number = GetNextLastRecordNumber(npgsqlConnection);

                var list = new List<string>();
                list.Fill(
                    @"SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' and table_name similar to 'addrob\d+'",
                    npgsqlConnection);

                var sql1 = string.Join("\nUNION ALL\n",
                    list.Select(x =>
                        $"SELECT COUNT(1) FROM {x} WHERE {x}.actstatus=1 AND {x}.record_number>@last_record_number AND {x}.record_number<=@next_last_record_number"));

                using (var npgsqlCommand = new NpgsqlCommand(sql1, npgsqlConnection))
                {
                    npgsqlCommand.Parameters.AddWithValue("last_record_number", last_record_number);
                    npgsqlCommand.Parameters.AddWithValue("next_last_record_number", next_last_record_number);

                    using (var reader = npgsqlCommand.ExecuteReader())
                    {
                        while (reader.Read()) total += reader.GetInt64(0);
                    }
                }

                var sql = string.Join("\nUNION ALL\n",
                    list.Select(x =>
                        $"SELECT {x}.record_id,offname,formalname,shortname,socrbase.socrname,aolevel FROM {x} JOIN socrbase ON {x}.shortname=socrbase.scname AND {x}.aolevel=socrbase.level WHERE {x}.actstatus=1 AND {x}.record_number>@last_record_number AND {x}.record_number<=@next_last_record_number"));

                using (var npgsqlCommand = new NpgsqlCommand(sql, npgsqlConnection))
                {
                    npgsqlCommand.Parameters.AddWithValue("last_record_number", last_record_number);
                    npgsqlCommand.Parameters.AddWithValue("next_last_record_number", next_last_record_number);

                    var take = 10000;

                    using (var reader = npgsqlCommand.ExecuteReader())
                    {
                        while (true)
                        {
                            var docs = new List<Doc>(take);

                            for (var i = 0; i < take && reader.Read(); i++)
                            {
                                var offname = reader.SafeGetString(1);
                                var formalname = reader.SafeGetString(2);
                                var shortname = reader.SafeGetString(3);
                                var socrname = reader.SafeGetString(4);
                                var aolevel = reader.GetInt32(5);
                                var title = aolevel > 1
                                    ? $"{(socr ? socrname : shortname)} {(formal ? formalname : offname)}"
                                    : formal
                                        ? formalname
                                        : offname;
                                docs.Add(new Doc
                                {
                                    id = reader.GetInt64(0),
                                    text = title
                                });
                            }


                            if (docs.Any())
                            {
                                var sb = new StringBuilder("REPLACE INTO addrob(id,text) VALUES ");
                                sb.Append(string.Join(",", docs.Select(x => $"({x.id},'{x.text.TextEscape()}')")));

                                ExecuteNonQueryWithRepeatOnError(sb.ToString(), connection);

                                current += docs.Count;

                                await _progressHub.ProgressAsync(100f * current / total, id, session);
                            }


                            if (docs.Count < take) break;
                        }
                    }
                }

                SetLastRecordNumber(npgsqlConnection, FiasServiceType.Addrob, next_last_record_number);

                await npgsqlConnection.CloseAsync();

                await _progressHub.ProgressAsync(100f, id, session);
            }
        }

        private async Task UpdateHouseAsync(MySqlConnection connection, string session)
        {
            using (var npgsqlConnection = new NpgsqlConnection(GetFiasConnectionString()))
            using (var npgsqlConnection2 = new NpgsqlConnection(GetFiasConnectionString()))
            {
                var current = 0L;
                var total = 0L;

                var id = Guid.NewGuid().ToString();
                await _progressHub.InitAsync(id, session);

                await npgsqlConnection.OpenAsync();
                await npgsqlConnection2.OpenAsync();

                npgsqlConnection.ReloadTypes();
                npgsqlConnection.TypeMapper.MapEnum<FiasServiceType>("service_type");

                var last_record_number = GetLastRecordNumber(npgsqlConnection, FiasServiceType.House);
                var next_last_record_number = GetNextLastRecordNumber(npgsqlConnection);

                var list2 = new List<string>();
                list2.Fill(
                    @"SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' and table_name similar to 'addrob\d+'",
                    npgsqlConnection);

                var sql2 = string.Join("\nUNION ALL\n",
                    list2.Select(x =>
                        $"SELECT {x}.aoguid,offname,formalname,shortname,socrbase.socrname,aolevel FROM {x} JOIN socrbase ON {x}.shortname=socrbase.scname AND {x}.aolevel=socrbase.level WHERE {x}.aoguid=ANY(@guids) AND {x}.actstatus=1"));

                var list = new List<string>();
                list.Fill(
                    @"SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' and table_name similar to 'house\d+'",
                    npgsqlConnection);

                var sql1 = string.Join("\nUNION ALL\n",
                    list.Select(x =>
                        $"SELECT COUNT(1) FROM {x} WHERE record_number>@last_record_number AND record_number<=@next_last_record_number"));

                using (var npgsqlCommand = new NpgsqlCommand(sql1, npgsqlConnection))
                {
                    npgsqlCommand.Parameters.AddWithValue("last_record_number", last_record_number);
                    npgsqlCommand.Parameters.AddWithValue("next_last_record_number", next_last_record_number);

                    using (var reader = npgsqlCommand.ExecuteReader())
                    {
                        while (reader.Read()) total += reader.GetInt64(0);
                    }
                }

                var sql = string.Join("\nUNION ALL\n",
                    list.Select(x =>
                        $"SELECT record_id,housenum,buildnum,strucnum,aoguid FROM {x} WHERE record_number>@last_record_number AND record_number<=@next_last_record_number"));

                using (var npgsqlCommand = new NpgsqlCommand(sql, npgsqlConnection))
                {
                    npgsqlCommand.Parameters.AddWithValue("last_record_number", last_record_number);
                    npgsqlCommand.Parameters.AddWithValue("next_last_record_number", next_last_record_number);

                    var take = 10000;

                    using (var reader = npgsqlCommand.ExecuteReader())
                    {
                        while (true)
                        {
                            var docs1 = new List<Doc1>(take);

                            for (var i = 0; i < take && reader.Read(); i++)
                            {
                                var list1 = new List<string>();
                                var housenum = reader.SafeGetString(1);
                                var buildnum = reader.SafeGetString(2);
                                var strucnum = reader.SafeGetString(3);
                                var parentguid = reader.SafeGetString(4);
                                if (!string.IsNullOrEmpty(housenum)) list1.Add($"{housenum}");
                                if (!string.IsNullOrEmpty(buildnum)) list1.Add($"к{buildnum}");
                                if (!string.IsNullOrEmpty(strucnum)) list1.Add($"с{strucnum}");
                                docs1.Add(new Doc1
                                {
                                    id = reader.GetInt64(0),
                                    text = string.Join(" ", list1),
                                    parentguid = parentguid
                                });
                            }

                            if (docs1.Any())
                            {
                                var guids = docs1.Select(x => x.parentguid).ToArray();

                                var docs2 = GetDocs2(guids, sql2, npgsqlConnection2);

                                var q = from doc1 in docs1
                                    join doc2 in docs2 on doc1.parentguid equals doc2.guid
                                    select new {doc1.id, text = $"{doc2.text} HOUSE{doc1.text}"};

                                var sb = new StringBuilder("REPLACE INTO house(id,text) VALUES ");
                                sb.Append(string.Join(",", q.Select(x => $"({x.id},'{x.text.TextEscape()}')")));

                                ExecuteNonQueryWithRepeatOnError(sb.ToString(), connection);

                                current += docs1.Count;

                                await _progressHub.ProgressAsync(100f * current / total, id, session);
                            }


                            if (docs1.Count < take) break;
                        }
                    }
                }

                SetLastRecordNumber(npgsqlConnection, FiasServiceType.House, next_last_record_number);

                await npgsqlConnection2.CloseAsync();
                await npgsqlConnection.CloseAsync();

                await _progressHub.ProgressAsync(100f, id, session);
            }
        }

        private List<Doc2> GetDocs2(string[] guids, string sql2, NpgsqlConnection npgsqlConnection2)
        {
            var socr = true;
            var formal = false;

            var docs2 = new List<Doc2>();
            using (var npgsqlCommand2 = new NpgsqlCommand(sql2, npgsqlConnection2))
            {
                npgsqlCommand2.Parameters.AddWithValue("guids", guids);

                using (var reader2 = npgsqlCommand2.ExecuteReader())
                {
                    while (reader2.Read())
                    {
                        var offname = reader2.SafeGetString(1);
                        var formalname = reader2.SafeGetString(2);
                        var shortname = reader2.SafeGetString(3);
                        var socrname = reader2.SafeGetString(4);
                        var aolevel = reader2.GetInt32(5);
                        var title = aolevel > 1
                            ? $"{(socr ? socrname : shortname)} {(formal ? formalname : offname)}"
                            : formal
                                ? formalname
                                : offname;
                        docs2.Add(new Doc2
                        {
                            guid = reader2.SafeGetString(0),
                            text = title
                        });
                    }
                }
            }

            return docs2;
        }

        private async Task UpdateSteadAsync(MySqlConnection connection, string session)
        {
            using (var npgsqlConnection = new NpgsqlConnection(GetFiasConnectionString()))
            using (var npgsqlConnection2 = new NpgsqlConnection(GetFiasConnectionString()))
            {
                var current = 0L;
                var total = 0L;

                var id = Guid.NewGuid().ToString();
                await _progressHub.InitAsync(id, session);

                await npgsqlConnection.OpenAsync();
                await npgsqlConnection2.OpenAsync();

                npgsqlConnection.ReloadTypes();
                npgsqlConnection.TypeMapper.MapEnum<FiasServiceType>("service_type");

                var last_record_number = GetLastRecordNumber(npgsqlConnection, FiasServiceType.Stead);
                var next_last_record_number = GetNextLastRecordNumber(npgsqlConnection);

                var list2 = new List<string>();
                list2.Fill(
                    @"SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' and table_name similar to 'addrob\d+'",
                    npgsqlConnection);

                var sql2 = string.Join("\nUNION ALL\n",
                    list2.Select(x =>
                        $"SELECT {x}.aoguid,offname,formalname,shortname,socrbase.socrname,aolevel FROM {x} JOIN socrbase ON {x}.shortname=socrbase.scname AND {x}.aolevel=socrbase.level WHERE {x}.aoguid=ANY(@guids) AND {x}.actstatus=1"));

                var list = new List<string>();
                list.Fill(
                    @"SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' and table_name similar to 'stead\d+'",
                    npgsqlConnection);

                var sql1 = string.Join("\nUNION ALL\n",
                    list.Select(x =>
                        $"SELECT COUNT(1) FROM {x} WHERE record_number>@last_record_number AND record_number<=@next_last_record_number"));

                using (var npgsqlCommand = new NpgsqlCommand(sql1, npgsqlConnection))
                {
                    npgsqlCommand.Parameters.AddWithValue("last_record_number", last_record_number);
                    npgsqlCommand.Parameters.AddWithValue("next_last_record_number", next_last_record_number);

                    using (var reader = npgsqlCommand.ExecuteReader())
                    {
                        while (reader.Read()) total += reader.GetInt64(0);
                    }
                }

                var sql = string.Join("\nUNION ALL\n",
                    list.Select(x =>
                        $"SELECT record_id,number,parentguid FROM {x} WHERE record_number>@last_record_number AND record_number<=@next_last_record_number"));

                using (var npgsqlCommand = new NpgsqlCommand(sql, npgsqlConnection))
                {
                    npgsqlCommand.Parameters.AddWithValue("last_record_number", last_record_number);
                    npgsqlCommand.Parameters.AddWithValue("next_last_record_number", next_last_record_number);

                    var take = 10000;

                    using (var reader = npgsqlCommand.ExecuteReader())
                    {
                        while (true)
                        {
                            var docs1 = reader.ReadDocs1(take);

                            if (docs1.Any())
                            {
                                var guids = docs1.Select(x => x.parentguid).ToArray();

                                var docs2 = GetDocs2(guids, sql2, npgsqlConnection2);

                                var q = from doc1 in docs1
                                    join doc2 in docs2 on doc1.parentguid equals doc2.guid
                                    select new {doc1.id, text = $"{doc2.text} STEAD{doc1.text}"};

                                var sb = new StringBuilder("REPLACE INTO stead(id,text) VALUES ");
                                sb.Append(string.Join(",", q.Select(x => $"({x.id},'{x.text.TextEscape()}')")));

                                ExecuteNonQueryWithRepeatOnError(sb.ToString(), connection);

                                current += docs1.Count;

                                await _progressHub.ProgressAsync(100f * current / total, id, session);
                            }


                            if (docs1.Count < take) break;
                        }
                    }
                }

                SetLastRecordNumber(npgsqlConnection, FiasServiceType.Stead, next_last_record_number);

                await npgsqlConnection2.CloseAsync();
                await npgsqlConnection.CloseAsync();

                await _progressHub.ProgressAsync(100f, id, session);
            }
        }

        private async Task UpdatePlacexAsync(MySqlConnection connection, string session)
        {
            using (var npgsqlConnection = new NpgsqlConnection(GetOsmConnectionString()))
            {
                var current = 0L;
                var total = 0L;

                var id = Guid.NewGuid().ToString();
                await _progressHub.InitAsync(id, session);

                await npgsqlConnection.OpenAsync();

                npgsqlConnection.ReloadTypes();
                npgsqlConnection.TypeMapper.MapEnum<OsmServiceType>("service_type");

                var last_record_number = GetLastRecordNumber(npgsqlConnection, OsmServiceType.Place);
                var next_last_record_number = GetNextLastRecordNumber(npgsqlConnection);

                var sql1 =
                    "SELECT COUNT(1) FROM place WHERE tags?'name' AND record_number>@last_record_number AND record_number<=@next_last_record_number";

                using (var npgsqlCommand = new NpgsqlCommand(sql1, npgsqlConnection))
                {
                    npgsqlCommand.Parameters.AddWithValue("last_record_number", last_record_number);
                    npgsqlCommand.Parameters.AddWithValue("next_last_record_number", next_last_record_number);

                    total = (long) npgsqlCommand.ExecuteScalar();
                }

                var sql =
                    "SELECT record_id,tags->'name' FROM place WHERE tags?'name' AND record_number>@last_record_number AND record_number<=@next_last_record_number";

                using (var npgsqlCommand = new NpgsqlCommand(sql, npgsqlConnection))
                {
                    npgsqlCommand.Parameters.AddWithValue("last_record_number", last_record_number);
                    npgsqlCommand.Parameters.AddWithValue("next_last_record_number", next_last_record_number);

                    var take = 10000;

                    using (var reader = npgsqlCommand.ExecuteReader())
                    {
                        while (true)
                        {
                            var docs = reader.ReadDocs(take);

                            if (docs.Any())
                            {
                                var sb = new StringBuilder("REPLACE INTO placex(id,text) VALUES ");
                                sb.Append(string.Join(",", docs.Select(x => $"({x.id},'{x.text.TextEscape()}')")));

                                ExecuteNonQueryWithRepeatOnError(sb.ToString(), connection);

                                current += docs.Count;

                                await _progressHub.ProgressAsync(100f * current / total, id, session);
                            }

                            if (docs.Count < take) break;
                        }
                    }
                }

                SetLastRecordNumber(npgsqlConnection, OsmServiceType.Place, next_last_record_number);

                await npgsqlConnection.CloseAsync();

                await _progressHub.ProgressAsync(100f, id, session);
            }
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


        private int ExecuteNonQueryWithRepeatOnError(string sql, MySqlConnection connection)
        {
            while (true)
                try
                {
                    connection.TryOpen();
                    using (var mySqlCommand = new MySqlCommand(sql, connection))
                    {
                        return mySqlCommand.ExecuteNonQuery();
                    }
                }
                catch (Exception)
                {
                }
        }
    }
}