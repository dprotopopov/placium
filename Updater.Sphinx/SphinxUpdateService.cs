using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using MySql.Data.MySqlClient;
using Npgsql;
using NpgsqlTypes;
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
                    "CREATE TABLE addrob(title text)",
                    "CREATE TABLE house(title text)",
                    "CREATE TABLE stead(title text)",
                    "CREATE TABLE placex(title text)",
                    "CREATE TABLE addrx(title text,priority int)"
                }, connection);

                await UpdateAddrobAsync(connection, session);
                await UpdateHouseAsync(connection, session);
                await UpdateSteadAsync(connection, session);
                await UpdatePlacexAsync(connection, session);
                await UpdateAddrxAsync(connection, session);
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

                var last_record_number = GetLastRecordNumber(npgsqlConnection, FiasServiceType.Addrob);
                var next_last_record_number = GetNextLastRecordNumber(npgsqlConnection);

                var list = new List<string>();
                list.Fill(
                    @"SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' and table_name similar to 'addrob\d+'",
                    npgsqlConnection);

                var sql2 = string.Join("\nUNION ALL\n",
                    list.Select(x =>
                        $@"SELECT {x}.aoguid,offname,formalname,shortname,socrbase.socrname,aolevel,parentguid FROM {x}
                        JOIN socrbase ON {x}.shortname=socrbase.scname AND {x}.aolevel=socrbase.level
                        WHERE {x}.aoguid=ANY(@guids) AND {x}.livestatus=1"));

                var sql1 = string.Join("\nUNION ALL\n",
                    list.Select(x =>
                        $"SELECT COUNT(*) FROM {x} WHERE {x}.livestatus=1 AND {x}.record_number>@last_record_number"));

                var sql = string.Join("\nUNION ALL\n",
                    list.Select(x =>
                        $@"SELECT {x}.record_id,offname,formalname,shortname,socrbase.socrname,aolevel,parentguid FROM {x}
                        JOIN socrbase ON {x}.shortname=socrbase.scname AND {x}.aolevel=socrbase.level
                        WHERE {x}.livestatus=1 AND {x}.record_number>@last_record_number"));

                using (var command = new NpgsqlCommand(string.Join(";", sql1, sql), npgsqlConnection))
                using (var command2 = new NpgsqlCommand(sql2, npgsqlConnection2))
                {
                    command.Parameters.AddWithValue("last_record_number", last_record_number);

                    command.Prepare();

                    command2.Parameters.Add("guids", NpgsqlDbType.Array | NpgsqlDbType.Varchar);

                    command2.Prepare();

                    using (var reader = command.ExecuteReader())
                    {
                        while (reader.Read()) total += reader.GetInt64(0);

                        var take = 10000;

                        reader.NextResult();

                        while (true)
                        {
                            var docs1 = new List<Doc1>(take);

                            for (var i = 0; i < take && reader.Read(); i++)
                            {
                                var offname = reader.SafeGetString(1);
                                var formalname = reader.SafeGetString(2);
                                var shortname = reader.SafeGetString(3);
                                var socrname = reader.SafeGetString(4);
                                var aolevel = reader.GetInt32(5);
                                var parentguid = reader.SafeGetString(6);
                                var title = aolevel > 1
                                    ? $"{(socr ? socrname : shortname)} {(formal ? formalname : offname)}"
                                    : formal
                                        ? formalname
                                        : offname;
                                docs1.Add(new Doc1
                                {
                                    id = reader.GetInt64(0),
                                    text = title,
                                    parentguid = parentguid
                                });
                            }


                            if (docs1.Any())
                            {
                                var guids = docs1.Select(x => x.parentguid).ToArray();

                                var docs2 = GetDocs2(guids, command2, take);

                                var q = from doc1 in docs1
                                    join doc2 in docs2 on doc1.parentguid equals doc2.guid into ps
                                    from doc in ps.DefaultIfEmpty()
                                    select new {doc1.id, text = $"#{doc1.text} @{doc?.text ?? doc1.text}"};

                                var sb = new StringBuilder("REPLACE INTO addrob(id,title) VALUES ");
                                sb.Append(string.Join(",", q.Select(x => $"({x.id},'{x.text.TextEscape()}')")));

                                ExecuteNonQueryWithRepeatOnError(sb.ToString(), connection);

                                current += docs1.Count;

                                await _progressHub.ProgressAsync(100f * current / total, id, session);
                            }

                            if (docs1.Count < take) break;
                        }
                    }
                }

                SetLastRecordNumber(npgsqlConnection, FiasServiceType.Addrob, next_last_record_number);

                await npgsqlConnection2.CloseAsync();
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
                var list = new List<string>();

                using (var command = new NpgsqlCommand(
                    string.Join(";", new[] {@"addrob\d+", @"house\d+"}.Select(x =>
                        $"SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' and table_name similar to '{x}'")),
                    npgsqlConnection))
                {
                    command.Prepare();

                    using (var reader = command.ExecuteReader())
                    {
                        list2.Fill(reader);
                        reader.NextResult();
                        list.Fill(reader);
                    }
                }

                var sql2 = string.Join("\nUNION ALL\n",
                    list2.Select(x =>
                        $@"SELECT {x}.aoguid,offname,formalname,shortname,socrbase.socrname,aolevel,parentguid FROM {x}
                        JOIN socrbase ON {x}.shortname=socrbase.scname AND {x}.aolevel=socrbase.level
                        WHERE {x}.aoguid=ANY(@guids) AND {x}.livestatus=1"));

                var sql1 = string.Join("\nUNION ALL\n",
                    list.Select(x =>
                        $"SELECT COUNT(*) FROM {x} WHERE {x}.record_number>@last_record_number AND startdate<=now() AND now()<enddate"));

                var sql = string.Join("\nUNION ALL\n",
                    list.Select(x =>
                        $@"SELECT {x}.record_id,housenum,buildnum,strucnum,eststat.name,aoguid FROM {x}
                        JOIN eststat ON {x}.eststatus=eststat.eststatid
                        WHERE {x}.record_number>@last_record_number AND startdate<=now() AND now()<enddate"));

                using (var command = new NpgsqlCommand(string.Join(";", sql1, sql), npgsqlConnection))
                using (var command2 = new NpgsqlCommand(sql2, npgsqlConnection2))
                {
                    command.Parameters.AddWithValue("last_record_number", last_record_number);

                    command.Prepare();

                    command2.Parameters.Add("guids", NpgsqlDbType.Array | NpgsqlDbType.Varchar);

                    command2.Prepare();

                    using (var reader = command.ExecuteReader())
                    {
                        while (reader.Read()) total += reader.GetInt64(0);

                        var take = 10000;

                        reader.NextResult();

                        while (true)
                        {
                            var docs1 = new List<Doc1>(take);

                            for (var i = 0; i < take && reader.Read(); i++)
                            {
                                var list1 = new List<string>();
                                var housenum = reader.SafeGetString(1);
                                var buildnum = reader.SafeGetString(2);
                                var strucnum = reader.SafeGetString(3);
                                var name = reader.SafeGetString(4);
                                var parentguid = reader.SafeGetString(5);
                                if (!string.IsNullOrEmpty(housenum)) list1.Add($"{housenum}");
                                if (!string.IsNullOrEmpty(buildnum)) list1.Add($"к{buildnum}");
                                if (!string.IsNullOrEmpty(strucnum)) list1.Add($"с{strucnum}");
                                list1.Add(name);
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

                                var docs2 = GetDocs2(guids, command2, guids.Length);

                                var guids2 = docs2.Select(x => x.parentguid).ToArray();

                                var docs3 = GetDocs2(guids2, command2, guids2.Length);

                                var q = from doc1 in docs1
                                    join doc2 in docs2 on doc1.parentguid equals doc2.guid
                                    join doc3 in docs3 on doc2.parentguid equals doc3.guid into ps
                                    from doc in ps.DefaultIfEmpty()
                                    select new {doc1.id, text = $"#{doc1.text} @{doc2.text} @{doc?.text ?? doc2.text}"};

                                var sb = new StringBuilder("REPLACE INTO house(id,title) VALUES ");
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
                var list = new List<string>();

                using (var command = new NpgsqlCommand(
                    string.Join(";", new[] {@"addrob\d+", @"stead\d+"}.Select(x =>
                        $"SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' and table_name similar to '{x}'")),
                    npgsqlConnection))
                {
                    command.Prepare();

                    using (var reader = command.ExecuteReader())
                    {
                        list2.Fill(reader);
                        reader.NextResult();
                        list.Fill(reader);
                    }
                }

                var sql2 = string.Join("\nUNION ALL\n",
                    list2.Select(x =>
                        $@"SELECT {x}.aoguid,offname,formalname,shortname,socrbase.socrname,aolevel,parentguid FROM {x}
                        JOIN socrbase ON {x}.shortname=socrbase.scname AND {x}.aolevel=socrbase.level
                        WHERE {x}.aoguid=ANY(@guids) AND {x}.livestatus=1"));

                var sql1 = string.Join("\nUNION ALL\n",
                    list.Select(x =>
                        $"SELECT COUNT(*) FROM {x} WHERE {x}.record_number>@last_record_number AND livestatus=1"));

                var sql = string.Join("\nUNION ALL\n",
                    list.Select(x =>
                        $"SELECT {x}.record_id,number,parentguid FROM {x} WHERE {x}.record_number>@last_record_number AND livestatus=1"));

                using (var command = new NpgsqlCommand(string.Join(";", sql1, sql), npgsqlConnection))
                using (var command2 = new NpgsqlCommand(sql2, npgsqlConnection2))
                {
                    command.Parameters.AddWithValue("last_record_number", last_record_number);

                    command.Prepare();

                    command2.Parameters.Add("guids", NpgsqlDbType.Array | NpgsqlDbType.Varchar);

                    command2.Prepare();

                    using (var reader = command.ExecuteReader())
                    {
                        while (reader.Read()) total += reader.GetInt64(0);

                        var take = 10000;

                        reader.NextResult();

                        while (true)
                        {
                            var docs1 = reader.ReadDocs1(take);

                            if (docs1.Any())
                            {
                                var guids = docs1.Select(x => x.parentguid).ToArray();

                                var docs2 = GetDocs2(guids, command2, guids.Length);

                                var guids2 = docs2.Select(x => x.parentguid).ToArray();

                                var docs3 = GetDocs2(guids2, command2, guids2.Length);

                                var q = from doc1 in docs1
                                    join doc2 in docs2 on doc1.parentguid equals doc2.guid
                                    join doc3 in docs3 on doc2.parentguid equals doc3.guid into ps
                                    from doc in ps.DefaultIfEmpty()
                                    select new {doc1.id, text = $"#{doc1.text} @{doc2.text} @{doc?.text ?? doc2.text}"};

                                var sb = new StringBuilder("REPLACE INTO stead(id,title) VALUES ");
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

                var last_record_number = GetLastRecordNumber(npgsqlConnection, OsmServiceType.Placex);
                var next_last_record_number = GetNextLastRecordNumber(npgsqlConnection);

                var sql1 =
                    "SELECT COUNT(*) FROM placex WHERE tags?'name' AND record_number>@last_record_number";

                var sql =
                    "SELECT record_id,tags->'name' FROM placex WHERE tags?'name' AND record_number>@last_record_number";

                using (var command = new NpgsqlCommand(string.Join(";", sql1, sql), npgsqlConnection))
                {
                    command.Parameters.AddWithValue("last_record_number", last_record_number);

                    command.Prepare();

                    using (var reader = command.ExecuteReader())
                    {
                        if (reader.Read())
                            total = reader.GetInt64(0);

                        var take = 10000;

                        reader.NextResult();

                        while (true)
                        {
                            var docs = reader.ReadDocs(take);

                            if (docs.Any())
                            {
                                var sb = new StringBuilder("REPLACE INTO placex(id,title) VALUES ");
                                sb.Append(string.Join(",", docs.Select(x => $"({x.id},'{x.text.TextEscape()}')")));

                                ExecuteNonQueryWithRepeatOnError(sb.ToString(), connection);

                                current += docs.Count;

                                await _progressHub.ProgressAsync(100f * current / total, id, session);
                            }

                            if (docs.Count < take) break;
                        }
                    }
                }

                SetLastRecordNumber(npgsqlConnection, OsmServiceType.Placex, next_last_record_number);

                await npgsqlConnection.CloseAsync();

                await _progressHub.ProgressAsync(100f, id, session);
            }
        }

        private async Task UpdateAddrxAsync(MySqlConnection connection, string session)
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

                var last_record_number = GetLastRecordNumber(npgsqlConnection, OsmServiceType.Addrx);
                var next_last_record_number = GetNextLastRecordNumber(npgsqlConnection);

                var sql1 =
                    "SELECT COUNT(*) FROM addrx WHERE record_number>@last_record_number";

                var sql =
                    "SELECT id,tags FROM addrx WHERE record_number>@last_record_number";

                using (var command = new NpgsqlCommand(string.Join(";", sql1, sql), npgsqlConnection))
                {
                    command.Parameters.AddWithValue("last_record_number", last_record_number);

                    command.Prepare();

                    using (var reader = command.ExecuteReader())
                    {
                        if (reader.Read())
                            total = reader.GetInt64(0);

                        var take = 10000;

                        reader.NextResult();

                        while (true)
                        {
                            var docs = reader.ReadDocs3(take);

                            if (docs.Any())
                            {
                                var sb = new StringBuilder("REPLACE INTO addrx(id,title,priority) VALUES ");
                                sb.Append(string.Join(",",
                                    docs.Select(x => $"({x.id},'{x.text.TextEscape()}',{x.priority})")));

                                ExecuteNonQueryWithRepeatOnError(sb.ToString(), connection);

                                current += docs.Count;

                                await _progressHub.ProgressAsync(100f * current / total, id, session);
                            }

                            if (docs.Count < take) break;
                        }
                    }
                }

                SetLastRecordNumber(npgsqlConnection, OsmServiceType.Addrx, next_last_record_number);

                await npgsqlConnection.CloseAsync();

                await _progressHub.ProgressAsync(100f, id, session);
            }
        }

        private List<Doc2> GetDocs2(string[] guids, NpgsqlCommand npgsqlCommand2, int take)
        {
            var socr = true;
            var formal = false;

            var docs2 = new List<Doc2>(take);

            npgsqlCommand2.Parameters["guids"].Value = guids;

            using (var reader2 = npgsqlCommand2.ExecuteReader())
            {
                while (reader2.Read())
                {
                    var offname = reader2.SafeGetString(1);
                    var formalname = reader2.SafeGetString(2);
                    var shortname = reader2.SafeGetString(3);
                    var socrname = reader2.SafeGetString(4);
                    var aolevel = reader2.GetInt32(5);
                    var parentguid = reader2.SafeGetString(6);
                    var title = aolevel > 1
                        ? $"{(socr ? socrname : shortname)} {(formal ? formalname : offname)}"
                        : formal
                            ? formalname
                            : offname;

                    docs2.Add(new Doc2
                    {
                        guid = reader2.SafeGetString(0),
                        text = title,
                        parentguid = parentguid
                    });
                }
            }

            return docs2;
        }

        private long GetLastRecordNumber(NpgsqlConnection connection, OsmServiceType service_type)
        {
            using (var command = new NpgsqlCommand(
                "SELECT last_record_number FROM service_history WHERE service_type=@service_type LIMIT 1"
                , connection))
            {
                command.Parameters.AddWithValue("service_type", service_type);

                command.Prepare();

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

                command.Prepare();

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

                command.Prepare();

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

                command.Prepare();

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