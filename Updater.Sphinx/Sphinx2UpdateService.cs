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
    public class Sphinx2UpdateService : BaseService, IUpdateService
    {
        private readonly ProgressHub _progressHub;

        public Sphinx2UpdateService(ProgressHub progressHub, IConfiguration configuration) : base(configuration)
        {
            _progressHub = progressHub;
        }

        public async Task UpdateAsync(string session, bool full)
        {
            using (var connection = new MySqlConnection(GetSphinxConnectionString()))
            {
                if (full)
                    TryExecuteNonQueries(new[]
                    {
                        "DROP TABLE addrobx"
                    }, connection);

                TryExecuteNonQueries(new[]
                {
                    "CREATE TABLE addrobx(title text,priority int) phrase_boundary='U+2C' phrase_boundary_step='100'"
                }, connection);
            }

            if (full)
                using (var npgsqlConnection = new NpgsqlConnection(GetFiasConnectionString()))
                {
                    await npgsqlConnection.OpenAsync();
                    npgsqlConnection.ReloadTypes();
                    npgsqlConnection.TypeMapper.MapEnum<FiasServiceType2>("service_type2");

                    SetLastRecordNumber(npgsqlConnection, FiasServiceType2.Addrob, 0);
                    SetLastRecordNumber(npgsqlConnection, FiasServiceType2.House, 0);
                    SetLastRecordNumber(npgsqlConnection, FiasServiceType2.Stead, 0);
                    SetLastRecordNumber(npgsqlConnection, FiasServiceType2.Room, 0);

                    await npgsqlConnection.CloseAsync();
                }

            await UpdateAddrobAsync(session, full);
            await UpdateHouseAsync(session, full);
            await UpdateSteadAsync(session, full);
            await UpdateRoomAsync(session, full);
        }

        private async Task UpdateAddrobAsync(string session, bool full)
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
                npgsqlConnection.TypeMapper.MapEnum<FiasServiceType2>("service_type2");

                var last_record_number = GetLastRecordNumber(npgsqlConnection, FiasServiceType2.Addrob, full);
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
                {
                    command.Parameters.AddWithValue("last_record_number", last_record_number);

                    command.Prepare();

                    using (var reader = command.ExecuteReader())
                    {
                        while (reader.Read()) total += reader.GetInt64(0);

                        var take = 100;

                        reader.NextResult();

                        var obj = new object();
                        var reader_is_empty = false;

                        Parallel.For(0, 12,
                            i =>
                            {
                                using (var mySqlConnection = new MySqlConnection(GetSphinxConnectionString()))
                                using (var npgsqlConnection2 = new NpgsqlConnection(GetFiasConnectionString()))
                                {
                                    npgsqlConnection2.Open();

                                    using (var command2 = new NpgsqlCommand(sql2, npgsqlConnection2))
                                    {
                                        command2.Parameters.Add("guids", NpgsqlDbType.Array | NpgsqlDbType.Varchar);

                                        command2.Prepare();

                                        while (true)
                                        {
                                            var guids1 = new List<string>();

                                            var docs1 = new List<Doc1>(take);

                                            lock (obj)
                                            {
                                                if (reader_is_empty) break;
                                                for (var j = 0; j < take && reader.Read(); j++)
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

                                                reader_is_empty = docs1.Count() < take;
                                                if (!docs1.Any()) break;
                                            }

                                            if (docs1.Any())
                                            {
                                                for (var guids = docs1.Where(x => !string.IsNullOrEmpty(x.parentguid))
                                                        .Select(x => x.parentguid).ToArray();
                                                    guids.Any();
                                                    guids = docs1.Where(x => !string.IsNullOrEmpty(x.parentguid))
                                                        .Select(x => x.parentguid).ToArray())
                                                {
                                                    guids = guids.Except(guids1).ToArray();

                                                    if (!guids.Any()) break;

                                                    var docs2 = GetDocs2(guids, command2, guids.Length);

                                                    if (!docs2.Any()) break;

                                                    var q = from doc1 in docs1
                                                        join doc2 in docs2 on doc1.parentguid equals doc2.guid
                                                        select new {doc1, doc2};

                                                    foreach (var pair in q)
                                                    {
                                                        pair.doc1.parentguid = pair.doc2.parentguid;
                                                        pair.doc1.text = $"{pair.doc2.text}, {pair.doc1.text}";
                                                    }

                                                    guids1.AddRange(guids);
                                                }

                                                var sb = new StringBuilder(
                                                    "REPLACE INTO addrobx(id,title,priority) VALUES ");
                                                sb.Append(string.Join(",",
                                                    docs1.Select(x =>
                                                        $"({x.id},'{x.text.TextEscape()}','{x.text.Split(",").Length}')")));

                                                ExecuteNonQueryWithRepeatOnError(sb.ToString(), mySqlConnection);

                                                lock (obj)
                                                {
                                                    current += docs1.Count();

                                                    _progressHub.ProgressAsync(100f * current / total, id, session)
                                                        .GetAwaiter()
                                                        .GetResult();
                                                }
                                            }
                                        }
                                    }

                                    npgsqlConnection2.Close();
                                    mySqlConnection.TryClose();
                                }
                            });
                    }
                }

                SetLastRecordNumber(npgsqlConnection, FiasServiceType2.Addrob, next_last_record_number);

                await npgsqlConnection.CloseAsync();

                await _progressHub.ProgressAsync(100f, id, session);
            }
        }

        private async Task UpdateHouseAsync(string session, bool full)
        {
            using (var npgsqlConnection = new NpgsqlConnection(GetFiasConnectionString()))
            {
                var current = 0L;
                var total = 0L;

                var id = Guid.NewGuid().ToString();
                await _progressHub.InitAsync(id, session);

                await npgsqlConnection.OpenAsync();

                npgsqlConnection.ReloadTypes();
                npgsqlConnection.TypeMapper.MapEnum<FiasServiceType2>("service_type2");

                var last_record_number = GetLastRecordNumber(npgsqlConnection, FiasServiceType2.House, full);
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
                        $"SELECT COUNT(*) FROM {x} JOIN (SELECT now() as n) as q ON startdate<=n AND n<enddate WHERE {x}.record_number>@last_record_number"));

                var sql = string.Join("\nUNION ALL\n",
                    list.Select(x =>
                        $@"SELECT {x}.record_id,housenum,buildnum,strucnum,eststat.name,aoguid FROM {x}
                        JOIN (SELECT now() as n) as q ON startdate<=n AND n<enddate 
                        JOIN eststat ON {x}.eststatus=eststat.eststatid
                        WHERE {x}.record_number>@last_record_number"));

                using (var command = new NpgsqlCommand(string.Join(";", sql1, sql), npgsqlConnection))
                {
                    command.Parameters.AddWithValue("last_record_number", last_record_number);

                    command.Prepare();

                    using (var reader = command.ExecuteReader())
                    {
                        while (reader.Read()) total += reader.GetInt64(0);

                        var take = 100;

                        reader.NextResult();

                        var obj = new object();
                        var reader_is_empty = false;

                        Parallel.For(0, 12,
                            i =>
                            {
                                using (var mySqlConnection = new MySqlConnection(GetSphinxConnectionString()))
                                using (var npgsqlConnection2 = new NpgsqlConnection(GetFiasConnectionString()))
                                {
                                    npgsqlConnection2.Open();

                                    using (var command2 = new NpgsqlCommand(sql2, npgsqlConnection2))
                                    {
                                        command2.Parameters.Add("guids", NpgsqlDbType.Array | NpgsqlDbType.Varchar);

                                        command2.Prepare();

                                        while (true)
                                        {
                                            var guids1 = new List<string>();

                                            var docs1 = new List<Doc1>(take);

                                            lock (obj)
                                            {
                                                if (reader_is_empty) break;
                                                for (var j = 0; j < take && reader.Read(); j++)
                                                {
                                                    var housenum = reader.SafeGetString(1);
                                                    var buildnum = reader.SafeGetString(2);
                                                    var strucnum = reader.SafeGetString(3);
                                                    var name = reader.SafeGetString(4);
                                                    var parentguid = reader.SafeGetString(5);
                                                    var list1 = new List<string> {name};
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

                                                reader_is_empty = docs1.Count() < take;
                                                if (!docs1.Any()) break;
                                            }

                                            if (docs1.Any())
                                            {
                                                for (var guids = docs1.Where(x => !string.IsNullOrEmpty(x.parentguid))
                                                        .Select(x => x.parentguid).ToArray();
                                                    guids.Any();
                                                    guids = docs1.Where(x => !string.IsNullOrEmpty(x.parentguid))
                                                        .Select(x => x.parentguid).ToArray())
                                                {
                                                    guids = guids.Except(guids1).ToArray();

                                                    if (!guids.Any()) break;

                                                    var docs2 = GetDocs2(guids, command2, guids.Length);

                                                    if (!docs2.Any()) break;

                                                    var q = from doc1 in docs1
                                                        join doc2 in docs2 on doc1.parentguid equals doc2.guid
                                                        select new {doc1, doc2};

                                                    foreach (var pair in q)
                                                    {
                                                        pair.doc1.parentguid = pair.doc2.parentguid;
                                                        pair.doc1.text = $"{pair.doc2.text}, {pair.doc1.text}";
                                                    }

                                                    guids1.AddRange(guids);
                                                }

                                                var sb = new StringBuilder(
                                                    "REPLACE INTO addrobx(id,title,priority) VALUES ");
                                                sb.Append(string.Join(",",
                                                    docs1.Select(x =>
                                                        $"({x.id},'{x.text.TextEscape()}','{x.text.Split(",").Length}')")));

                                                ExecuteNonQueryWithRepeatOnError(sb.ToString(), mySqlConnection);

                                                lock (obj)
                                                {
                                                    current += docs1.Count();

                                                    _progressHub.ProgressAsync(100f * current / total, id, session)
                                                        .GetAwaiter()
                                                        .GetResult();
                                                }
                                            }
                                        }
                                    }

                                    npgsqlConnection2.Close();
                                    mySqlConnection.TryClose();
                                }
                            });
                    }
                }

                SetLastRecordNumber(npgsqlConnection, FiasServiceType2.House, next_last_record_number);

                await npgsqlConnection.CloseAsync();

                await _progressHub.ProgressAsync(100f, id, session);
            }
        }

        private async Task UpdateRoomAsync(string session, bool full)
        {
            using (var npgsqlConnection = new NpgsqlConnection(GetFiasConnectionString()))
            {
                var current = 0L;
                var total = 0L;

                var id = Guid.NewGuid().ToString();
                await _progressHub.InitAsync(id, session);

                await npgsqlConnection.OpenAsync();

                npgsqlConnection.ReloadTypes();
                npgsqlConnection.TypeMapper.MapEnum<FiasServiceType2>("service_type2");

                var last_record_number = GetLastRecordNumber(npgsqlConnection, FiasServiceType2.Room, full);
                var next_last_record_number = GetNextLastRecordNumber(npgsqlConnection);

                var list3 = new List<string>();
                var list2 = new List<string>();
                var list = new List<string>();

                using (var command = new NpgsqlCommand(
                    string.Join(";", new[] {@"addrob\d+", @"house\d+", @"room\d+"}.Select(x =>
                        $"SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' and table_name similar to '{x}'")),
                    npgsqlConnection))
                {
                    command.Prepare();

                    using (var reader = command.ExecuteReader())
                    {
                        list3.Fill(reader);
                        reader.NextResult();
                        list2.Fill(reader);
                        reader.NextResult();
                        list.Fill(reader);
                    }
                }

                var sql3 = string.Join("\nUNION ALL\n",
                    list3.Select(x =>
                        $@"SELECT {x}.aoguid,offname,formalname,shortname,socrbase.socrname,aolevel,parentguid FROM {x}
                        JOIN socrbase ON {x}.shortname=socrbase.scname AND {x}.aolevel=socrbase.level
                        WHERE {x}.aoguid=ANY(@guids) AND {x}.livestatus=1"));

                var sql2 = string.Join("\nUNION ALL\n",
                    list2.Select(x =>
                        $@"SELECT {x}.houseguid,housenum,buildnum,strucnum,eststat.name,aoguid FROM {x}
                        JOIN (SELECT now() as n) as q ON startdate<=n AND n<enddate 
                        JOIN eststat ON {x}.eststatus=eststat.eststatid
                        WHERE {x}.houseguid=ANY(@guids)"));

                var sql1 = string.Join("\nUNION ALL\n",
                    list.Select(x =>
                        $"SELECT COUNT(*) FROM {x} WHERE record_number>@last_record_number AND livestatus=1"));

                var sql = string.Join("\nUNION ALL\n",
                    list.Select(x =>
                        $@"SELECT record_id,flatnumber,roomnumber,houseguid FROM {x}
                        WHERE record_number>@last_record_number AND livestatus=1"));

                using (var command = new NpgsqlCommand(string.Join(";", sql1, sql), npgsqlConnection))
                {
                    command.Parameters.AddWithValue("last_record_number", last_record_number);

                    command.Prepare();

                    using (var reader = command.ExecuteReader())
                    {
                        while (reader.Read()) total += reader.GetInt64(0);

                        var take = 100;

                        reader.NextResult();

                        var obj = new object();
                        var reader_is_empty = false;

                        Parallel.For(0, 12,
                            i =>
                            {
                                using (var mySqlConnection = new MySqlConnection(GetSphinxConnectionString()))
                                using (var npgsqlConnection2 = new NpgsqlConnection(GetFiasConnectionString()))
                                using (var npgsqlConnection3 = new NpgsqlConnection(GetFiasConnectionString()))
                                {
                                    npgsqlConnection2.Open();
                                    npgsqlConnection3.Open();

                                    using (var command2 = new NpgsqlCommand(sql2, npgsqlConnection2))
                                    using (var command3 = new NpgsqlCommand(sql3, npgsqlConnection3))
                                    {
                                        command2.Parameters.Add("guids", NpgsqlDbType.Array | NpgsqlDbType.Varchar);

                                        command2.Prepare();

                                        command3.Parameters.Add("guids", NpgsqlDbType.Array | NpgsqlDbType.Varchar);

                                        command3.Prepare();

                                        while (true)
                                        {
                                            var guids1 = new List<string>();

                                            var docs1 = new List<Doc1>(take);

                                            lock (obj)
                                            {
                                                if (reader_is_empty) break;
                                                for (var j = 0; j < take && reader.Read(); j++)
                                                {
                                                    var list1 = new List<string>();
                                                    var flatnumber = reader.SafeGetString(1);
                                                    var roomnumber = reader.SafeGetString(2);
                                                    var parentguid = reader.SafeGetString(3);
                                                    if (!string.IsNullOrEmpty(flatnumber))
                                                        list1.Add($"Квартира {flatnumber}");
                                                    if (!string.IsNullOrEmpty(roomnumber))
                                                        list1.Add($"Комната {roomnumber}");
                                                    docs1.Add(new Doc1
                                                    {
                                                        id = reader.GetInt64(0),
                                                        text = string.Join(" ", list1),
                                                        parentguid = parentguid
                                                    });
                                                }

                                                reader_is_empty = docs1.Count() < take;
                                                if (!docs1.Any()) break;
                                            }

                                            if (docs1.Any())
                                            {
                                                var guids = docs1.Where(x => !string.IsNullOrEmpty(x.parentguid))
                                                    .Select(x => x.parentguid).ToArray();
                                                var docs2 = new List<Doc2>(take);

                                                command2.Parameters["guids"].Value = guids;

                                                using (var reader2 = command2.ExecuteReader())
                                                {
                                                    while (reader2.Read())
                                                    {
                                                        var housenum = reader2.SafeGetString(1);
                                                        var buildnum = reader2.SafeGetString(2);
                                                        var strucnum = reader2.SafeGetString(3);
                                                        var name = reader2.SafeGetString(4);
                                                        var parentguid = reader2.SafeGetString(5);
                                                        var list1 = new List<string> {name};
                                                        if (!string.IsNullOrEmpty(housenum)) list1.Add($"{housenum}");
                                                        if (!string.IsNullOrEmpty(buildnum)) list1.Add($"к{buildnum}");
                                                        if (!string.IsNullOrEmpty(strucnum)) list1.Add($"с{strucnum}");

                                                        docs2.Add(new Doc2
                                                        {
                                                            guid = reader2.SafeGetString(0),
                                                            text = string.Join(" ", list1),
                                                            parentguid = parentguid
                                                        });
                                                    }
                                                }

                                                var q = from doc1 in docs1
                                                    join doc2 in docs2 on doc1.parentguid equals doc2.guid
                                                    select new {doc1, doc2};

                                                foreach (var pair in q)
                                                {
                                                    pair.doc1.parentguid = pair.doc2.parentguid;
                                                    pair.doc1.text = $"{pair.doc2.text}, {pair.doc1.text}";
                                                }
                                            }


                                            if (docs1.Any())
                                            {
                                                for (var guids = docs1.Where(x => !string.IsNullOrEmpty(x.parentguid))
                                                        .Select(x => x.parentguid).ToArray();
                                                    guids.Any();
                                                    guids = docs1.Where(x => !string.IsNullOrEmpty(x.parentguid))
                                                        .Select(x => x.parentguid).ToArray())
                                                {
                                                    guids = guids.Except(guids1).ToArray();

                                                    if (!guids.Any()) break;

                                                    var docs2 = GetDocs2(guids, command3, guids.Length);

                                                    if (!docs2.Any()) break;

                                                    var q = from doc1 in docs1
                                                        join doc2 in docs2 on doc1.parentguid equals doc2.guid
                                                        select new {doc1, doc2};

                                                    foreach (var pair in q)
                                                    {
                                                        pair.doc1.parentguid = pair.doc2.parentguid;
                                                        pair.doc1.text = $"{pair.doc2.text}, {pair.doc1.text}";
                                                    }

                                                    guids1.AddRange(guids);
                                                }

                                                var sb = new StringBuilder(
                                                    "REPLACE INTO addrobx(id,title,priority) VALUES ");
                                                sb.Append(string.Join(",",
                                                    docs1.Select(x =>
                                                        $"({x.id},'{x.text.TextEscape()}','{x.text.Split(",").Length}')")));

                                                ExecuteNonQueryWithRepeatOnError(sb.ToString(), mySqlConnection);

                                                lock (obj)
                                                {
                                                    current += docs1.Count();

                                                    _progressHub.ProgressAsync(100f * current / total, id, session)
                                                        .GetAwaiter()
                                                        .GetResult();
                                                }
                                            }
                                        }
                                    }

                                    npgsqlConnection3.Close();
                                    npgsqlConnection2.Close();
                                    mySqlConnection.TryClose();
                                }
                            });
                    }
                }

                SetLastRecordNumber(npgsqlConnection, FiasServiceType2.Room, next_last_record_number);

                await npgsqlConnection.CloseAsync();

                await _progressHub.ProgressAsync(100f, id, session);
            }
        }

        private async Task UpdateSteadAsync(string session, bool full)
        {
            using (var npgsqlConnection = new NpgsqlConnection(GetFiasConnectionString()))
            {
                var current = 0L;
                var total = 0L;

                var id = Guid.NewGuid().ToString();
                await _progressHub.InitAsync(id, session);

                await npgsqlConnection.OpenAsync();

                npgsqlConnection.ReloadTypes();
                npgsqlConnection.TypeMapper.MapEnum<FiasServiceType2>("service_type2");

                var last_record_number = GetLastRecordNumber(npgsqlConnection, FiasServiceType2.Stead, full);
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
                        $"SELECT COUNT(*) FROM {x} WHERE record_number>@last_record_number AND livestatus=1"));

                var sql = string.Join("\nUNION ALL\n",
                    list.Select(x =>
                        $"SELECT record_id,number,parentguid FROM {x} WHERE record_number>@last_record_number AND livestatus=1"));

                using (var command = new NpgsqlCommand(string.Join(";", sql1, sql), npgsqlConnection))
                {
                    command.Parameters.AddWithValue("last_record_number", last_record_number);

                    command.Prepare();

                    using (var reader = command.ExecuteReader())
                    {
                        while (reader.Read()) total += reader.GetInt64(0);

                        var take = 100;

                        reader.NextResult();

                        var obj = new object();
                        var reader_is_empty = false;

                        Parallel.For(0, 12,
                            i =>
                            {
                                using (var mySqlConnection = new MySqlConnection(GetSphinxConnectionString()))
                                using (var npgsqlConnection2 = new NpgsqlConnection(GetFiasConnectionString()))
                                {
                                    npgsqlConnection2.Open();

                                    using (var command2 = new NpgsqlCommand(sql2, npgsqlConnection2))
                                    {
                                        command2.Parameters.Add("guids", NpgsqlDbType.Array | NpgsqlDbType.Varchar);

                                        command2.Prepare();

                                        while (true)
                                        {
                                            var guids1 = new List<string>();

                                            List<Doc1> docs1;
                                            lock (obj)
                                            {
                                                if (reader_is_empty) break;
                                                docs1 = reader.ReadDocs1(take);

                                                reader_is_empty = docs1.Count() < take;
                                                if (!docs1.Any()) break;
                                            }

                                            if (docs1.Any())
                                            {
                                                for (var guids = docs1.Where(x => !string.IsNullOrEmpty(x.parentguid))
                                                        .Select(x => x.parentguid).ToArray();
                                                    guids.Any();
                                                    guids = docs1.Where(x => !string.IsNullOrEmpty(x.parentguid))
                                                        .Select(x => x.parentguid).ToArray())
                                                {
                                                    guids = guids.Except(guids1).ToArray();

                                                    if (!guids.Any()) break;

                                                    var docs2 = GetDocs2(guids, command2, guids.Length);

                                                    if (!docs2.Any()) break;

                                                    var q = from doc1 in docs1
                                                        join doc2 in docs2 on doc1.parentguid equals doc2.guid
                                                        select new {doc1, doc2};

                                                    foreach (var pair in q)
                                                    {
                                                        pair.doc1.parentguid = pair.doc2.parentguid;
                                                        pair.doc1.text = $"{pair.doc2.text}, {pair.doc1.text}";
                                                    }

                                                    guids1.AddRange(guids);
                                                }

                                                var sb = new StringBuilder(
                                                    "REPLACE INTO addrobx(id,title,priority) VALUES ");
                                                sb.Append(string.Join(",",
                                                    docs1.Select(x =>
                                                        $"({x.id},'{x.text.TextEscape()}','{x.text.Split(",").Length}')")));

                                                ExecuteNonQueryWithRepeatOnError(sb.ToString(), mySqlConnection);

                                                lock (obj)
                                                {
                                                    current += docs1.Count();

                                                    _progressHub.ProgressAsync(100f * current / total, id, session)
                                                        .GetAwaiter()
                                                        .GetResult();
                                                }
                                            }
                                        }
                                    }

                                    npgsqlConnection2.Close();
                                    mySqlConnection.TryClose();
                                }
                            });
                    }
                }

                SetLastRecordNumber(npgsqlConnection, FiasServiceType2.Stead, next_last_record_number);

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
    }
}