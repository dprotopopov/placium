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
    public class Sphinx1UpdateService : BaseService, IUpdateService
    {
        private readonly ProgressHub _progressHub;

        public Sphinx1UpdateService(ProgressHub progressHub, IConfiguration configuration) : base(configuration)
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
                        "DROP TABLE addrob",
                        "DROP TABLE house",
                        "DROP TABLE stead"
                    }, connection);

                TryExecuteNonQueries(new[]
                {
                    "CREATE TABLE addrob(title text,title2 text)"
                    + " charset_table='0..9,A..Z->a..z,a..z,U+410..U+42F->U+430..U+44F,U+430..U+44F,U+401->U+0435,U+451->U+0435'",
                    "CREATE TABLE house(housenumber text,title text,title2 text)"
                    + " charset_table='0..9,A..Z->a..z,a..z,U+410..U+42F->U+430..U+44F,U+430..U+44F,U+401->U+0435,U+451->U+0435'",
                    "CREATE TABLE stead(housenumber text,title text,title2 text)"
                    + " charset_table='0..9,A..Z->a..z,a..z,U+410..U+42F->U+430..U+44F,U+430..U+44F,U+401->U+0435,U+451->U+0435'"
                }, connection);
            }

            if (full)
                using (var npgsqlConnection = new NpgsqlConnection(GetFiasConnectionString()))
                {
                    await npgsqlConnection.OpenAsync();
                    npgsqlConnection.ReloadTypes();
                    npgsqlConnection.TypeMapper.MapEnum<FiasServiceType>("service_type");

                    SetLastRecordNumber(npgsqlConnection, FiasServiceType.Addrob, 0);
                    SetLastRecordNumber(npgsqlConnection, FiasServiceType.House, 0);
                    SetLastRecordNumber(npgsqlConnection, FiasServiceType.Stead, 0);

                    await npgsqlConnection.CloseAsync();
                }

            await UpdateAddrobAsync(session, full);
            await UpdateHouseAsync(session, full);
            await UpdateSteadAsync(session, full);
        }

        private async Task UpdateAddrobAsync(string session, bool full)
        {
            var socr = true;
            var formal = false;

            using (var mySqlConnection = new MySqlConnection(GetSphinxConnectionString()))
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

                var last_record_number = GetLastRecordNumber(npgsqlConnection, FiasServiceType.Addrob, full);
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
                                    addrfull = title,
                                    parentguid = parentguid
                                });
                            }


                            if (docs1.Any())
                            {
                                var guids = docs1.Where(x => !string.IsNullOrEmpty(x.parentguid))
                                    .Select(x => x.parentguid).ToArray();

                                var docs2 = GetDocs2(guids, command2, take);

                                var q = from doc1 in docs1
                                    join doc2 in docs2 on doc1.parentguid equals doc2.guid into ps
                                    from doc in ps.DefaultIfEmpty()
                                    select new {doc1.id, text = $"{doc1.addrfull}", text2 = $"{doc?.addrfull ?? doc1.addrfull}"};

                                var sb = new StringBuilder("REPLACE INTO addrob(id,title,title2) VALUES ");
                                sb.Append(string.Join(",",
                                    q.Select(x => $"({x.id},'{x.text.TextEscape()}','{x.text2.TextEscape()}')")));

                                ExecuteNonQueryWithRepeatOnError(sb.ToString(), mySqlConnection);

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
                mySqlConnection.TryClose();

                await _progressHub.ProgressAsync(100f, id, session);
            }
        }

        private async Task UpdateHouseAsync(string session, bool full)
        {
            using (var mySqlConnection = new MySqlConnection(GetSphinxConnectionString()))
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

                var last_record_number = GetLastRecordNumber(npgsqlConnection, FiasServiceType.House, full);
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
                                    addrfull = string.Join(" ", list1),
                                    parentguid = parentguid
                                });
                            }

                            if (docs1.Any())
                            {
                                var guids = docs1.Where(x => !string.IsNullOrEmpty(x.parentguid))
                                    .Select(x => x.parentguid).ToArray();

                                var docs2 = GetDocs2(guids, command2, guids.Length);

                                var guids2 = docs2.Where(x => !string.IsNullOrEmpty(x.parentguid))
                                    .Select(x => x.parentguid).ToArray();

                                var docs3 = GetDocs2(guids2, command2, guids2.Length);

                                var q = from doc1 in docs1
                                    join doc2 in docs2 on doc1.parentguid equals doc2.guid
                                    join doc3 in docs3 on doc2.parentguid equals doc3.guid into ps
                                    from doc in ps.DefaultIfEmpty()
                                    select new
                                    {
                                        doc1.id, housenumber = $"{doc1.addrfull}", text = $"{doc2.addrfull}",
                                        text2 = $"{doc?.addrfull ?? doc2.addrfull}"
                                    };

                                var sb = new StringBuilder("REPLACE INTO house(id,housenumber,title,title2) VALUES ");
                                sb.Append(string.Join(",",
                                    q.Select(x =>
                                        $"({x.id},'{x.housenumber.TextEscape()}','{x.text.TextEscape()}','{x.text2.TextEscape()}')")));

                                ExecuteNonQueryWithRepeatOnError(sb.ToString(), mySqlConnection);

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
                mySqlConnection.TryClose();

                await _progressHub.ProgressAsync(100f, id, session);
            }
        }

        private async Task UpdateSteadAsync(string session, bool full)
        {
            using (var mySqlConnection = new MySqlConnection(GetSphinxConnectionString()))
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

                var last_record_number = GetLastRecordNumber(npgsqlConnection, FiasServiceType.Stead, full);
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
                                var guids = docs1.Where(x => !string.IsNullOrEmpty(x.parentguid))
                                    .Select(x => x.parentguid).ToArray();

                                var docs2 = GetDocs2(guids, command2, guids.Length);

                                var guids2 = docs2.Where(x => !string.IsNullOrEmpty(x.parentguid))
                                    .Select(x => x.parentguid).ToArray();

                                var docs3 = GetDocs2(guids2, command2, guids2.Length);

                                var q = from doc1 in docs1
                                    join doc2 in docs2 on doc1.parentguid equals doc2.guid
                                    join doc3 in docs3 on doc2.parentguid equals doc3.guid into ps
                                    from doc in ps.DefaultIfEmpty()
                                    select new
                                    {
                                        doc1.id, housenumber = $"{doc1.addrfull}", text = $"{doc2.addrfull}",
                                        text2 = $"{doc?.addrfull ?? doc2.addrfull}"
                                    };

                                var sb = new StringBuilder("REPLACE INTO stead(id,housenumber,title,title2) VALUES ");
                                sb.Append(string.Join(",",
                                    q.Select(x =>
                                        $"({x.id},'{x.housenumber.TextEscape()}','{x.text.TextEscape()}','{x.text2.TextEscape()}')")));

                                ExecuteNonQueryWithRepeatOnError(sb.ToString(), mySqlConnection);

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
                mySqlConnection.TryClose();

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
                        addrfull = title,
                        parentguid = parentguid
                    });
                }
            }

            return docs2;
        }
    }
}