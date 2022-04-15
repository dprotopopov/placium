using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MySql.Data.MySqlClient;
using Npgsql;
using NpgsqlTypes;
using Placium.Common;
using Placium.Types;

namespace Updater.Addrobx.Sphinx
{
    public class SphinxAddrobxUpdateService : BaseAppService, IUpdateService
    {
        private readonly IParallelConfig _parallelConfig;
        private readonly IProgressClient _progressClient;

        public SphinxAddrobxUpdateService(IProgressClient progressClient, IConnectionsConfig configuration,
            IParallelConfig parallelConfig) : base(
            configuration)
        {
            _progressClient = progressClient;
            _parallelConfig = parallelConfig;
        }

        public async Task UpdateAsync(string session, bool full)
        {
            await using (var connection = new MySqlConnection(GetSphinxConnectionString()))
            {
                if (full)
                    TryExecuteNonQueries(new[]
                    {
                        "DROP TABLE addrobx"
                    }, connection);

                TryExecuteNonQueries(new[]
                {
                    "CREATE TABLE addrobx(title text indexed stored,guid string,priority int,building int)"
                    + " phrase_boundary='U+2C'"
                    + " phrase_boundary_step='100'"
                    + " min_infix_len='1'"
                    + " expand_keywords='1'"
                    + " charset_table='0..9,A..Z->a..z,a..z,U+410..U+42F->U+430..U+44F,U+430..U+44F,U+401->U+0435,U+451->U+0435'"
                    + " morphology='stem_ru'"
                }, connection);
            }

            if (full)
            {
                await using var npgsqlConnection = new NpgsqlConnection(GetFiasConnectionString());
                await npgsqlConnection.OpenAsync();
                npgsqlConnection.ReloadTypes();
                npgsqlConnection.TypeMapper.MapEnum<FiasServiceType>("service_type");

                SetLastRecordNumber(npgsqlConnection, FiasServiceType.Addrob, 0);
                SetLastRecordNumber(npgsqlConnection, FiasServiceType.House, 0);
                SetLastRecordNumber(npgsqlConnection, FiasServiceType.Stead, 0);
                SetLastRecordNumber(npgsqlConnection, FiasServiceType.Room, 0);

                await npgsqlConnection.CloseAsync();
            }

            await UpdateAddrobAsync(session, full);
            await UpdateHouseAsync(session, full);
            await UpdateSteadAsync(session, full);
            await UpdateRoomAsync(session, full);

            await using (var connection = new MySqlConnection(GetSphinxConnectionString()))
            {
                TryExecuteNonQueries(new[]
                {
                    "FLUSH RTINDEX addrobx"
                }, connection);
            }
        }

        private async Task UpdateAddrobAsync(string session, bool full)
        {
            var socr = true;
            var formal = false;

            await using var npgsqlConnection = new NpgsqlConnection(GetFiasConnectionString());
            var current = 0L;
            var total = 0L;

            var id = Guid.NewGuid().ToString();
            await _progressClient.Init(id, session);

            await npgsqlConnection.OpenAsync();

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
                    $@"SELECT {x}.record_id,offname,formalname,shortname,socrbase.socrname,aolevel,parentguid,aoguid FROM {x}
                        JOIN socrbase ON {x}.shortname=socrbase.scname AND {x}.aolevel=socrbase.level
                        WHERE {x}.livestatus=1 AND {x}.record_number>@last_record_number"));

            await using (var command = new NpgsqlCommand(string.Join(";", sql1, sql), npgsqlConnection))
            {
                command.Parameters.AddWithValue("last_record_number", last_record_number);

                await command.PrepareAsync();

                await using var reader = command.ExecuteReader();
                while (reader.Read()) total += reader.GetInt64(0);

                var take = 100;

                reader.NextResult();

                var obj = new object();
                var reader_is_empty = false;

                Parallel.For(0, _parallelConfig.GetNumberOfThreads(),
                    i =>
                    {
                        using var mySqlConnection = new MySqlConnection(GetSphinxConnectionString());
                        using var npgsqlConnection2 = new NpgsqlConnection(GetFiasConnectionString());
                        npgsqlConnection2.Open();

                        using (var command2 = new NpgsqlCommand(sql2, npgsqlConnection2))
                        {
                            command2.Parameters.Add("guids", NpgsqlDbType.Array | NpgsqlDbType.Varchar);

                            command2.Prepare();

                            for (;;)
                            {
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
                                        var guid = reader.SafeGetString(7);
                                        var title = aolevel > 1
                                            ? $"{(socr ? socrname : shortname)} {(formal ? formalname : offname)}"
                                            : formal
                                                ? formalname
                                                : offname;
                                        docs1.Add(new Doc1
                                        {
                                            id = reader.GetInt64(0),
                                            addrfull = title,
                                            guid = guid,
                                            parentguid = parentguid,
                                            building = 0,
                                            postalcode = null
                                        });
                                    }

                                    reader_is_empty = docs1.Count() < take;
                                    if (!docs1.Any()) break;
                                }

                                if (docs1.Any())
                                {
                                    ProcessDoc1(docs1, command2, mySqlConnection, false);

                                    lock (obj)
                                    {
                                        current += docs1.Count();

                                        _progressClient.Progress(100f * current / total, id, session)
                                            .GetAwaiter()
                                            .GetResult();
                                    }
                                }
                            }
                        }

                        npgsqlConnection2.Close();
                        mySqlConnection.TryClose();
                    });
            }

            SetLastRecordNumber(npgsqlConnection, FiasServiceType.Addrob, next_last_record_number);

            await npgsqlConnection.CloseAsync();

            await _progressClient.Finalize(id, session);
        }

        private async Task UpdateHouseAsync(string session, bool full)
        {
            await using var npgsqlConnection = new NpgsqlConnection(GetFiasConnectionString());
            var current = 0L;
            var total = 0L;

            var id = Guid.NewGuid().ToString();
            await _progressClient.Init(id, session);

            await npgsqlConnection.OpenAsync();

            npgsqlConnection.ReloadTypes();
            npgsqlConnection.TypeMapper.MapEnum<FiasServiceType>("service_type");

            var last_record_number = GetLastRecordNumber(npgsqlConnection, FiasServiceType.House, full);
            var next_last_record_number = GetNextLastRecordNumber(npgsqlConnection);

            var list2 = new List<string>();
            var list = new List<string>();

            await using (var command = new NpgsqlCommand(
                             string.Join(";", new[] { @"addrob\d+", @"house\d+" }.Select(x =>
                                 $"SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' and table_name similar to '{x}'")),
                             npgsqlConnection))
            {
                await command.PrepareAsync();

                await using var reader = command.ExecuteReader();
                list2.Fill(reader);
                reader.NextResult();
                list.Fill(reader);
            }

            var sql2 = string.Join("\nUNION ALL\n",
                list2.Select(x =>
                    $@"SELECT {x}.aoguid,offname,formalname,shortname,socrbase.socrname,aolevel,parentguid,postalcode FROM {x}
                        JOIN socrbase ON {x}.shortname=socrbase.scname AND {x}.aolevel=socrbase.level
                        WHERE {x}.aoguid=ANY(@guids) AND {x}.livestatus=1"));

            var sql1 = string.Join("\nUNION ALL\n",
                list.Select(x =>
                    $"SELECT COUNT(*) FROM {x} JOIN (SELECT now() as n) as q ON startdate<=n AND n<enddate WHERE {x}.record_number>@last_record_number"));

            var sql = string.Join("\nUNION ALL\n",
                list.Select(x =>
                    $@"SELECT {x}.record_id,housenum,buildnum,strucnum,eststat.name,aoguid,houseguid,postalcode FROM {x}
                        JOIN (SELECT now() as n) as q ON startdate<=n AND n<enddate 
                        JOIN eststat ON {x}.eststatus=eststat.eststatid
                        WHERE {x}.record_number>@last_record_number"));

            await using (var command = new NpgsqlCommand(string.Join(";", sql1, sql), npgsqlConnection))
            {
                command.Parameters.AddWithValue("last_record_number", last_record_number);

                await command.PrepareAsync();

                await using var reader = command.ExecuteReader();
                while (reader.Read()) total += reader.GetInt64(0);

                var take = 100;

                reader.NextResult();

                var obj = new object();
                var reader_is_empty = false;

                Parallel.For(0, _parallelConfig.GetNumberOfThreads(),
                    i =>
                    {
                        using var mySqlConnection = new MySqlConnection(GetSphinxConnectionString());
                        using var npgsqlConnection2 = new NpgsqlConnection(GetFiasConnectionString());
                        npgsqlConnection2.Open();

                        using (var command2 = new NpgsqlCommand(sql2, npgsqlConnection2))
                        {
                            command2.Parameters.Add("guids", NpgsqlDbType.Array | NpgsqlDbType.Varchar);

                            command2.Prepare();

                            for (;;)
                            {
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
                                        var guid = reader.SafeGetString(6);
                                        var postalcode = reader.SafeGetString(7);
                                        var list1 = new List<string> { name };
                                        if (!string.IsNullOrEmpty(housenum)) list1.Add($"{housenum}");
                                        if (!string.IsNullOrEmpty(buildnum)) list1.Add($"к{buildnum}");
                                        if (!string.IsNullOrEmpty(strucnum)) list1.Add($"с{strucnum}");
                                        docs1.Add(new Doc1
                                        {
                                            id = reader.GetInt64(0),
                                            addrfull = string.Join(" ", list1),
                                            guid = guid,
                                            parentguid = parentguid,
                                            building = 1,
                                            postalcode = postalcode
                                        });
                                    }

                                    reader_is_empty = docs1.Count() < take;
                                    if (!docs1.Any()) break;
                                }

                                if (docs1.Any())
                                {
                                    ProcessDoc1(docs1, command2, mySqlConnection, true);

                                    lock (obj)
                                    {
                                        current += docs1.Count();

                                        _progressClient.Progress(100f * current / total, id, session)
                                            .GetAwaiter()
                                            .GetResult();
                                    }
                                }
                            }
                        }

                        npgsqlConnection2.Close();
                        mySqlConnection.TryClose();
                    });
            }

            SetLastRecordNumber(npgsqlConnection, FiasServiceType.House, next_last_record_number);

            await npgsqlConnection.CloseAsync();

            await _progressClient.Finalize(id, session);
        }

        private async Task UpdateRoomAsync(string session, bool full)
        {
            await using var npgsqlConnection = new NpgsqlConnection(GetFiasConnectionString());
            var current = 0L;
            var total = 0L;

            var id = Guid.NewGuid().ToString();
            await _progressClient.Init(id, session);

            await npgsqlConnection.OpenAsync();

            npgsqlConnection.ReloadTypes();
            npgsqlConnection.TypeMapper.MapEnum<FiasServiceType>("service_type");

            var last_record_number = GetLastRecordNumber(npgsqlConnection, FiasServiceType.Room, full);
            var next_last_record_number = GetNextLastRecordNumber(npgsqlConnection);

            var list3 = new List<string>();
            var list2 = new List<string>();
            var list = new List<string>();

            await using (var command = new NpgsqlCommand(
                             string.Join(";", new[] { @"addrob\d+", @"house\d+", @"room\d+" }.Select(x =>
                                 $"SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' and table_name similar to '{x}'")),
                             npgsqlConnection))
            {
                await command.PrepareAsync();

                await using var reader = command.ExecuteReader();
                list3.Fill(reader);
                reader.NextResult();
                list2.Fill(reader);
                reader.NextResult();
                list.Fill(reader);
            }

            var sql3 = string.Join("\nUNION ALL\n",
                list3.Select(x =>
                    $@"SELECT {x}.aoguid,offname,formalname,shortname,socrbase.socrname,aolevel,parentguid,postalcode FROM {x}
                        JOIN socrbase ON {x}.shortname=socrbase.scname AND {x}.aolevel=socrbase.level
                        WHERE {x}.aoguid=ANY(@guids) AND {x}.livestatus=1"));

            var sql2 = string.Join("\nUNION ALL\n",
                list2.Select(x =>
                    $@"SELECT {x}.houseguid,housenum,buildnum,strucnum,eststat.name,aoguid,postalcode FROM {x}
                        JOIN (SELECT now() as n) as q ON startdate<=n AND n<enddate 
                        JOIN eststat ON {x}.eststatus=eststat.eststatid
                        WHERE {x}.houseguid=ANY(@guids)"));

            var sql1 = string.Join("\nUNION ALL\n",
                list.Select(x =>
                    $"SELECT COUNT(*) FROM {x} WHERE record_number>@last_record_number AND livestatus=1"));

            var sql = string.Join("\nUNION ALL\n",
                list.Select(x =>
                    $@"SELECT record_id,flatnumber,roomnumber,houseguid,roomguid,postalcode FROM {x}
                        WHERE record_number>@last_record_number AND livestatus=1"));

            await using (var command = new NpgsqlCommand(string.Join(";", sql1, sql), npgsqlConnection))
            {
                command.Parameters.AddWithValue("last_record_number", last_record_number);

                await command.PrepareAsync();

                using var reader = command.ExecuteReader();
                while (reader.Read()) total += reader.GetInt64(0);

                var take = 100;

                reader.NextResult();

                var obj = new object();
                var reader_is_empty = false;

                Parallel.For(0, _parallelConfig.GetNumberOfThreads(),
                    i =>
                    {
                        using var mySqlConnection = new MySqlConnection(GetSphinxConnectionString());
                        using var npgsqlConnection2 = new NpgsqlConnection(GetFiasConnectionString());
                        using var npgsqlConnection3 = new NpgsqlConnection(GetFiasConnectionString());
                        npgsqlConnection2.Open();
                        npgsqlConnection3.Open();

                        using (var command2 = new NpgsqlCommand(sql2, npgsqlConnection2))
                        using (var command3 = new NpgsqlCommand(sql3, npgsqlConnection3))
                        {
                            command2.Parameters.Add("guids", NpgsqlDbType.Array | NpgsqlDbType.Varchar);

                            command2.Prepare();

                            command3.Parameters.Add("guids", NpgsqlDbType.Array | NpgsqlDbType.Varchar);

                            command3.Prepare();

                            for (;;)
                            {
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
                                        var guid = reader.SafeGetString(4);
                                        var postalcode = reader.SafeGetString(5);
                                        if (!string.IsNullOrEmpty(flatnumber))
                                            list1.Add($"Квартира {flatnumber}");
                                        if (!string.IsNullOrEmpty(roomnumber))
                                            list1.Add($"Комната {roomnumber}");
                                        docs1.Add(new Doc1
                                        {
                                            id = reader.GetInt64(0),
                                            addrfull = string.Join(" ", list1),
                                            guid = guid,
                                            parentguid = parentguid,
                                            building = 2,
                                            postalcode = postalcode
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
                                            var postalcode = reader2.SafeGetString(6);
                                            var list1 = new List<string> { name };
                                            if (!string.IsNullOrEmpty(housenum)) list1.Add($"{housenum}");
                                            if (!string.IsNullOrEmpty(buildnum)) list1.Add($"к{buildnum}");
                                            if (!string.IsNullOrEmpty(strucnum)) list1.Add($"с{strucnum}");

                                            docs2.Add(new Doc2
                                            {
                                                guid = reader2.SafeGetString(0),
                                                addrfull = string.Join(" ", list1),
                                                parentguid = parentguid,
                                                postalcode = postalcode
                                            });
                                        }
                                    }

                                    var q = from doc1 in docs1
                                        join doc2 in docs2 on doc1.parentguid equals doc2.guid
                                        select new { doc1, doc2 };

                                    foreach (var pair in q)
                                    {
                                        pair.doc1.parentguid = pair.doc2.parentguid;
                                        pair.doc1.addrfull = $"{pair.doc2.addrfull}, {pair.doc1.addrfull}";
                                        if (string.IsNullOrEmpty(pair.doc1.postalcode))
                                            pair.doc1.postalcode = pair.doc2.postalcode;
                                    }
                                }


                                if (docs1.Any())
                                {
                                    ProcessDoc1(docs1, command3, mySqlConnection, true);

                                    lock (obj)
                                    {
                                        current += docs1.Count();

                                        _progressClient.Progress(100f * current / total, id, session)
                                            .GetAwaiter()
                                            .GetResult();
                                    }
                                }
                            }
                        }

                        npgsqlConnection3.Close();
                        npgsqlConnection2.Close();
                        mySqlConnection.TryClose();
                    });
            }

            SetLastRecordNumber(npgsqlConnection, FiasServiceType.Room, next_last_record_number);

            await npgsqlConnection.CloseAsync();

            await _progressClient.Finalize(id, session);
        }

        private async Task UpdateSteadAsync(string session, bool full)
        {
            await using var npgsqlConnection = new NpgsqlConnection(GetFiasConnectionString());
            var current = 0L;
            var total = 0L;

            var id = Guid.NewGuid().ToString();
            await _progressClient.Init(id, session);

            await npgsqlConnection.OpenAsync();

            npgsqlConnection.ReloadTypes();
            npgsqlConnection.TypeMapper.MapEnum<FiasServiceType>("service_type");

            var last_record_number = GetLastRecordNumber(npgsqlConnection, FiasServiceType.Stead, full);
            var next_last_record_number = GetNextLastRecordNumber(npgsqlConnection);

            var list2 = new List<string>();
            var list = new List<string>();

            await using (var command = new NpgsqlCommand(
                             string.Join(";", new[] { @"addrob\d+", @"stead\d+" }.Select(x =>
                                 $"SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' and table_name similar to '{x}'")),
                             npgsqlConnection))
            {
                await command.PrepareAsync();

                await using var reader = command.ExecuteReader();
                list2.Fill(reader);
                reader.NextResult();
                list.Fill(reader);
            }

            var sql2 = string.Join("\nUNION ALL\n",
                list2.Select(x =>
                    $@"SELECT {x}.aoguid,offname,formalname,shortname,socrbase.socrname,aolevel,parentguid,postalcode FROM {x}
                        JOIN socrbase ON {x}.shortname=socrbase.scname AND {x}.aolevel=socrbase.level
                        WHERE {x}.aoguid=ANY(@guids) AND {x}.livestatus=1"));

            var sql1 = string.Join("\nUNION ALL\n",
                list.Select(x =>
                    $"SELECT COUNT(*) FROM {x} WHERE record_number>@last_record_number AND livestatus=1"));

            var sql = string.Join("\nUNION ALL\n",
                list.Select(x =>
                    $"SELECT record_id,number,parentguid,steadguid,postalcode FROM {x} WHERE record_number>@last_record_number AND livestatus=1"));

            await using (var command = new NpgsqlCommand(string.Join(";", sql1, sql), npgsqlConnection))
            {
                command.Parameters.AddWithValue("last_record_number", last_record_number);

                await command.PrepareAsync();

                await using var reader = command.ExecuteReader();
                while (reader.Read()) total += reader.GetInt64(0);

                var take = 100;

                reader.NextResult();

                var obj = new object();
                var reader_is_empty = false;

                Parallel.For(0, _parallelConfig.GetNumberOfThreads(),
                    i =>
                    {
                        using var mySqlConnection = new MySqlConnection(GetSphinxConnectionString());
                        using var npgsqlConnection2 = new NpgsqlConnection(GetFiasConnectionString());
                        npgsqlConnection2.Open();

                        using (var command2 = new NpgsqlCommand(sql2, npgsqlConnection2))
                        {
                            command2.Parameters.Add("guids", NpgsqlDbType.Array | NpgsqlDbType.Varchar);

                            command2.Prepare();

                            for (;;)
                            {
                                List<Doc1> docs1;
                                lock (obj)
                                {
                                    if (reader_is_empty) break;
                                    docs1 = ReadDocs1(reader, take, 1, true);

                                    reader_is_empty = docs1.Count() < take;
                                    if (!docs1.Any()) break;
                                }

                                if (docs1.Any())
                                {
                                    ProcessDoc1(docs1, command2, mySqlConnection, false);

                                    lock (obj)
                                    {
                                        current += docs1.Count();

                                        _progressClient.Progress(100f * current / total, id, session)
                                            .GetAwaiter()
                                            .GetResult();
                                    }
                                }
                            }
                        }

                        npgsqlConnection2.Close();
                        mySqlConnection.TryClose();
                    });
            }

            SetLastRecordNumber(npgsqlConnection, FiasServiceType.Stead, next_last_record_number);

            await npgsqlConnection.CloseAsync();

            await _progressClient.Finalize(id, session);
        }

        public List<Doc1> ReadDocs1(NpgsqlDataReader reader, int take, int building, bool use_postalcode)
        {
            var result = new List<Doc1>(take);
            for (var i = 0; i < take && reader.Read(); i++)
                result.Add(new Doc1
                {
                    id = reader.GetInt64(0),
                    addrfull = reader.SafeGetString(1),
                    parentguid = reader.SafeGetString(2),
                    guid = reader.SafeGetString(3),
                    postalcode = use_postalcode ? reader.SafeGetString(4) : null,
                    building = building
                });
            return result;
        }

        private void ProcessDoc1(List<Doc1> docs1, NpgsqlCommand command2, MySqlConnection mySqlConnection,
            bool use_postalcode)
        {
            var guids1 = new List<string>();

            for (var guids = docs1.Where(x => !string.IsNullOrEmpty(x.parentguid))
                     .Select(x => x.parentguid).ToArray();
                 guids.Any();
                 guids = docs1.Where(x => !string.IsNullOrEmpty(x.parentguid))
                     .Select(x => x.parentguid).ToArray())
            {
                guids = guids.Except(guids1).ToArray();

                if (!guids.Any()) break;

                var docs2 = GetDocs2(guids, command2, guids.Length, use_postalcode);

                if (!docs2.Any()) break;

                var q = from doc1 in docs1
                    join doc2 in docs2 on doc1.parentguid equals doc2.guid
                    select new { doc1, doc2 };

                foreach (var pair in q)
                {
                    pair.doc1.parentguid = pair.doc2.parentguid;
                    pair.doc1.addrfull = $"{pair.doc2.addrfull}, {pair.doc1.addrfull}";
                    if (string.IsNullOrEmpty(pair.doc1.postalcode)) pair.doc1.postalcode = pair.doc2.postalcode;
                }

                guids1.AddRange(guids);
            }

            foreach (var doc1 in docs1)
                if (!string.IsNullOrEmpty(doc1.postalcode))
                    doc1.addrfull = $"{doc1.postalcode}, {doc1.addrfull}";

            var sb = new StringBuilder(
                "REPLACE INTO addrobx(id,title,guid,priority,building) VALUES ");
            sb.Append(string.Join(",",
                docs1.Select(x =>
                    $"({x.id},'{x.addrfull.TextEscape()}','{x.guid}',{x.addrfull.Split(",").Length},{x.building})")));

            ExecuteNonQueryWithRepeatOnError(sb.ToString(), mySqlConnection);
        }

        private List<Doc2> GetDocs2(string[] guids, NpgsqlCommand npgsqlCommand2, int take, bool use_postalcode)
        {
            var socr = true;
            var formal = false;

            var docs2 = new List<Doc2>(take);

            npgsqlCommand2.Parameters["guids"].Value = guids;

            using var reader2 = npgsqlCommand2.ExecuteReader();
            while (reader2.Read())
            {
                var offname = reader2.SafeGetString(1);
                var formalname = reader2.SafeGetString(2);
                var shortname = reader2.SafeGetString(3);
                var socrname = reader2.SafeGetString(4);
                var aolevel = reader2.GetInt32(5);
                var parentguid = reader2.SafeGetString(6);
                var postalcode = use_postalcode ? reader2.SafeGetString(7) : null;
                var title = aolevel > 1
                    ? $"{(socr ? socrname : shortname)} {(formal ? formalname : offname)}"
                    : formal
                        ? formalname
                        : offname;

                docs2.Add(new Doc2
                {
                    guid = reader2.SafeGetString(0),
                    addrfull = title,
                    parentguid = parentguid,
                    postalcode = postalcode
                });
            }

            return docs2;
        }
    }
}