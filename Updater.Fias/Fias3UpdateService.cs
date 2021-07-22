using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Npgsql;
using NpgsqlTypes;
using Placium.Common;
using Placium.Types;

namespace Updater.Fias
{
    public class Fias3UpdateService : BaseService, IUpdateService
    {
        private readonly ProgressHub _progressHub;

        public Fias3UpdateService(ProgressHub progressHub, IConfiguration configuration) : base(configuration)
        {
            _progressHub = progressHub;
        }

        public async Task UpdateAsync(string session, bool full)
        {
            using (var npgsqlConnection = new NpgsqlConnection(GetFiasConnectionString()))
            {
                await npgsqlConnection.OpenAsync();
                if (full)
                    await ExecuteResourceAsync(Assembly.GetExecutingAssembly(), "Updater.Fias.CreateAddresxTables.sql",
                        npgsqlConnection);
                await npgsqlConnection.CloseAsync();
            }

            if (full)
                using (var npgsqlConnection = new NpgsqlConnection(GetFiasConnectionString()))
                {
                    await npgsqlConnection.OpenAsync();
                    npgsqlConnection.ReloadTypes();
                    npgsqlConnection.TypeMapper.MapEnum<FiasServiceType3>("service_type3");

                    SetLastRecordNumber(npgsqlConnection, FiasServiceType3.Addrob, 0);
                    SetLastRecordNumber(npgsqlConnection, FiasServiceType3.House, 0);
                    SetLastRecordNumber(npgsqlConnection, FiasServiceType3.Stead, 0);
                    SetLastRecordNumber(npgsqlConnection, FiasServiceType3.Room, 0);

                    await npgsqlConnection.CloseAsync();
                }

            await UpdateAddrobAsync(session, full);
            await UpdateHouseAsync(session, full);
            await UpdateSteadAsync(session, full);
            //await UpdateRoomAsync(session, full);
        }

        private async Task UpdateAddrobAsync(string session, bool full)
        {
            var socr = true;
            var formal = false;

            using (var npgsqlConnection3 = new NpgsqlConnection(GetFiasConnectionString()))
            using (var npgsqlConnection = new NpgsqlConnection(GetFiasConnectionString()))
            {
                var current = 0L;
                var total = 0L;

                var id = Guid.NewGuid().ToString();
                await _progressHub.InitAsync(id, session);

                await npgsqlConnection3.OpenAsync();
                await npgsqlConnection.OpenAsync();

                npgsqlConnection.ReloadTypes();
                npgsqlConnection.TypeMapper.MapEnum<FiasServiceType3>("service_type3");

                await ExecuteResourceAsync(Assembly.GetExecutingAssembly(),
                    "Updater.Fias.CreateAddresxTempTables.sql",
                    npgsqlConnection3);

                var last_record_number = GetLastRecordNumber(npgsqlConnection, FiasServiceType3.Addrob, full);
                var next_last_record_number = GetNextLastRecordNumber(npgsqlConnection);

                var list = new List<string>();
                list.Fill(
                    @"SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' and table_name similar to 'addrob\d+'",
                    npgsqlConnection);

                var sql2 = string.Join("\nUNION ALL\n",
                    list.Select(x =>
                        $@"SELECT {x}.aoguid,offname,formalname,shortname,socrbase.socrname,aolevel,parentguid,regioncode,postalcode FROM {x}
                        JOIN socrbase ON {x}.shortname=socrbase.scname AND {x}.aolevel=socrbase.level
                        WHERE {x}.aoguid=ANY(@guids) AND {x}.livestatus=1"));

                var sql1 = string.Join("\nUNION ALL\n",
                    list.Select(x =>
                        $"SELECT COUNT(*) FROM {x} WHERE {x}.livestatus=1 AND {x}.record_number>@last_record_number"));

                var sql = string.Join("\nUNION ALL\n",
                    list.Select(x =>
                        $@"SELECT {x}.record_id,offname,formalname,shortname,socrbase.socrname,aolevel,parentguid,regioncode,postalcode,{x}.aoguid FROM {x}
                        JOIN socrbase ON {x}.shortname=socrbase.scname AND {x}.aolevel=socrbase.level
                        WHERE {x}.livestatus=1 AND {x}.record_number>@last_record_number"));

                await ExecuteResourceAsync(Assembly.GetExecutingAssembly(), "Updater.Fias.CreateAddrxTempTables.sql",
                    npgsqlConnection3);

                using (var writer = npgsqlConnection3.BeginTextImport(
                    "COPY temp_addresx (id,title,priority,addressString,postalCode,regionCode,country,geoLon,geoLat,geoExists,building,guid) FROM STDIN WITH NULL AS ''")
                )
                {
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
                                                        var regioncode = reader.SafeGetString(7);
                                                        var postalcode = reader.SafeGetString(8);
                                                        var guid = reader.SafeGetString(9);
                                                        var addrfull = aolevel > 1
                                                            ? $"{(socr ? socrname : shortname)} {(formal ? formalname : offname)}"
                                                            : formal
                                                                ? formalname
                                                                : offname;
                                                        var addrshort = offname;
                                                        docs1.Add(new Doc1
                                                        {
                                                            id = reader.GetInt64(0),
                                                            addrfull = addrfull,
                                                            parentguid = parentguid,
                                                            addrshort = addrshort,
                                                            regioncode = regioncode,
                                                            postalcode = postalcode,
                                                            guid = guid
                                                        });
                                                    }

                                                    reader_is_empty = docs1.Count() < take;
                                                    if (!docs1.Any()) break;
                                                }

                                                if (docs1.Any())
                                                {
                                                    ProcessDoc1(docs1, command2);

                                                    lock (obj)
                                                    {
                                                        foreach (var doc in docs1)
                                                        {
                                                            var values = new[]
                                                            {
                                                                doc.id.ToString(),
                                                                doc.addrshort.ValueAsText(),
                                                                doc.addrshort.Split(",").Length.ToString(),
                                                                doc.addrfull.ValueAsText(),
                                                                doc.postalcode.ValueAsText(),
                                                                doc.regioncode.ValueAsText(),
                                                                "RU".ValueAsText(),
                                                                doc.lon.ValueAsText(),
                                                                doc.lat.ValueAsText(),
                                                                doc.geoexists.ToString(),
                                                                doc.building.ToString(),
                                                                doc.guid.ValueAsText()
                                                            };

                                                            writer.WriteLine(string.Join("\t", values));
                                                        }

                                                        current += docs1.Count();

                                                        _progressHub.ProgressAsync(100f * current / total, id, session)
                                                            .GetAwaiter()
                                                            .GetResult();
                                                    }
                                                }
                                            }
                                        }

                                        npgsqlConnection2.Close();
                                    }
                                });
                        }
                    }
                }

                await ExecuteResourceAsync(Assembly.GetExecutingAssembly(),
                    "Updater.Fias.InsertAddresxFromTempTables.sql",
                    npgsqlConnection3);

                SetLastRecordNumber(npgsqlConnection, FiasServiceType3.Addrob, next_last_record_number);

                await npgsqlConnection.CloseAsync();
                await npgsqlConnection3.CloseAsync();

                await _progressHub.ProgressAsync(100f, id, session);
            }
        }

        private async Task UpdateHouseAsync(string session, bool full)
        {
            using (var npgsqlConnection3 = new NpgsqlConnection(GetFiasConnectionString()))
            using (var npgsqlConnection = new NpgsqlConnection(GetFiasConnectionString()))
            {
                var current = 0L;
                var total = 0L;

                var id = Guid.NewGuid().ToString();
                await _progressHub.InitAsync(id, session);

                await npgsqlConnection3.OpenAsync();
                await npgsqlConnection.OpenAsync();

                npgsqlConnection.ReloadTypes();
                npgsqlConnection.TypeMapper.MapEnum<FiasServiceType3>("service_type3");

                await ExecuteResourceAsync(Assembly.GetExecutingAssembly(),
                    "Updater.Fias.CreateAddresxTempTables.sql",
                    npgsqlConnection3);

                var last_record_number = GetLastRecordNumber(npgsqlConnection, FiasServiceType3.House, full);
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
                        $@"SELECT {x}.aoguid,offname,formalname,shortname,socrbase.socrname,aolevel,parentguid,regioncode,postalcode FROM {x}
                        JOIN socrbase ON {x}.shortname=socrbase.scname AND {x}.aolevel=socrbase.level
                        WHERE {x}.aoguid=ANY(@guids) AND {x}.livestatus=1"));

                var sql1 = string.Join("\nUNION ALL\n",
                    list.Select(x =>
                        $"SELECT COUNT(*) FROM {x} JOIN (SELECT now() as n) as q ON startdate<=n AND n<enddate WHERE {x}.record_number>@last_record_number"));

                var sql = string.Join("\nUNION ALL\n",
                    list.Select(x =>
                        $@"SELECT {x}.record_id,housenum,buildnum,strucnum,eststat.name,aoguid,postalcode,{x}.houseguid FROM {x}
                        JOIN (SELECT now() as n) as q ON startdate<=n AND n<enddate 
                        JOIN eststat ON {x}.eststatus=eststat.eststatid
                        WHERE {x}.record_number>@last_record_number"));

                using (var writer = npgsqlConnection3.BeginTextImport(
                    "COPY temp_addresx (id,title,priority,addressString,postalCode,regionCode,country,geoLon,geoLat,geoExists,building,guid) FROM STDIN WITH NULL AS ''")
                )
                {
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
                                    using (var npgsqlConnection2 = new NpgsqlConnection(GetFiasConnectionString()))
                                    {
                                        npgsqlConnection2.Open();

                                        using (var command2 = new NpgsqlCommand(sql2, npgsqlConnection2))
                                        {
                                            command2.Parameters.Add("guids", NpgsqlDbType.Array | NpgsqlDbType.Varchar);

                                            command2.Prepare();

                                            while (true)
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
                                                        var postalcode = reader.SafeGetString(6);
                                                        var guid = reader.SafeGetString(7);
                                                        var list1 = new List<string> {name};
                                                        var list11 = new List<string>();
                                                        if (!string.IsNullOrEmpty(housenum)) list1.Add($"{housenum}");
                                                        if (!string.IsNullOrEmpty(buildnum)) list1.Add($"к{buildnum}");
                                                        if (!string.IsNullOrEmpty(strucnum)) list1.Add($"с{strucnum}");
                                                        if (!string.IsNullOrEmpty(housenum)) list11.Add($"{housenum}");
                                                        if (!string.IsNullOrEmpty(buildnum)) list11.Add($"к{buildnum}");
                                                        if (!string.IsNullOrEmpty(strucnum)) list11.Add($"с{strucnum}");

                                                        docs1.Add(new Doc1
                                                        {
                                                            id = reader.GetInt64(0),
                                                            housesteadfull = string.Join(" ", list1),
                                                            housesteadshort = string.Join(" ", list11),
                                                            parentguid = parentguid,
                                                            postalcode = postalcode,
                                                            building = 1,
                                                            guid = guid
                                                        });
                                                    }

                                                    reader_is_empty = docs1.Count() < take;
                                                    if (!docs1.Any()) break;
                                                }

                                                if (docs1.Any())
                                                {
                                                    ProcessDoc1(docs1, command2);

                                                    lock (obj)
                                                    {
                                                        foreach (var doc in docs1)
                                                        {
                                                            var values = new[]
                                                            {
                                                                doc.id.ToString(),
                                                                doc.addrshort.ValueAsText(),
                                                                doc.addrshort.Split(",").Length.ToString(),
                                                                doc.addrfull.ValueAsText(),
                                                                doc.postalcode.ValueAsText(),
                                                                doc.regioncode.ValueAsText(),
                                                                "RU".ValueAsText(),
                                                                doc.lon.ValueAsText(),
                                                                doc.lat.ValueAsText(),
                                                                doc.geoexists.ToString(),
                                                                doc.building.ToString(),
                                                                doc.guid.ValueAsText()
                                                            };

                                                            writer.WriteLine(string.Join("\t", values));
                                                        }

                                                        current += docs1.Count();

                                                        _progressHub.ProgressAsync(100f * current / total, id, session)
                                                            .GetAwaiter()
                                                            .GetResult();
                                                    }
                                                }
                                            }
                                        }

                                        npgsqlConnection2.Close();
                                    }
                                });
                        }
                    }
                }

                await ExecuteResourceAsync(Assembly.GetExecutingAssembly(),
                    "Updater.Fias.InsertAddresxFromTempTables.sql",
                    npgsqlConnection3);

                SetLastRecordNumber(npgsqlConnection, FiasServiceType3.House, next_last_record_number);

                await npgsqlConnection.CloseAsync();
                await npgsqlConnection3.CloseAsync();

                await _progressHub.ProgressAsync(100f, id, session);
            }
        }

        private async Task UpdateRoomAsync(string session, bool full)
        {
            using (var npgsqlConnection4 = new NpgsqlConnection(GetFiasConnectionString()))
            using (var npgsqlConnection = new NpgsqlConnection(GetFiasConnectionString()))
            {
                var current = 0L;
                var total = 0L;

                var id = Guid.NewGuid().ToString();
                await _progressHub.InitAsync(id, session);

                await npgsqlConnection4.OpenAsync();
                await npgsqlConnection.OpenAsync();

                npgsqlConnection.ReloadTypes();
                npgsqlConnection.TypeMapper.MapEnum<FiasServiceType3>("service_type3");

                await ExecuteResourceAsync(Assembly.GetExecutingAssembly(),
                    "Updater.Fias.CreateAddresxTempTables.sql",
                    npgsqlConnection4);

                var last_record_number = GetLastRecordNumber(npgsqlConnection, FiasServiceType3.Room, full);
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
                        $@"SELECT {x}.aoguid,offname,formalname,shortname,socrbase.socrname,aolevel,parentguid,regioncode,postalcode FROM {x}
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

                using (var writer = npgsqlConnection4.BeginTextImport(
                    "COPY temp_addresx (id,title,priority,addressString,postalCode,regionCode,country,geoLon,geoLat,geoExists,building,guid) FROM STDIN WITH NULL AS ''")
                )
                {
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
                                                var docs1 = new List<Doc1>(take);

                                                lock (obj)
                                                {
                                                    if (reader_is_empty) break;
                                                    for (var j = 0; j < take && reader.Read(); j++)
                                                    {
                                                        var list1 = new List<string>();
                                                        var list11 = new List<string>();
                                                        var flatnumber = reader.SafeGetString(1);
                                                        var roomnumber = reader.SafeGetString(2);
                                                        var parentguid = reader.SafeGetString(3);
                                                        var guid = reader.SafeGetString(4);
                                                        var postalcode = reader.SafeGetString(5);
                                                        if (!string.IsNullOrEmpty(flatnumber))
                                                            list1.Add($"квартира {flatnumber}");
                                                        if (!string.IsNullOrEmpty(roomnumber))
                                                            list1.Add($"комната {roomnumber}");
                                                        if (!string.IsNullOrEmpty(flatnumber))
                                                            list11.Add($"квартира {flatnumber}");
                                                        if (!string.IsNullOrEmpty(roomnumber))
                                                            list11.Add($"комната {roomnumber}");
                                                        docs1.Add(new Doc1
                                                        {
                                                            id = reader.GetInt64(0),
                                                            roomshort = string.Join(" ", list11),
                                                            roomfull = string.Join(" ", list1),
                                                            parentguid = parentguid,
                                                            guid = guid,
                                                            building = 1,
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
                                                            var list1 = new List<string> {name};
                                                            var list11 = new List<string>();
                                                            if (!string.IsNullOrEmpty(housenum))
                                                                list1.Add($"{housenum}");
                                                            if (!string.IsNullOrEmpty(buildnum))
                                                                list1.Add($"к{buildnum}");
                                                            if (!string.IsNullOrEmpty(strucnum))
                                                                list1.Add($"с{strucnum}");
                                                            if (!string.IsNullOrEmpty(housenum))
                                                                list11.Add($"{housenum}");
                                                            if (!string.IsNullOrEmpty(buildnum))
                                                                list11.Add($"к{buildnum}");
                                                            if (!string.IsNullOrEmpty(strucnum))
                                                                list11.Add($"с{strucnum}");

                                                            docs2.Add(new Doc2
                                                            {
                                                                guid = reader2.SafeGetString(0),
                                                                housefull = string.Join(" ", list1),
                                                                houseshort = string.Join(" ", list11),
                                                                parentguid = parentguid,
                                                                postalcode = postalcode
                                                            });
                                                        }
                                                    }

                                                    var q = from doc1 in docs1
                                                        join doc2 in docs2 on doc1.parentguid equals doc2.guid
                                                        select new {doc1, doc2};

                                                    foreach (var pair in q)
                                                    {
                                                        pair.doc1.parentguid = pair.doc2.parentguid;
                                                        pair.doc1.postalcode =
                                                            string.IsNullOrWhiteSpace(pair.doc1.postalcode)
                                                                ? pair.doc2.postalcode
                                                                : pair.doc1.postalcode;
                                                        pair.doc1.regioncode =
                                                            string.IsNullOrWhiteSpace(pair.doc1.regioncode)
                                                                ? pair.doc2.regioncode
                                                                : pair.doc1.regioncode;
                                                        pair.doc1.housesteadfull =
                                                            string.IsNullOrWhiteSpace(pair.doc1.housesteadfull)
                                                                ? pair.doc2.housefull
                                                                : pair.doc1.housesteadfull;
                                                        pair.doc1.housesteadshort =
                                                            string.IsNullOrWhiteSpace(pair.doc1.housesteadshort)
                                                                ? pair.doc2.houseshort
                                                                : pair.doc1.housesteadshort;
                                                    }
                                                }


                                                if (docs1.Any())
                                                {
                                                    ProcessDoc1(docs1, command3);

                                                    lock (obj)
                                                    {
                                                        foreach (var doc in docs1)
                                                        {
                                                            var values = new[]
                                                            {
                                                                doc.id.ToString(),
                                                                doc.addrshort.ValueAsText(),
                                                                doc.addrshort.Split(",").Length.ToString(),
                                                                doc.addrfull.ValueAsText(),
                                                                doc.postalcode.ValueAsText(),
                                                                doc.regioncode.ValueAsText(),
                                                                "RU".ValueAsText(),
                                                                doc.lon.ValueAsText(),
                                                                doc.lat.ValueAsText(),
                                                                doc.geoexists.ToString(),
                                                                doc.building.ToString(),
                                                                doc.guid.ValueAsText()
                                                            };

                                                            writer.WriteLine(string.Join("\t", values));
                                                        }

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
                                    }
                                });
                        }
                    }
                }

                await ExecuteResourceAsync(Assembly.GetExecutingAssembly(),
                    "Updater.Fias.InsertAddresxFromTempTables.sql",
                    npgsqlConnection4);

                SetLastRecordNumber(npgsqlConnection, FiasServiceType3.Room, next_last_record_number);

                await npgsqlConnection.CloseAsync();
                await npgsqlConnection4.CloseAsync();

                await _progressHub.ProgressAsync(100f, id, session);
            }
        }

        private async Task UpdateSteadAsync(string session, bool full)
        {
            using (var npgsqlConnection3 = new NpgsqlConnection(GetFiasConnectionString()))
            using (var npgsqlConnection = new NpgsqlConnection(GetFiasConnectionString()))
            {
                var current = 0L;
                var total = 0L;

                var id = Guid.NewGuid().ToString();
                await _progressHub.InitAsync(id, session);

                await npgsqlConnection3.OpenAsync();
                await npgsqlConnection.OpenAsync();

                npgsqlConnection.ReloadTypes();
                npgsqlConnection.TypeMapper.MapEnum<FiasServiceType3>("service_type3");

                await ExecuteResourceAsync(Assembly.GetExecutingAssembly(),
                    "Updater.Fias.CreateAddresxTempTables.sql",
                    npgsqlConnection3);

                var last_record_number = GetLastRecordNumber(npgsqlConnection, FiasServiceType3.Stead, full);
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
                        $@"SELECT {x}.aoguid,offname,formalname,shortname,socrbase.socrname,aolevel,parentguid,regioncode,postalcode FROM {x}
                        JOIN socrbase ON {x}.shortname=socrbase.scname AND {x}.aolevel=socrbase.level
                        WHERE {x}.aoguid=ANY(@guids) AND {x}.livestatus=1"));

                var sql1 = string.Join("\nUNION ALL\n",
                    list.Select(x =>
                        $"SELECT COUNT(*) FROM {x} WHERE record_number>@last_record_number AND livestatus=1"));

                var sql = string.Join("\nUNION ALL\n",
                    list.Select(x =>
                        $"SELECT record_id,number,parentguid,steadguid,regioncode,postalcode FROM {x} WHERE record_number>@last_record_number AND livestatus=1"));

                using (var writer = npgsqlConnection3.BeginTextImport(
                    "COPY temp_addresx (id,title,priority,addressString,postalCode,regionCode,country,geoLon,geoLat,geoExists,building,guid) FROM STDIN WITH NULL AS ''")
                )
                {
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
                                    using (var npgsqlConnection2 = new NpgsqlConnection(GetFiasConnectionString()))
                                    {
                                        npgsqlConnection2.Open();

                                        using (var command2 = new NpgsqlCommand(sql2, npgsqlConnection2))
                                        {
                                            command2.Parameters.Add("guids", NpgsqlDbType.Array | NpgsqlDbType.Varchar);

                                            command2.Prepare();

                                            while (true)
                                            {
                                                var docs1 = new List<Doc1>(take);

                                                lock (obj)
                                                {
                                                    if (reader_is_empty) break;

                                                    for (var j = 0; j < take && reader.Read(); j++)
                                                        docs1.Add(new Doc1
                                                        {
                                                            id = reader.GetInt64(0),
                                                            housesteadfull = reader.SafeGetString(1),
                                                            parentguid = reader.SafeGetString(2),
                                                            guid = reader.SafeGetString(3),
                                                            regioncode = reader.SafeGetString(4),
                                                            postalcode = reader.SafeGetString(5),
                                                            building = 1
                                                        });

                                                    reader_is_empty = docs1.Count() < take;
                                                    if (!docs1.Any()) break;
                                                }

                                                if (docs1.Any())
                                                {
                                                    ProcessDoc1(docs1, command2);

                                                    lock (obj)
                                                    {
                                                        foreach (var doc in docs1)
                                                        {
                                                            var values = new[]
                                                            {
                                                                doc.id.ToString(),
                                                                doc.addrshort.ValueAsText(),
                                                                doc.addrshort.Split(",").Length.ToString(),
                                                                doc.addrfull.ValueAsText(),
                                                                doc.postalcode.ValueAsText(),
                                                                doc.regioncode.ValueAsText(),
                                                                "RU".ValueAsText(),
                                                                doc.lon.ValueAsText(),
                                                                doc.lat.ValueAsText(),
                                                                doc.geoexists.ToString(),
                                                                doc.building.ToString(),
                                                                doc.guid.ValueAsText()
                                                            };

                                                            writer.WriteLine(string.Join("\t", values));
                                                        }

                                                        current += docs1.Count();

                                                        _progressHub.ProgressAsync(100f * current / total, id, session)
                                                            .GetAwaiter()
                                                            .GetResult();
                                                    }
                                                }
                                            }
                                        }

                                        npgsqlConnection2.Close();
                                    }
                                });
                        }
                    }
                }

                await ExecuteResourceAsync(Assembly.GetExecutingAssembly(),
                    "Updater.Fias.InsertAddresxFromTempTables.sql",
                    npgsqlConnection3);

                SetLastRecordNumber(npgsqlConnection, FiasServiceType3.Stead, next_last_record_number);

                await npgsqlConnection.CloseAsync();
                await npgsqlConnection3.CloseAsync();

                await _progressHub.ProgressAsync(100f, id, session);
            }
        }

        private void ProcessDoc1(List<Doc1> docs1, NpgsqlCommand command2)
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

                var docs2 = GetDocs2(guids, command2, guids.Length);

                if (!docs2.Any()) break;

                var q = from doc1 in docs1
                    join doc2 in docs2 on doc1.parentguid equals doc2.guid
                    select new {doc1, doc2};

                foreach (var pair in q)
                {
                    pair.doc1.parentguid = pair.doc2.parentguid;
                    pair.doc1.addrfull = string.IsNullOrEmpty(pair.doc1.addrfull)
                        ? pair.doc2.addrfull
                        : $"{pair.doc2.addrfull}, {pair.doc1.addrfull}";
                    pair.doc1.addrshort = string.IsNullOrEmpty(pair.doc1.addrshort)
                        ? pair.doc2.addrshort
                        : $"{pair.doc2.addrshort}, {pair.doc1.addrshort}";
                    pair.doc1.postalcode =
                        string.IsNullOrWhiteSpace(pair.doc1.postalcode)
                            ? pair.doc2.postalcode
                            : pair.doc1.postalcode;
                    pair.doc1.regioncode =
                        string.IsNullOrWhiteSpace(pair.doc1.regioncode)
                            ? pair.doc2.regioncode
                            : pair.doc1.regioncode;
                    pair.doc1.housesteadfull = string.IsNullOrWhiteSpace(pair.doc1.housesteadfull)
                        ? pair.doc2.housefull
                        : pair.doc1.housesteadfull;
                    pair.doc1.housesteadshort = string.IsNullOrWhiteSpace(pair.doc1.housesteadshort)
                        ? pair.doc2.houseshort
                        : pair.doc1.housesteadshort;
                }

                guids1.AddRange(guids);
            }

            foreach (var doc1 in docs1)
            {
                if (!string.IsNullOrWhiteSpace(doc1.housesteadfull))
                {
                    doc1.addrfull = $"{doc1.addrfull}, {doc1.housesteadfull}";
                    doc1.building = 1;
                }

                if (!string.IsNullOrWhiteSpace(doc1.housesteadshort))
                {
                    doc1.addrshort = $"{doc1.addrshort}, {doc1.housesteadshort}";
                    doc1.building = 1;
                }

                if (!string.IsNullOrWhiteSpace(doc1.roomfull)) doc1.addrfull = $"{doc1.addrfull}, {doc1.roomfull}";

                if (!string.IsNullOrWhiteSpace(doc1.roomshort)) doc1.addrshort = $"{doc1.addrshort}, {doc1.roomshort}";

                if (!string.IsNullOrWhiteSpace(doc1.postalcode)) doc1.addrfull = $"{doc1.postalcode}, {doc1.addrfull}";
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
                    var regioncode = reader2.SafeGetString(7);
                    var postalcode = reader2.SafeGetString(8);
                    var addrfull = aolevel > 1
                        ? $"{(socr ? socrname : shortname)} {(formal ? formalname : offname)}"
                        : formal
                            ? formalname
                            : offname;
                    var addrshort = offname;

                    docs2.Add(new Doc2
                    {
                        guid = reader2.SafeGetString(0),
                        addrfull = addrfull,
                        addrshort = addrshort,
                        regioncode = regioncode,
                        postalcode = postalcode,
                        parentguid = parentguid
                    });
                }
            }

            return docs2;
        }
    }
}