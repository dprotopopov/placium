using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MySql.Data.MySqlClient;
using Npgsql;
using NpgsqlTypes;
using Placium.Common;
using Placium.Types;

namespace Updater.Garx.Sphinx
{
    public class SphinxGarxUpdateService : BaseAppService, IUpdateService
    {
        private readonly IParallelConfig _parallelConfig;
        private readonly IProgressClient _progressClient;
        private readonly ISphinxConfig _sphinxConfig;

        public SphinxGarxUpdateService(IProgressClient progressClient, IConnectionsConfig configuration,
            IParallelConfig parallelConfig, ISphinxConfig sphinxConfig) : base(
            configuration)
        {
            _progressClient = progressClient;
            _parallelConfig = parallelConfig;
            _sphinxConfig = sphinxConfig;
        }

        public async Task UpdateAsync(string session, bool full)
        {
            await using (var connection = new MySqlConnection(GetSphinxConnectionString()))
            {
                if (full)
                    TryExecuteNonQueries(new[]
                    {
                        "DROP TABLE garx"
                    }, connection);

                TryExecuteNonQueries(new[]
                {
                    "CREATE TABLE garx(title text indexed stored,objectid int,objectguid string,priority int,building int)"
                    + " phrase_boundary='U+2C'"
                    + " phrase_boundary_step='100'"
                    + " min_infix_len='1'"
                    + " expand_keywords='1'"
                    + " charset_table='0..9,A..Z->a..z,a..z,U+410..U+42F->U+430..U+44F,U+430..U+44F,U+401->U+0435,U+451->U+0435'"
                    + " blend_chars = '., +, &->+'"
                    + $" wordforms='{_sphinxConfig.GetWordformsPath("address.txt")}'"
                    + " morphology='stem_ru'"
                }, connection);
            }

            if (full)
            {
                await using var npgsqlConnection = new NpgsqlConnection(GetGarConnectionString());
                await npgsqlConnection.OpenAsync();
                npgsqlConnection.ReloadTypes();
                npgsqlConnection.TypeMapper.MapEnum<GarServiceType>("service_type");

                SetLastRecordNumber(npgsqlConnection, GarServiceType.Addrob, 0);
                SetLastRecordNumber(npgsqlConnection, GarServiceType.House, 0);
                SetLastRecordNumber(npgsqlConnection, GarServiceType.Stead, 0);
                SetLastRecordNumber(npgsqlConnection, GarServiceType.Room, 0);
                SetLastRecordNumber(npgsqlConnection, GarServiceType.Carplace, 0);

                await npgsqlConnection.CloseAsync();
            }

            await UpdateAddrobAsync(session, full);
            await UpdateHouseAsync(session, full);
            await UpdateSteadAsync(session, full);
            await UpdateRoomAsync(session, full);
            await UpdateCarplaceAsync(session, full);

            await using (var connection = new MySqlConnection(GetSphinxConnectionString()))
            {
                TryExecuteNonQueries(new[]
                {
                    "FLUSH RTINDEX garx"
                }, connection);
            }
        }

        private async Task UpdateAddrobAsync(string session, bool full)
        {
            await using var npgsqlConnection = new NpgsqlConnection(GetGarConnectionString());
            var current = 0L;
            var total = 0L;

            var id = Guid.NewGuid().ToString();
            await _progressClient.Init(id, session);

            await npgsqlConnection.OpenAsync();

            npgsqlConnection.ReloadTypes();
            npgsqlConnection.TypeMapper.MapEnum<GarServiceType>("service_type");

            var last_record_number = GetLastRecordNumber(npgsqlConnection, GarServiceType.Addrob, full);
            var next_last_record_number = GetNextLastRecordNumber(npgsqlConnection);

            var sql2 = $@"SELECT t.""OBJECTID"",t.""OBJECTGUID"",t.""NAME"",t.""LEVEL"",h.""PARENTOBJID"" FROM ""AS_ADDR_OBJ"" t 
                        LEFT JOIN ""AS_ADM_HIERARCHY"" h ON t.""OBJECTID""=h.""OBJECTID""
                        WHERE t.""OBJECTID""=ANY(@ids)";

            var sql1 = $@"SELECT COUNT(*) FROM ""AS_ADDR_OBJ"" t WHERE t.record_number>@last_record_number";

            var sql = $@"SELECT t.record_id,t.""OBJECTID"",t.""OBJECTGUID"",t.""NAME"",h.""PARENTOBJID"",t.""LEVEL"",at.""NAME"" FROM ""AS_ADDR_OBJ"" t 
                        LEFT JOIN ""AS_ADM_HIERARCHY"" h ON t.""OBJECTID""=h.""OBJECTID""
                        LEFT JOIN ""AS_ADDR_OBJ_TYPES"" at ON t.""TYPENAME""=at.""ID"" AND t.""LEVEL""=at.""LEVEL""
                        WHERE t.record_number>@last_record_number";

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
                        using var npgsqlConnection2 = new NpgsqlConnection(GetGarConnectionString());
                        npgsqlConnection2.Open();

                        using (var command2 = new NpgsqlCommand(sql2, npgsqlConnection2))
                        {
                            command2.Parameters.Add("ids", NpgsqlDbType.Array | NpgsqlDbType.Bigint);

                            command2.Prepare();

                            for (;;)
                            {
                                var docs1 = new List<Doc1>(take);

                                lock (obj)
                                {
                                    if (reader_is_empty) break;
                                    for (var j = 0; j < take && reader.Read(); j++)
                                    {
                                        var objectid = reader.GetInt64(1);
                                        var objectguid = reader.SafeGetString(2);
                                        var name = reader.SafeGetString(3);
                                        var parentid = reader.SafeGetInt64(4);
                                        var aolevel = reader.GetInt32(5);
                                        var addrtype = reader.SafeGetString(6);
                                        docs1.Add(new Doc1
                                        {
                                            id = reader.GetInt64(0),
                                            addrfull = name,
                                            objectid = objectid,
                                            objectguid = objectguid,
                                            parentid = parentid,
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

            SetLastRecordNumber(npgsqlConnection, GarServiceType.Addrob, next_last_record_number);

            await npgsqlConnection.CloseAsync();

            await _progressClient.Finalize(id, session);
        }

        private async Task UpdateHouseAsync(string session, bool full)
        {
            await using var npgsqlConnection = new NpgsqlConnection(GetGarConnectionString());
            var current = 0L;
            var total = 0L;

            var id = Guid.NewGuid().ToString();
            await _progressClient.Init(id, session);

            await npgsqlConnection.OpenAsync();

            npgsqlConnection.ReloadTypes();
            npgsqlConnection.TypeMapper.MapEnum<GarServiceType>("service_type");

            var last_record_number = GetLastRecordNumber(npgsqlConnection, GarServiceType.House, full);
            var next_last_record_number = GetNextLastRecordNumber(npgsqlConnection);

            var sql2 = $@"SELECT t.""OBJECTID"",t.""OBJECTGUID"",t.""NAME"",h.""PARENTOBJID"" FROM ""AS_ADDR_OBJ"" t 
                        LEFT JOIN ""AS_ADM_HIERARCHY"" h ON t.""OBJECTID""=h.""OBJECTID""
                        WHERE t.""OBJECTID""=ANY(@ids)";

            var sql1 = $@"SELECT COUNT(*) FROM ""AS_HOUSES"" t WHERE t.record_number>@last_record_number";

            var sql = $@"SELECT t.record_id,t.""OBJECTID"",t.""OBJECTGUID"",t.""HOUSENUM"",h.""PARENTOBJID"",ht.""NAME"" FROM ""AS_HOUSES"" t 
                        LEFT JOIN ""AS_ADM_HIERARCHY"" h ON t.""OBJECTID""=h.""OBJECTID""
                        LEFT JOIN ""AS_HOUSE_TYPES"" ht ON t.""HOUSETYPE""=ht.""ID""
                        WHERE t.record_number>@last_record_number";

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
                        using var npgsqlConnection2 = new NpgsqlConnection(GetGarConnectionString());
                        npgsqlConnection2.Open();

                        using (var command2 = new NpgsqlCommand(sql2, npgsqlConnection2))
                        {
                            command2.Parameters.Add("ids", NpgsqlDbType.Array | NpgsqlDbType.Bigint);

                            command2.Prepare();

                            for (;;)
                            {
                                var docs1 = new List<Doc1>(take);

                                lock (obj)
                                {
                                    if (reader_is_empty) break;
                                    for (var j = 0; j < take && reader.Read(); j++)
                                    {
                                        var objectid = reader.GetInt64(1);
                                        var objectguid = reader.SafeGetString(2);
                                        var housenum = reader.SafeGetString(3);
                                        var parentid = reader.SafeGetInt64(4);
                                        var housetype = reader.SafeGetString(5);
                                        docs1.Add(new Doc1
                                        {
                                            id = reader.GetInt64(0),
                                            addrfull = housenum,
                                            objectid = objectid,
                                            objectguid = objectguid,
                                            parentid = parentid,
                                            building = 1,
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

            SetLastRecordNumber(npgsqlConnection, GarServiceType.House, next_last_record_number);

            await npgsqlConnection.CloseAsync();

            await _progressClient.Finalize(id, session);
        }

        private async Task UpdateRoomAsync(string session, bool full)
        {
            await using var npgsqlConnection = new NpgsqlConnection(GetGarConnectionString());
            var current = 0L;
            var total = 0L;

            var id = Guid.NewGuid().ToString();
            await _progressClient.Init(id, session);

            await npgsqlConnection.OpenAsync();

            npgsqlConnection.ReloadTypes();
            npgsqlConnection.TypeMapper.MapEnum<GarServiceType>("service_type");

            var last_record_number = GetLastRecordNumber(npgsqlConnection, GarServiceType.Room, full);
            var next_last_record_number = GetNextLastRecordNumber(npgsqlConnection);

            var sql3 = $@"SELECT t.""OBJECTID"",t.""OBJECTGUID"",t.""NAME"",h.""PARENTOBJID"" FROM ""AS_ADDR_OBJ"" t 
                        LEFT JOIN ""AS_ADM_HIERARCHY"" h ON t.""OBJECTID""=h.""OBJECTID""
                        WHERE t.""OBJECTID""=ANY(@ids)";

            var sql2 = $@"SELECT t.""OBJECTID"",t.""OBJECTGUID"",t.""HOUSENUM"",h.""PARENTOBJID"" FROM ""AS_HOUSES"" t 
                        LEFT JOIN ""AS_ADM_HIERARCHY"" h ON t.""OBJECTID""=h.""OBJECTID""
                        WHERE t.""OBJECTID""=ANY(@ids)";

            var sql1 = $@"SELECT COUNT(*) FROM ""AS_ROOMS"" t WHERE t.record_number>@last_record_number";

            var sql = $@"SELECT t.record_id,t.""OBJECTID"",t.""OBJECTGUID"",t.""NUMBER"",h.""PARENTOBJID"",rt.""NAME"" FROM ""AS_ROOMS"" t 
                        LEFT JOIN ""AS_ADM_HIERARCHY"" h ON t.""OBJECTID""=h.""OBJECTID""
                        LEFT JOIN ""AS_ROOM_TYPES"" rt ON t.""ROOMTYPE""=rt.""ID""
                        WHERE t.record_number>@last_record_number";

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
                        using var npgsqlConnection2 = new NpgsqlConnection(GetGarConnectionString());
                        using var npgsqlConnection3 = new NpgsqlConnection(GetGarConnectionString());
                        npgsqlConnection2.Open();
                        npgsqlConnection3.Open();

                        using (var command2 = new NpgsqlCommand(sql2, npgsqlConnection2))
                        using (var command3 = new NpgsqlCommand(sql3, npgsqlConnection3))
                        {
                            command2.Parameters.Add("ids", NpgsqlDbType.Array | NpgsqlDbType.Bigint);

                            command2.Prepare();

                            command3.Parameters.Add("ids", NpgsqlDbType.Array | NpgsqlDbType.Bigint);

                            command3.Prepare();

                            for (;;)
                            {
                                var docs1 = new List<Doc1>(take);

                                lock (obj)
                                {
                                    if (reader_is_empty) break;
                                    for (var j = 0; j < take && reader.Read(); j++)
                                    {
                                        var objectid = reader.GetInt64(1);
                                        var objectguid = reader.SafeGetString(2);
                                        var number = reader.SafeGetString(3);
                                        var parentid = reader.SafeGetInt64(4);
                                        var roomtype = reader.SafeGetString(5);
                                        var list1 = new List<string> { };
                                        if (!string.IsNullOrEmpty(roomtype)) list1.Add($"{roomtype}");
                                        if (!string.IsNullOrEmpty(number)) list1.Add($"{number}");
                                        docs1.Add(new Doc1
                                        {
                                            id = reader.GetInt64(0),
                                            addrfull = string.Join(" ", list1),
                                            objectid = objectid,
                                            objectguid = objectguid,
                                            parentid = parentid,
                                            building = 1,
                                            postalcode = null
                                        });
                                    }

                                    reader_is_empty = docs1.Count() < take;
                                    if (!docs1.Any()) break;
                                }

                                if (docs1.Any())
                                {
                                    var ids = docs1.Where(x => x.parentid != null).Select(x => x.parentid.Value).ToArray();
                                    var docs2 = new List<Doc2>(take);

                                    command2.Parameters["ids"].Value = ids;

                                    using (var reader2 = command2.ExecuteReader())
                                    {
                                        while (reader2.Read())
                                        {
                                            var objectguid = reader2.SafeGetString(1);
                                            var housenum = reader2.SafeGetString(2);
                                            var parentid = reader2.SafeGetInt64(3);

                                            docs2.Add(new Doc2
                                            {
                                                objectid = reader2.GetInt64(0),
                                                addrfull = housenum,
                                                parentid = parentid,
                                            });
                                        }
                                    }

                                    var q = from doc1 in docs1
                                        join doc2 in docs2 on doc1.parentid equals doc2.objectid
                                        select new { doc1, doc2 };

                                    foreach (var pair in q)
                                    {
                                        pair.doc1.parentid = pair.doc2.parentid;
                                        pair.doc1.addrfull = $"{pair.doc2.addrfull}, {pair.doc1.addrfull}";
                                        if (string.IsNullOrEmpty(pair.doc1.postalcode))
                                            pair.doc1.postalcode = pair.doc2.postalcode;
                                    }
                                }


                                if (docs1.Any())
                                {
                                    ProcessDoc1(docs1, command3, mySqlConnection, false);

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

            SetLastRecordNumber(npgsqlConnection, GarServiceType.Room, next_last_record_number);

            await npgsqlConnection.CloseAsync();

            await _progressClient.Finalize(id, session);
        }

        private async Task UpdateSteadAsync(string session, bool full)
        {
            await using var npgsqlConnection = new NpgsqlConnection(GetGarConnectionString());
            var current = 0L;
            var total = 0L;

            var id = Guid.NewGuid().ToString();
            await _progressClient.Init(id, session);

            await npgsqlConnection.OpenAsync();

            npgsqlConnection.ReloadTypes();
            npgsqlConnection.TypeMapper.MapEnum<GarServiceType>("service_type");

            var last_record_number = GetLastRecordNumber(npgsqlConnection, GarServiceType.Stead, full);
            var next_last_record_number = GetNextLastRecordNumber(npgsqlConnection);

            var sql2 = $@"SELECT t.""OBJECTID"",t.""OBJECTGUID"",t.""NAME"",h.""PARENTOBJID"" FROM ""AS_ADDR_OBJ"" t 
                        LEFT JOIN ""AS_ADM_HIERARCHY"" h ON t.""OBJECTID""=h.""OBJECTID""
                        WHERE t.""OBJECTID""=ANY(@ids)";

            var sql1 = $@"SELECT COUNT(*) FROM ""AS_STEADS"" t WHERE t.record_number>@last_record_number";

            var sql = $@"SELECT t.record_id,t.""OBJECTID"",t.""OBJECTGUID"",t.""HOUSENUM"",h.""PARENTOBJID"" FROM ""AS_STEADS"" t 
                        LEFT JOIN ""AS_ADM_HIERARCHY"" h ON t.""OBJECTID""=h.""OBJECTID""
                        WHERE t.record_number>@last_record_number";

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
                        using var npgsqlConnection2 = new NpgsqlConnection(GetGarConnectionString());
                        npgsqlConnection2.Open();

                        using (var command2 = new NpgsqlCommand(sql2, npgsqlConnection2))
                        {
                            command2.Parameters.Add("ids", NpgsqlDbType.Array | NpgsqlDbType.Bigint);

                            command2.Prepare();

                            for (; ; )
                            {
                                var docs1 = new List<Doc1>(take);

                                lock (obj)
                                {
                                    if (reader_is_empty) break;
                                    for (var j = 0; j < take && reader.Read(); j++)
                                    {
                                        var objectid = reader.GetInt64(1);
                                        var objectguid = reader.SafeGetString(2);
                                        var housenum = reader.SafeGetString(3);
                                        var parentid = reader.SafeGetInt64(4);
                                        docs1.Add(new Doc1
                                        {
                                            id = reader.GetInt64(0),
                                            addrfull = housenum,
                                            objectid = objectid,
                                            objectguid = objectguid,
                                            parentid = parentid,
                                            building = 1,
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

            SetLastRecordNumber(npgsqlConnection, GarServiceType.Stead, next_last_record_number);

            await npgsqlConnection.CloseAsync();

            await _progressClient.Finalize(id, session);
        }


        private async Task UpdateCarplaceAsync(string session, bool full)
        {
            await using var npgsqlConnection = new NpgsqlConnection(GetGarConnectionString());
            var current = 0L;
            var total = 0L;

            var id = Guid.NewGuid().ToString();
            await _progressClient.Init(id, session);

            await npgsqlConnection.OpenAsync();

            npgsqlConnection.ReloadTypes();
            npgsqlConnection.TypeMapper.MapEnum<GarServiceType>("service_type");

            var last_record_number = GetLastRecordNumber(npgsqlConnection, GarServiceType.Carplace, full);
            var next_last_record_number = GetNextLastRecordNumber(npgsqlConnection);

            var sql2 = $@"SELECT t.""OBJECTID"",t.""OBJECTGUID"",t.""NAME"",h.""PARENTOBJID"" FROM ""AS_ADDR_OBJ"" t 
                        LEFT JOIN ""AS_ADM_HIERARCHY"" h ON t.""OBJECTID""=h.""OBJECTID""
                        WHERE t.""OBJECTID""=ANY(@ids)";

            var sql1 = $@"SELECT COUNT(*) FROM ""AS_CARPLACES"" t WHERE t.record_number>@last_record_number";

            var sql = $@"SELECT t.record_id,t.""OBJECTID"",t.""OBJECTGUID"",t.""HOUSENUM"",h.""PARENTOBJID"" FROM ""AS_CARPLACES"" t 
                        LEFT JOIN ""AS_ADM_HIERARCHY"" h ON t.""OBJECTID""=h.""OBJECTID""
                        WHERE t.record_number>@last_record_number";

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
                        using var npgsqlConnection2 = new NpgsqlConnection(GetGarConnectionString());
                        npgsqlConnection2.Open();

                        using (var command2 = new NpgsqlCommand(sql2, npgsqlConnection2))
                        {
                            command2.Parameters.Add("ids", NpgsqlDbType.Array | NpgsqlDbType.Bigint);

                            command2.Prepare();

                            for (; ; )
                            {
                                var docs1 = new List<Doc1>(take);

                                lock (obj)
                                {
                                    if (reader_is_empty) break;
                                    for (var j = 0; j < take && reader.Read(); j++)
                                    {
                                        var objectid = reader.GetInt64(1);
                                        var objectguid = reader.SafeGetString(2);
                                        var housenum = reader.SafeGetString(3);
                                        var parentid = reader.SafeGetInt64(4);
                                        docs1.Add(new Doc1
                                        {
                                            id = reader.GetInt64(0),
                                            addrfull = housenum,
                                            objectid = objectid,
                                            objectguid = objectguid,
                                            parentid = parentid,
                                            building = 1,
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

            SetLastRecordNumber(npgsqlConnection, GarServiceType.Carplace, next_last_record_number);

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
                    objectguid = reader.SafeGetString(1),
                    addrfull = reader.SafeGetString(2),
                    parentid = reader.SafeGetInt64(3),
                    postalcode = use_postalcode ? reader.SafeGetString(4) : null,
                    building = building
                });
            return result;
        }

        private void ProcessDoc1(List<Doc1> docs1, NpgsqlCommand command2, MySqlConnection mySqlConnection,
            bool use_postalcode)
        {
            var ids1 = new List<long>();

            for (var ids = docs1.Where(x => x.parentid!=null).Select(x => x.parentid.Value).ToArray();
                 ids.Any();
                 ids = docs1.Where(x => x.parentid != null).Select(x => x.parentid.Value).ToArray())
            {
                ids = ids.Except(ids1).ToArray();

                if (!ids.Any()) break;

                var docs2 = GetDocs2(ids, command2, ids.Length, use_postalcode);

                if (!docs2.Any()) break;

                var q = from doc1 in docs1
                    join doc2 in docs2 on doc1.parentid equals doc2.objectid
                    select new { doc1, doc2 };

                foreach (var pair in q)
                {
                    pair.doc1.parentid = pair.doc2.parentid;
                    pair.doc1.addrfull = $"{pair.doc2.addrfull}, {pair.doc1.addrfull}";
                    if (string.IsNullOrEmpty(pair.doc1.postalcode)) pair.doc1.postalcode = pair.doc2.postalcode;
                }

                ids1.AddRange(ids);
            }

            foreach (var doc1 in docs1)
                if (!string.IsNullOrEmpty(doc1.postalcode))
                    doc1.addrfull = $"{doc1.postalcode}, {doc1.addrfull}";

            var sb = new StringBuilder(
                "REPLACE INTO garx(id,title,objectid,objectguid,priority,building) VALUES ");
            sb.Append(string.Join(",",
                docs1.Select(x =>
                    $"({x.id},'{x.addrfull.TextEscape()}',{x.objectid},'{x.objectguid}',{x.addrfull.Split(",").Length},{x.building})")));

            ExecuteNonQueryWithRepeatOnError(sb.ToString(), mySqlConnection);
        }

        private List<Doc2> GetDocs2(long[] ids, NpgsqlCommand npgsqlCommand2, int take, bool use_postalcode)
        {
            var docs2 = new List<Doc2>(take);

            npgsqlCommand2.Parameters["ids"].Value = ids;

            using var reader2 = npgsqlCommand2.ExecuteReader();
            while (reader2.Read())
            {
                var guid = reader2.SafeGetString(1);
                var name = reader2.SafeGetString(2);
                var aolevel = reader2.GetInt32(3);
                var parentid = reader2.SafeGetInt64(4);
                var postalcode = use_postalcode ? reader2.SafeGetString(5) : null;

                docs2.Add(new Doc2
                {
                    objectid = reader2.GetInt64(0),
                    addrfull = name,
                    parentid = parentid,
                    postalcode = postalcode
                });
            }

            return docs2;
        }
    }
}