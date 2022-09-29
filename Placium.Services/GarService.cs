using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Npgsql;
using Placium.Common;
using Placium.Models;

namespace Placium.Services
{
    public class GarService : BaseApiService
    {
        private readonly string _childrenAddrobSql;
        private readonly string _childrenHouseSql;
        private readonly string _childrenRoomSql;
        private readonly string _childrenSteadSql;
        private readonly string _childrenCarplaceSql;
        private readonly string _parentAddrobSql;
        private readonly string _parentHouseSql;
        private readonly string _parentRoomSql;
        private readonly string _parentSteadSql;
        private readonly string _parentCarplaceSql;
        private readonly string _rootAddrobSql;
        private readonly string _rootHouseSql;
        private readonly string _rootRoomSql;
        private readonly string _rootSteadSql;
        private readonly string _rootCarplaceSql;
        public GarService(IConfiguration configuration) : base(configuration)
        {
            _parentRoomSql = $@"SELECT h.""PARENTOBJID"",t.""NUMBER"",t.""OBJECTGUID"" FROM ""AS_ROOMS"" t JOIN ""AS_ADM_HIERARCHY"" h ON t.""OBJECTID""=h.""OBJECTID""
                        WHERE h.""OBJECTID""=@p AND t.""STARTDATE""<=@n AND @n<t.""ENDDATE""";
            _parentHouseSql = $@"SELECT h.""PARENTOBJID"",t.""HOUSENUM"",t.""OBJECTGUID"" FROM ""AS_HOUSES"" t JOIN ""AS_ADM_HIERARCHY"" h ON t.""OBJECTID""=h.""OBJECTID""
                        WHERE h.""OBJECTID""=@p AND t.""STARTDATE""<=@n AND @n<t.""ENDDATE""";
            _parentSteadSql = $@"SELECT h.""PARENTOBJID"",t.""NUMBER"",t.""OBJECTGUID"" FROM ""AS_STEADS"" t JOIN ""AS_ADM_HIERARCHY"" h ON t.""OBJECTID""=h.""OBJECTID""::varchar(255)
                        WHERE h.""OBJECTID""=@p AND t.""STARTDATE""<=@n AND @n<t.""ENDDATE""";
            _parentCarplaceSql = $@"SELECT h.""PARENTOBJID"",t.""NUMBER"",t.""OBJECTGUID"" FROM ""AS_CARPLACES"" t JOIN ""AS_ADM_HIERARCHY"" h ON t.""OBJECTID""=h.""OBJECTID""
                        WHERE h.""OBJECTID""=@p AND t.""STARTDATE""<=@n AND @n<t.""ENDDATE""";
            _parentAddrobSql = $@"SELECT h.""PARENTOBJID"",t.""NAME"",t.""OBJECTGUID"" FROM ""AS_ADDR_OBJ"" t JOIN ""AS_ADM_HIERARCHY"" h ON t.""OBJECTID""=h.""OBJECTID""
                        WHERE h.""OBJECTID""=@p AND t.""STARTDATE""<=@n AND @n<t.""ENDDATE""";

            _childrenRoomSql = $@"SELECT t.""OBJECTID"",t.""NUMBER"",t.""OBJECTGUID"" FROM ""AS_ROOMS"" t JOIN ""AS_ADM_HIERARCHY"" h ON t.""OBJECTID""=h.""OBJECTID""
                        WHERE h.""PARENTOBJID""=@p AND t.""STARTDATE""<=@n AND @n<t.""ENDDATE""";
            _childrenHouseSql = @"SELECT t.""OBJECTID"",t.""HOUSENUM"",t.""OBJECTGUID"" FROM ""AS_HOUSES"" t JOIN ""AS_ADM_HIERARCHY"" h ON t.""OBJECTID""=h.""OBJECTID""
                        WHERE h.""PARENTOBJID""=@p AND t.""STARTDATE""<=@n AND @n<t.""ENDDATE""";
            _childrenSteadSql = @"SELECT t.""OBJECTID""::bigint,t.""NUMBER"",t.""OBJECTGUID"" FROM ""AS_STEADS"" t JOIN ""AS_ADM_HIERARCHY"" h ON t.""OBJECTID""=h.""OBJECTID""::varchar(255)
                        WHERE h.""PARENTOBJID""=@p AND t.""STARTDATE""<=@n AND @n<t.""ENDDATE""";
            _childrenCarplaceSql = @"SELECT t.""OBJECTID"",t.""NUMBER"",t.""OBJECTGUID"" FROM ""AS_CARPLACES"" t JOIN ""AS_ADM_HIERARCHY"" h ON t.""OBJECTID""=h.""OBJECTID""
                        WHERE h.""PARENTOBJID""=@p AND t.""STARTDATE""<=@n AND @n<t.""ENDDATE""";
            _childrenAddrobSql = @"SELECT t.""OBJECTID"",t.""NAME"",t.""OBJECTGUID"" FROM ""AS_ADDR_OBJ"" t JOIN ""AS_ADM_HIERARCHY"" h ON t.""OBJECTID""=h.""OBJECTID""
                        WHERE h.""PARENTOBJID""=@p AND t.""STARTDATE""<=@n AND @n<t.""ENDDATE""";

            _rootRoomSql = $@"SELECT t.""OBJECTID"",t.""NUMBER"",t.""OBJECTGUID"" FROM ""AS_ROOMS"" t LEFT JOIN ""AS_ADM_HIERARCHY"" h ON t.""OBJECTID""=h.""PARENTOBJID""
                        WHERE h.""OBJECTID"" IS NULL AND t.""STARTDATE""<=@n AND @n<t.""ENDDATE""";
            _rootHouseSql = $@"SELECT t.""OBJECTID"",t.""HOUSENUM"",t.""OBJECTGUID"" FROM ""AS_HOUSES"" t LEFT JOIN ""AS_ADM_HIERARCHY"" h ON t.""OBJECTID""=h.""PARENTOBJID""
                        WHERE h.""OBJECTID"" IS NULL AND t.""STARTDATE""<=@n AND @n<t.""ENDDATE""";
            _rootSteadSql = $@"SELECT t.""OBJECTID""::bigint,t.""NUMBER"",t.""OBJECTGUID"" FROM ""AS_STEADS"" t LEFT JOIN ""AS_ADM_HIERARCHY"" h ON t.""OBJECTID""=h.""PARENTOBJID""::varchar(255)
                        WHERE h.""OBJECTID"" IS NULL AND t.""STARTDATE""<=@n AND @n<t.""ENDDATE""";
            _rootCarplaceSql = $@"SELECT t.""OBJECTID"",t.""NUMBER"",t.""OBJECTGUID"" FROM ""AS_CARPLACES"" t LEFT JOIN ""AS_ADM_HIERARCHY"" h ON t.""OBJECTID""=h.""PARENTOBJID""
                        WHERE h.""OBJECTID"" IS NULL AND t.""STARTDATE""<=@n AND @n<t.""ENDDATE""";
            _rootAddrobSql = $@"SELECT t.""OBJECTID"",t.""NAME"",t.""OBJECTGUID"" FROM ""AS_ADDR_OBJ"" t LEFT JOIN ""AS_ADM_HIERARCHY"" h ON t.""OBJECTID""=h.""PARENTOBJID""
                        WHERE h.""OBJECTID"" IS NULL AND t.""STARTDATE""<=@n AND @n<t.""ENDDATE""";
        }

        public async Task<List<Element>> GetDetailsAsync(long? objectid, DateTime? dateTime = null)
        {
            if (dateTime == null) dateTime = DateTime.Now;

            await using var connection = new NpgsqlConnection(GetGarConnectionString());
            await connection.OpenAsync();

            connection.ReloadTypes();

            var result = new List<Element>();


            while (objectid != null)
            {
                await using var command = new NpgsqlCommand(_parentCarplaceSql, connection);
                command.Parameters.AddWithValue("p", objectid);
                command.Parameters.AddWithValue("n", dateTime);

                await command.PrepareAsync();

                using var reader = command.ExecuteReader();
                if (reader.Read())
                {
                    var number = reader.SafeGetString(1);
                    var guid = reader.SafeGetString(2);
                    result.Add(new Element { objectid = objectid, guid = Guid.Parse(guid), title = number });
                    objectid = reader.SafeGetInt64(0);
                }
                else break;
            }

            while (objectid != null)
            {
                await using var command = new NpgsqlCommand(_parentRoomSql, connection);
                command.Parameters.AddWithValue("p", objectid);
                command.Parameters.AddWithValue("n", dateTime);

                await command.PrepareAsync();

                await using var reader = command.ExecuteReader();
                if (reader.Read())
                {
                    var number = reader.SafeGetString(1);
                    var guid = reader.SafeGetString(2);
                    result.Add(new Element { objectid = objectid, guid = Guid.Parse(guid), title = number });
                    objectid = reader.SafeGetInt64(0);
                }
                else break;
            }


            while (objectid != null)
            {
                await using var command = new NpgsqlCommand(_parentHouseSql, connection);
                command.Parameters.AddWithValue("p", objectid);
                command.Parameters.AddWithValue("n", dateTime);

                await command.PrepareAsync();

                await using var reader = command.ExecuteReader();
                if (reader.Read())
                {
                    var number = reader.SafeGetString(1);
                    var guid = reader.SafeGetString(2);
                    result.Add(new Element { objectid = objectid, guid = Guid.Parse(guid), title = number });
                    objectid = reader.SafeGetInt64(0);
                }
                else break;
            }

            while (objectid != null)
            {
                await using var command = new NpgsqlCommand(_parentSteadSql, connection);
                command.Parameters.AddWithValue("p", objectid);
                command.Parameters.AddWithValue("n", dateTime);

                await command.PrepareAsync();

                using var reader = command.ExecuteReader();
                if (reader.Read())
                {
                    var number = reader.SafeGetString(1);
                    var guid = reader.SafeGetString(2);
                    result.Add(new Element { objectid = objectid, guid = Guid.Parse(guid), title = number });
                    objectid = reader.SafeGetInt64(0);
                }
                else break;
            }

            while (objectid != null)
            {
                await using var command = new NpgsqlCommand(_parentAddrobSql, connection);
                command.Parameters.AddWithValue("p", objectid);
                command.Parameters.AddWithValue("n", dateTime);

                await command.PrepareAsync();

                using var reader = command.ExecuteReader();
                if (reader.Read())
                {
                    var number = reader.SafeGetString(1);
                    var guid = reader.SafeGetString(2);
                    result.Add(new Element { objectid = objectid, guid = Guid.Parse(guid), title = number });
                    objectid = reader.SafeGetInt64(0);
                }
                else break;
            }

            result.Reverse();

            await connection.CloseAsync();

            return result;
        }
        public async Task<List<Element>> GetChildrenAsync(long? objectid, DateTime? dateTime = null)
        {
            if (dateTime == null) dateTime = DateTime.Now;

            await using var connection = new NpgsqlConnection(GetGarConnectionString());
            await connection.OpenAsync();

            connection.ReloadTypes();

            var result = new List<Element>();

            await using (var command = new NpgsqlCommand(string.Join(";",
                             _childrenCarplaceSql, _childrenRoomSql, _childrenHouseSql, _childrenSteadSql, _childrenAddrobSql), connection))
            {
                command.Parameters.AddWithValue("p", objectid);
                command.Parameters.AddWithValue("n", dateTime);

                await command.PrepareAsync();

                await using var reader = command.ExecuteReader();
                while (reader.Read())
                {
                    var number = reader.SafeGetString(1);
                    var guid = reader.SafeGetString(2);
                    objectid = reader.SafeGetInt64(0);
                    result.Add(new Element { objectid = objectid, guid = Guid.Parse(guid), title = number });
                }

                reader.NextResult();

                while (reader.Read())
                {
                    var number = reader.SafeGetString(1);
                    var guid = reader.SafeGetString(2);
                    objectid = reader.SafeGetInt64(0);
                    result.Add(new Element { objectid = objectid, guid = Guid.Parse(guid), title = number });
                }

                reader.NextResult();

                while (reader.Read())
                {
                    var number = reader.SafeGetString(1);
                    var guid = reader.SafeGetString(2);
                    objectid = reader.SafeGetInt64(0);
                    result.Add(new Element { objectid = objectid, guid = Guid.Parse(guid), title = number });
                }

                reader.NextResult();

                while (reader.Read())
                {
                    var number = reader.SafeGetString(1);
                    var guid = reader.SafeGetString(2);
                    objectid = reader.SafeGetInt64(0);
                    result.Add(new Element { objectid = objectid, guid = Guid.Parse(guid), title = number });
                }

                reader.NextResult();

                while (reader.Read())
                {
                    var number = reader.SafeGetString(1);
                    var guid = reader.SafeGetString(2);
                    objectid = reader.SafeGetInt64(0);
                    result.Add(new Element { objectid = objectid, guid = Guid.Parse(guid), title = number });
                }
            }

            await connection.CloseAsync();

            return result;
        }

        public async Task<List<Element>> GetRootsAsync(DateTime? dateTime = null)
        {
            if (dateTime == null) dateTime = DateTime.Now;

            await using var connection = new NpgsqlConnection(GetGarConnectionString());
            await connection.OpenAsync();

            connection.ReloadTypes();

            var result = new List<Element>();

            await using (var command = new NpgsqlCommand(string.Join(";",
                             _rootCarplaceSql, _rootRoomSql, _rootHouseSql, _rootSteadSql, _rootAddrobSql), connection))
            {
                command.Parameters.AddWithValue("n", dateTime);

                await command.PrepareAsync();

                await using var reader = command.ExecuteReader();
                while (reader.Read())
                {
                    var number = reader.SafeGetString(1);
                    var guid = reader.SafeGetString(2);
                    var objectid = reader.SafeGetInt64(0);
                    result.Add(new Element { objectid = objectid, guid = Guid.Parse(guid), title = number });
                }

                reader.NextResult();

                while (reader.Read())
                {
                    var number = reader.SafeGetString(1);
                    var guid = reader.SafeGetString(2);
                    var objectid = reader.SafeGetInt64(0);
                    result.Add(new Element { objectid = objectid, guid = Guid.Parse(guid), title = number });
                }

                reader.NextResult();

                while (reader.Read())
                {
                    var number = reader.SafeGetString(1);
                    var guid = reader.SafeGetString(2);
                    var objectid = reader.SafeGetInt64(0);
                    result.Add(new Element { objectid = objectid, guid = Guid.Parse(guid), title = number });
                }

                reader.NextResult();

                while (reader.Read())
                {
                    var number = reader.SafeGetString(1);
                    var guid = reader.SafeGetString(2);
                    var objectid = reader.SafeGetInt64(0);
                    result.Add(new Element { objectid = objectid, guid = Guid.Parse(guid), title = number });
                }

                reader.NextResult();

                while (reader.Read())
                {
                    var number = reader.SafeGetString(1);
                    var guid = reader.SafeGetString(2);
                    var objectid = reader.SafeGetInt64(0);
                    result.Add(new Element { objectid = objectid, guid = Guid.Parse(guid), title = number });
                }
            }

            await connection.CloseAsync();

            return result;
        }

    }
}