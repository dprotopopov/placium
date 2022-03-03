using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Npgsql;
using NpgsqlTypes;
using Placium.Common;
using Placium.Models;

namespace Placium.Services
{
    public class FiasService : BaseApiService
    {
        private readonly string _childrenAddrobSql;
        private readonly string _childrenHouseSql;
        private readonly string _childrenRoomSql;
        private readonly string _childrenSteadSql;
        private readonly List<string> _listAddrob = new List<string>();
        private readonly List<string> _listHouse = new List<string>();
        private readonly List<string> _listRoom = new List<string>();
        private readonly List<string> _listStead = new List<string>();
        private readonly string _parentAddrobSql;
        private readonly string _parentHouseSql;
        private readonly string _parentRoomSql;
        private readonly string _parentSteadSql;
        private readonly string _rootAddrobSql;
        private readonly string _rootHouseSql;
        private readonly string _rootRoomSql;
        private readonly string _rootSteadSql;

        public FiasService(IConfiguration configuration) : base(configuration)
        {
            using var connection = new NpgsqlConnection(GetFiasConnectionString());
            connection.Open();

            using (var command = new NpgsqlCommand(
                       string.Join(";", new[] { @"addrob\d+", @"house\d+", @"room\d+", @"stead\d+" }.Select(x =>
                           $@"SELECT table_name FROM information_schema.tables
                        WHERE table_schema = 'public' and table_name similar to '{x}'")),
                       connection))
            {
                command.Prepare();

                using var reader = command.ExecuteReader();
                _listAddrob.Fill(reader);
                reader.NextResult();
                _listHouse.Fill(reader);
                reader.NextResult();
                _listRoom.Fill(reader);
                reader.NextResult();
                _listStead.Fill(reader);
            }

            _parentRoomSql = string.Join("\nUNION ALL\n",
                _listRoom.Select(x =>
                    $@"SELECT houseguid,flatnumber,roomnumber FROM {x}
                        WHERE roomguid=@p AND livestatus=1"));
            _parentHouseSql = string.Join("\nUNION ALL\n",
                _listHouse.Select(x =>
                    $@"SELECT aoguid,housenum,buildnum,strucnum,eststat.name FROM {x}
                        JOIN (SELECT now() as n) as q ON startdate<=n AND n<enddate 
                        JOIN eststat ON {x}.eststatus=eststat.eststatid
                        WHERE houseguid=@p"));
            _parentSteadSql = string.Join("\nUNION ALL\n",
                _listStead.Select(x =>
                    $"SELECT parentguid,number FROM {x} WHERE steadguid=@p AND livestatus=1"));
            _parentAddrobSql = string.Join("\nUNION ALL\n",
                _listAddrob.Select(x =>
                    $@"SELECT parentguid,offname,formalname,shortname,socrbase.socrname,aolevel FROM {x}
                        JOIN socrbase ON {x}.shortname=socrbase.scname AND {x}.aolevel=socrbase.level
                        WHERE aoguid=@p AND livestatus=1"));

            _childrenRoomSql = string.Join("\nUNION ALL\n",
                _listRoom.Select(x =>
                    $@"SELECT roomguid,flatnumber,roomnumber FROM {x}
                        WHERE houseguid=@p AND livestatus=1"));
            _childrenHouseSql = string.Join("\nUNION ALL\n",
                _listHouse.Select(x =>
                    $@"SELECT houseguid,housenum,buildnum,strucnum,eststat.name FROM {x}
                        JOIN (SELECT now() as n) as q ON startdate<=n AND n<enddate 
                        JOIN eststat ON {x}.eststatus=eststat.eststatid
                        WHERE aoguid=@p"));
            _childrenSteadSql = string.Join("\nUNION ALL\n",
                _listStead.Select(x =>
                    $"SELECT steadguid,number FROM {x} WHERE parentguid=@p AND livestatus=1"));
            _childrenAddrobSql = string.Join("\nUNION ALL\n",
                _listAddrob.Select(x =>
                    $@"SELECT aoguid,offname,formalname,shortname,socrbase.socrname,aolevel FROM {x}
                        JOIN socrbase ON {x}.shortname=socrbase.scname AND {x}.aolevel=socrbase.level
                        WHERE parentguid=@p AND livestatus=1"));

            _rootRoomSql = string.Join("\nUNION ALL\n",
                _listRoom.Select(x =>
                    $@"SELECT roomguid,flatnumber,roomnumber FROM {x}
                        WHERE houseguid IS NULL AND livestatus=1"));
            _rootHouseSql = string.Join("\nUNION ALL\n",
                _listHouse.Select(x =>
                    $@"SELECT houseguid,housenum,buildnum,strucnum,eststat.name FROM {x}
                        JOIN (SELECT now() as n) as q ON startdate<=n AND n<enddate 
                        JOIN eststat ON {x}.eststatus=eststat.eststatid
                        WHERE aoguid IS NULL"));
            _rootSteadSql = string.Join("\nUNION ALL\n",
                _listStead.Select(x =>
                    $@"SELECT steadguid,number FROM {x}
                        WHERE parentguid IS NULL AND livestatus=1"));
            _rootAddrobSql = string.Join("\nUNION ALL\n",
                _listAddrob.Select(x =>
                    $@"SELECT aoguid,offname,formalname,shortname,socrbase.socrname,aolevel FROM {x}
                        JOIN socrbase ON {x}.shortname=socrbase.scname AND {x}.aolevel=socrbase.level
                        WHERE parentguid IS NULL AND livestatus=1"));
        }

        public async Task<List<object>> GetDetailsAsync(string guid, bool formal = false, bool socr = true)
        {
            await using var connection = new NpgsqlConnection(GetFiasConnectionString());
            await connection.OpenAsync();

            connection.ReloadTypes();

            var result = new List<object>();

            if (!string.IsNullOrEmpty(guid))
            {
                await using var command = new NpgsqlCommand(_parentRoomSql, connection);
                command.Parameters.AddWithValue("p", guid);

                await command.PrepareAsync();

                await using var reader = command.ExecuteReader();
                if (reader.Read())
                {
                    var flatnumber = reader.SafeGetString(1);
                    var roomnumber = reader.SafeGetString(2);
                    var list = new List<string>();
                    if (!string.IsNullOrEmpty(flatnumber)) list.Add($"Квартира {flatnumber}");
                    if (!string.IsNullOrEmpty(roomnumber)) list.Add($"Комната {roomnumber}");
                    result.Add(new Room
                    {
                        guid = Guid.Parse(guid),
                        flatnumber = flatnumber,
                        roomnumber = roomnumber,
                        title = string.Join(", ", list)
                    });
                    guid = reader.SafeGetString(0);
                }
            }


            if (!string.IsNullOrEmpty(guid))
            {
                await using var command = new NpgsqlCommand(_parentHouseSql, connection);
                command.Parameters.AddWithValue("p", guid);

                await command.PrepareAsync();

                await using var reader = command.ExecuteReader();
                if (reader.Read())
                {
                    var housenum = reader.SafeGetString(1);
                    var buildnum = reader.SafeGetString(2);
                    var strucnum = reader.SafeGetString(3);
                    var name = reader.SafeGetString(4);
                    var list = new List<string> { name };
                    if (!string.IsNullOrEmpty(housenum)) list.Add($"{housenum}");
                    if (!string.IsNullOrEmpty(buildnum)) list.Add($"к{buildnum}");
                    if (!string.IsNullOrEmpty(strucnum)) list.Add($"с{strucnum}");
                    result.Add(new House
                    {
                        guid = Guid.Parse(guid),
                        housenum = housenum,
                        buildnum = buildnum,
                        strucnum = strucnum,
                        name = name,
                        title = string.Join(" ", list)
                    });
                    guid = reader.SafeGetString(0);
                }
            }

            if (!string.IsNullOrEmpty(guid))
            {
                await using var command = new NpgsqlCommand(_parentSteadSql, connection);
                command.Parameters.AddWithValue("p", guid);

                await command.PrepareAsync();

                using var reader = command.ExecuteReader();
                if (reader.Read())
                {
                    var number = reader.SafeGetString(1);
                    var list = new List<string>();
                    if (!string.IsNullOrEmpty(number)) list.Add($"уч. {number}");
                    result.Add(new Stead
                    {
                        guid = Guid.Parse(guid),
                        number = number,
                        title = string.Join(", ", list)
                    });
                    guid = reader.SafeGetString(0);
                }
            }

            await using (var command = new NpgsqlCommand(_parentAddrobSql, connection))
            {
                command.Parameters.Add("p", NpgsqlDbType.Varchar);

                await command.PrepareAsync();

                for (var run = !string.IsNullOrEmpty(guid); run;)
                {
                    run = false;

                    command.Parameters["p"].Value = guid;

                    await using var reader = command.ExecuteReader();
                    if (reader.Read())
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
                        result.Add(new Address
                        {
                            guid = Guid.Parse(guid),
                            offname = offname,
                            formalname = formalname,
                            shortname = shortname,
                            socrname = socrname,
                            aolevel = aolevel,
                            title = title
                        });
                        guid = reader.SafeGetString(0);
                        run = !string.IsNullOrEmpty(guid);
                    }
                }
            }

            result.Reverse();

            await connection.CloseAsync();

            return result;
        }

        public async Task<List<object>> GetChildrenAsync(string guid, bool formal = false, bool socr = true)
        {
            await using var connection = new NpgsqlConnection(GetFiasConnectionString());
            await connection.OpenAsync();

            connection.ReloadTypes();

            var result = new List<object>();

            await using (var command = new NpgsqlCommand(string.Join(";",
                             _childrenRoomSql, _childrenHouseSql, _childrenSteadSql, _childrenAddrobSql), connection))
            {
                command.Parameters.AddWithValue("p", guid);

                await command.PrepareAsync();

                await using var reader = command.ExecuteReader();
                while (reader.Read())
                {
                    var flatnumber = reader.SafeGetString(1);
                    var roomnumber = reader.SafeGetString(2);
                    var list = new List<string>();
                    if (!string.IsNullOrEmpty(flatnumber)) list.Add($"Квартира {flatnumber}");
                    if (!string.IsNullOrEmpty(roomnumber)) list.Add($"Комната {roomnumber}");
                    result.Add(new Room
                    {
                        guid = Guid.Parse(reader.SafeGetString(0)),
                        flatnumber = flatnumber,
                        roomnumber = roomnumber,
                        title = string.Join(", ", list)
                    });
                }

                reader.NextResult();

                while (reader.Read())
                {
                    var housenum = reader.SafeGetString(1);
                    var buildnum = reader.SafeGetString(2);
                    var strucnum = reader.SafeGetString(3);
                    var name = reader.SafeGetString(4);
                    var list = new List<string> { name };
                    if (!string.IsNullOrEmpty(housenum)) list.Add($"{housenum}");
                    if (!string.IsNullOrEmpty(buildnum)) list.Add($"к{buildnum}");
                    if (!string.IsNullOrEmpty(strucnum)) list.Add($"с{strucnum}");
                    result.Add(new House
                    {
                        guid = Guid.Parse(reader.SafeGetString(0)),
                        housenum = housenum,
                        buildnum = buildnum,
                        strucnum = strucnum,
                        name = name,
                        title = string.Join(" ", list)
                    });
                }

                reader.NextResult();

                while (reader.Read())
                {
                    var number = reader.SafeGetString(1);
                    var list = new List<string>();
                    if (!string.IsNullOrEmpty(number)) list.Add($"уч. {number}");
                    result.Add(new Stead
                    {
                        guid = Guid.Parse(reader.SafeGetString(0)),
                        number = number,
                        title = string.Join(", ", list)
                    });
                }

                reader.NextResult();

                while (reader.Read())
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
                    result.Add(new Address
                    {
                        guid = Guid.Parse(reader.SafeGetString(0)),
                        offname = offname,
                        formalname = formalname,
                        shortname = shortname,
                        socrname = socrname,
                        aolevel = aolevel,
                        title = title
                    });
                }
            }

            await connection.CloseAsync();

            return result;
        }

        public async Task<List<object>> GetRootsAsync(bool formal = false, bool socr = true)
        {
            await using var connection = new NpgsqlConnection(GetFiasConnectionString());
            await connection.OpenAsync();

            connection.ReloadTypes();

            var result = new List<object>();

            await using (var command = new NpgsqlCommand(string.Join(";",
                             _rootRoomSql, _rootHouseSql, _rootSteadSql, _rootAddrobSql), connection))
            {
                await command.PrepareAsync();

                await using var reader = command.ExecuteReader();
                while (reader.Read())
                {
                    var flatnumber = reader.SafeGetString(1);
                    var roomnumber = reader.SafeGetString(2);
                    var list = new List<string>();
                    if (!string.IsNullOrEmpty(flatnumber)) list.Add($"Квартира {flatnumber}");
                    if (!string.IsNullOrEmpty(roomnumber)) list.Add($"Комната {roomnumber}");
                    result.Add(new Room
                    {
                        guid = Guid.Parse(reader.SafeGetString(0)),
                        flatnumber = flatnumber,
                        roomnumber = roomnumber,
                        title = string.Join(", ", list)
                    });
                }

                reader.NextResult();

                while (reader.Read())
                {
                    var housenum = reader.SafeGetString(1);
                    var buildnum = reader.SafeGetString(2);
                    var strucnum = reader.SafeGetString(3);
                    var name = reader.SafeGetString(4);
                    var list = new List<string> { name };
                    if (!string.IsNullOrEmpty(housenum)) list.Add($"{housenum}");
                    if (!string.IsNullOrEmpty(buildnum)) list.Add($"к{buildnum}");
                    if (!string.IsNullOrEmpty(strucnum)) list.Add($"с{strucnum}");
                    result.Add(new House
                    {
                        guid = Guid.Parse(reader.SafeGetString(0)),
                        housenum = housenum,
                        buildnum = buildnum,
                        strucnum = strucnum,
                        name = name,
                        title = string.Join(" ", list)
                    });
                }

                reader.NextResult();

                while (reader.Read())
                {
                    var number = reader.SafeGetString(1);
                    var list = new List<string>();
                    if (!string.IsNullOrEmpty(number)) list.Add($"уч. {number}");
                    result.Add(new Stead
                    {
                        guid = Guid.Parse(reader.SafeGetString(0)),
                        number = number,
                        title = string.Join(", ", list)
                    });
                }

                reader.NextResult();

                while (reader.Read())
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
                    result.Add(new Address
                    {
                        guid = Guid.Parse(reader.SafeGetString(0)),
                        offname = offname,
                        formalname = formalname,
                        shortname = shortname,
                        socrname = socrname,
                        aolevel = aolevel,
                        title = title
                    });
                }
            }

            await connection.CloseAsync();

            return result;
        }
    }
}