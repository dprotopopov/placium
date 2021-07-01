using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using MySql.Data.MySqlClient;
using Npgsql;
using NpgsqlTypes;
using Placium.Common;

namespace Placium.Seeker
{
    public class AddressService : BaseService
    {
        private readonly string _guidSql;
        private readonly List<string> _listAddrob = new List<string>();
        private readonly List<string> _listHouse = new List<string>();
        private readonly List<string> _listRoom = new List<string>();
        private readonly List<string> _listStead = new List<string>();
        private readonly string _parentAddrobSql;
        private readonly string _parentHouseSql;
        private readonly string _parentRoomSql;
        private readonly string _parentSteadSql;

        private readonly Regex _pointRegex = new Regex(@"POINT\s*\(\s*(?<lon>\d+(\.\d+)?)\s+(?<lat>\d+(\.\d+)?)\s*\)",
            RegexOptions.IgnoreCase);

        private readonly Regex _spaceRegex = new Regex(@"\s+", RegexOptions.IgnoreCase);

        public AddressService(IConfiguration configuration) : base(configuration)
        {
            using (var connection = new NpgsqlConnection(GetFiasConnectionString()))
            {
                connection.Open();
                using (var command = new NpgsqlCommand(
                    string.Join(";", new[] {@"addrob\d+", @"house\d+", @"room\d+", @"stead\d+"}.Select(x =>
                        $@"SELECT table_name FROM information_schema.tables
                        WHERE table_schema = 'public' and table_name similar to '{x}'")),
                    connection))
                {
                    command.Prepare();

                    using (var reader = command.ExecuteReader())
                    {
                        _listAddrob.Fill(reader);
                        reader.NextResult();
                        _listHouse.Fill(reader);
                        reader.NextResult();
                        _listRoom.Fill(reader);
                        reader.NextResult();
                        _listStead.Fill(reader);
                    }
                }
            }

            _parentRoomSql = string.Join("\nUNION ALL\n",
                _listRoom.Select(x =>
                    $@"SELECT houseguid,flatnumber,roomnumber FROM {x}
                        WHERE roomguid=@p AND livestatus=1"));
            _parentHouseSql = string.Join("\nUNION ALL\n",
                _listHouse.Select(x =>
                    $@"SELECT aoguid,housenum,buildnum,strucnum,eststat.name,postalcode FROM {x}
                        JOIN (SELECT now() as n) as q ON startdate<=n AND n<enddate 
                        JOIN eststat ON {x}.eststatus=eststat.eststatid
                        WHERE houseguid=@p"));
            _parentSteadSql = string.Join("\nUNION ALL\n",
                _listStead.Select(x =>
                    $"SELECT parentguid,number,postalcode FROM {x} WHERE steadguid=@p AND livestatus=1"));
            _parentAddrobSql = string.Join("\nUNION ALL\n",
                _listAddrob.Select(x =>
                    $@"SELECT parentguid,offname,formalname,shortname,socrbase.socrname,aolevel,regioncode,postalcode FROM {x}
                        JOIN socrbase ON {x}.shortname=socrbase.scname AND {x}.aolevel=socrbase.level
                        WHERE aoguid=@p AND livestatus=1"));

            var queries = new List<string>();
            queries.AddRange(_listRoom.Select(x =>
                $@"SELECT roomguid FROM {x} WHERE record_id=ANY(@ids) AND livestatus=1"));
            queries.AddRange(_listHouse.Select(x =>
                $@"SELECT houseguid FROM {x} JOIN (SELECT now() as n) as q ON startdate<=n AND n<enddate WHERE record_id=ANY(@ids)"));
            queries.AddRange(_listStead.Select(x =>
                $"SELECT steadguid FROM {x} WHERE record_id=ANY(@ids) AND livestatus=1"));
            queries.AddRange(_listAddrob.Select(x =>
                $@"SELECT aoguid FROM {x} WHERE record_id=ANY(@ids) AND livestatus=1"));
            _guidSql = string.Join("\nUNION ALL\n", queries);
        }

        public async Task<IEnumerable<AddressEntry>> GetAddressInfoAsync(string searchString, int limit = 20)
        {
            var ids = await GetFiasSuggestAsync(searchString, limit);
            var guids = await GetGuidsAsync(ids.ToArray());

            var result = new List<AddressEntry>();

            using (var mySqlConnection = new MySqlConnection(GetSphinxConnectionString()))
            {
                foreach (var guid in guids)
                {
                    var entry = await GetAddressEntryAsync(guid);

                    var addr = entry.AddressString.Split(",").ToList();
                    while (addr.Any())
                    {
                        if (GetPointByAddr(addr.ToArray(), mySqlConnection, out var lon,
                            out var lat))
                        {
                            entry.GeoLon = lon;
                            entry.GeoLat = lat;
                            break;
                        }

                        addr.RemoveAt(addr.Count - 1);
                    }

                    result.Add(entry);
                }

                return result;
            }
        }

        public async Task<IEnumerable<AddressEntry>> GetAddressInfo2Async(string searchString, int limit = 20)
        {
            var names = await GetFiasSuggestNamesAsync(searchString, limit);

            var result = new List<AddressEntry>();

            using (var mySqlConnection = new MySqlConnection(GetSphinxConnectionString()))
            {
                foreach (var name in names)
                {
                    var entry = new AddressEntry
                    {
                        AddressString = name
                    };

                    var addr = entry.AddressString.Split(",").ToList();
                    while (addr.Any())
                    {
                        if (GetPointByAddr(addr.ToArray(), mySqlConnection, out var lon,
                            out var lat))
                        {
                            entry.GeoLon = lon;
                            entry.GeoLat = lat;
                            break;
                        }

                        addr.RemoveAt(addr.Count - 1);
                    }

                    result.Add(entry);
                }
            }

            return result;
        }

        public async Task<IEnumerable<AddressEntry>> GetAddressInfo3Async(string searchString, int limit = 20)
        {
            var result = new List<AddressEntry>();
            var list = searchString.Split(",");
            var match = string.Join("<<",
                list.Where(x => !string.IsNullOrWhiteSpace(x)).Select(x =>
                    $"({string.Join(" NEAR/9 ", _spaceRegex.Split(x.Trim()).Select(y => y.Yo().ToLower().Escape()))})"));

            using (var mySqlConnection = new MySqlConnection(GetSphinxConnectionString()))
            {
                for (var priority = 0; priority < 20 && limit > 0; priority++)
                {
                    var skip = 0;
                    var take = 20;
                    while (limit > 0)
                    {
                        mySqlConnection.TryOpen();

                        using (var command =
                            new MySqlCommand(
                                @"SELECT addressString,postalCode,regionCode,country,geoLon,geoLat FROM address WHERE MATCH(@match) AND priority=@priority LIMIT @skip,@take",
                                mySqlConnection))
                        {
                            command.Parameters.AddWithValue("skip", skip);
                            command.Parameters.AddWithValue("take", take);
                            command.Parameters.AddWithValue("priority", priority);
                            command.Parameters.AddWithValue("match", match);

                            using (var reader = command.ExecuteReader())
                            {
                                var count = 0;
                                while (limit > 0 && reader.Read())
                                {
                                    count++;
                                    limit--;
                                    var addressString = reader.GetString(0);
                                    var postalCode = reader.GetString(1);
                                    var regionCode = reader.GetString(2);
                                    var country = reader.GetString(3);
                                    var geoLon = reader.GetString(4);
                                    var geoLat = reader.GetString(5);
                                    result.Add(new AddressEntry
                                    {
                                        AddressString = addressString,
                                        PostalCode = postalCode,
                                        RegionCode = regionCode,
                                        Country = country,
                                        GeoLon = geoLon,
                                        GeoLat = geoLat
                                    });
                                }

                                if (count < take) break;
                            }
                        }
                    }
                }
            }

            return result;
        }

        private async Task<List<string>> GetGuidsAsync(long[] ids)
        {
            var result = new List<string>();
            using (var connection = new NpgsqlConnection(GetFiasConnectionString()))
            {
                await connection.OpenAsync();

                connection.ReloadTypes();
                using (var command = new NpgsqlCommand(_guidSql, connection))
                {
                    command.Parameters.AddWithValue("ids", ids);

                    command.Prepare();
                    using (var reader = command.ExecuteReader())
                    {
                        while (reader.Read()) result.Add(reader.SafeGetString(0));
                    }
                }

                await connection.CloseAsync();

                return result;
            }
        }

        private async Task<AddressEntry> GetAddressEntryAsync(string guid)
        {
            var socr = false;
            var formal = false;

            using (var connection = new NpgsqlConnection(GetFiasConnectionString()))
            {
                await connection.OpenAsync();

                connection.ReloadTypes();

                var result = new List<string>();

                var entry = new AddressEntry();

                if (!string.IsNullOrEmpty(guid))
                    using (var command = new NpgsqlCommand(_parentRoomSql, connection))
                    {
                        command.Parameters.AddWithValue("p", guid);

                        command.Prepare();

                        using (var reader = command.ExecuteReader())
                        {
                            if (reader.Read())
                            {
                                var flatnumber = reader.SafeGetString(1);
                                var roomnumber = reader.SafeGetString(2);
                                var list = new List<string>();
                                if (!string.IsNullOrEmpty(flatnumber)) list.Add($"Квартира {flatnumber}");
                                if (!string.IsNullOrEmpty(roomnumber)) list.Add($"Комната {roomnumber}");
                                var levelEntry = new AddressLevelEntry
                                {
                                    FiasCode = guid,
                                    Name = string.Join(", ", list),
                                    Type = "кв",
                                    TypeFull = "квартира"
                                };
                                entry.Flat = levelEntry;
                                result.Add(string.Join(", ", list));
                                guid = reader.SafeGetString(0);
                            }
                        }
                    }


                if (!string.IsNullOrEmpty(guid))
                    using (var command = new NpgsqlCommand(_parentHouseSql, connection))
                    {
                        command.Parameters.AddWithValue("p", guid);

                        command.Prepare();

                        using (var reader = command.ExecuteReader())
                        {
                            if (reader.Read())
                            {
                                var housenum = reader.SafeGetString(1);
                                var buildnum = reader.SafeGetString(2);
                                var strucnum = reader.SafeGetString(3);
                                var name = reader.SafeGetString(4);
                                var postalcode = reader.SafeGetString(5);
                                var list = new List<string>();
                                if (!string.IsNullOrEmpty(housenum)) list.Add($"{housenum}");
                                if (!string.IsNullOrEmpty(buildnum)) list.Add($"к{buildnum}");
                                if (!string.IsNullOrEmpty(strucnum)) list.Add($"с{strucnum}");
                                if (string.IsNullOrEmpty(entry.PostalCode)) entry.PostalCode = postalcode;
                                var levelEntry = new AddressLevelEntry
                                {
                                    FiasCode = guid,
                                    Name = string.Join(" ", list),
                                    Type = "д",
                                    TypeFull = name
                                };
                                entry.House = levelEntry;
                                result.Add(string.Join(" ", list));
                                guid = reader.SafeGetString(0);
                            }
                        }
                    }

                if (!string.IsNullOrEmpty(guid))
                    using (var command = new NpgsqlCommand(_parentSteadSql, connection))
                    {
                        command.Parameters.AddWithValue("p", guid);

                        command.Prepare();

                        using (var reader = command.ExecuteReader())
                        {
                            if (reader.Read())
                            {
                                var number = reader.SafeGetString(1);
                                var postalcode = reader.SafeGetString(2);
                                var list = new List<string>();
                                if (!string.IsNullOrEmpty(number)) list.Add($"уч. {number}");
                                if (string.IsNullOrEmpty(entry.PostalCode)) entry.PostalCode = postalcode;
                                var levelEntry = new AddressLevelEntry
                                {
                                    FiasCode = guid,
                                    Name = string.Join(" ", list),
                                    Type = "уч",
                                    TypeFull = "участок"
                                };
                                entry.House = levelEntry;
                                result.Add(string.Join(", ", list));
                                guid = reader.SafeGetString(0);
                            }
                        }
                    }

                using (var command = new NpgsqlCommand(_parentAddrobSql, connection))
                {
                    command.Parameters.Add("p", NpgsqlDbType.Varchar);

                    command.Prepare();

                    for (var run = !string.IsNullOrEmpty(guid); run;)
                    {
                        run = false;

                        command.Parameters["p"].Value = guid;

                        using (var reader = command.ExecuteReader())
                        {
                            if (reader.Read())
                            {
                                var offname = reader.SafeGetString(1);
                                var formalname = reader.SafeGetString(2);
                                var shortname = reader.SafeGetString(3);
                                var socrname = reader.SafeGetString(4);
                                var aolevel = reader.GetInt32(5);
                                var regioncode = reader.SafeGetString(6);
                                var postalcode = reader.SafeGetString(7);
                                var title = aolevel > 1
                                    ? $"{(socr ? socrname : shortname)} {(formal ? formalname : offname)}"
                                    : formal
                                        ? formalname
                                        : offname;
                                if (string.IsNullOrEmpty(entry.PostalCode)) entry.PostalCode = postalcode;
                                var levelEntry = new AddressLevelEntry
                                {
                                    FiasCode = guid,
                                    Name = title,
                                    Type = shortname,
                                    TypeFull = socrname
                                };
                                switch (aolevel)
                                {
                                    case 1:
                                        entry.Region = levelEntry;
                                        entry.RegionCode = regioncode;
                                        break;
                                    case 2:
                                        break;
                                    case 3:
                                        entry.Area = levelEntry;
                                        break;
                                    case 4:
                                        entry.City = levelEntry;
                                        break;
                                    case 5:
                                        entry.CityDistrict = levelEntry;
                                        break;
                                    case 6:
                                        entry.Settlement = levelEntry;
                                        break;
                                    case 7:
                                        entry.Street = levelEntry;
                                        break;
                                }

                                result.Add(title);
                                guid = reader.SafeGetString(0);
                                run = !string.IsNullOrEmpty(guid);
                            }
                        }
                    }
                }

                result.Reverse();
                entry.AddressString = string.Join(", ", result);

                await connection.CloseAsync();

                return entry;
            }
        }

        private async Task<List<long>> GetFiasSuggestAsync(string search, int limit = 20)
        {
            var result = new List<long>();

            var list = search.Split(",");
            var match = string.Join("<<",
                list.Where(x => !string.IsNullOrWhiteSpace(x)).Select(x =>
                    $"({string.Join(" NEAR/9 ", _spaceRegex.Split(x.Trim()).Select(y => y.Yo().ToLower().Escape()))})"));

            using (var connection = new MySqlConnection(GetSphinxConnectionString()))
            {
                for (var priority = 0; limit > 0 && priority < 20; priority++)
                {
                    var dic = new Dictionary<string, object>
                    {
                        {"match", match},
                        {"priority", priority}
                    };
                    var count = result.FillAll(
                        "SELECT id FROM addrobx WHERE MATCH(@match) AND priority=@priority",
                        dic, connection, limit: limit);
                    limit -= count;
                }

                return result;
            }
        }

        private async Task<List<string>> GetFiasSuggestNamesAsync(string search, int limit = 20)
        {
            var list = search.Split(",");
            var result = new List<string>();

            var match = string.Join("<<",
                list.Where(x => !string.IsNullOrWhiteSpace(x)).Select(x =>
                    $"({string.Join(" NEAR/9 ", _spaceRegex.Split(x.Trim()).Select(y => y.Yo().ToLower().Escape()))})"));

            using (var connection = new MySqlConnection(GetSphinxConnectionString()))
            {
                for (var priority = 0; limit > 0 && priority < 20; priority++)
                {
                    var dic = new Dictionary<string, object>
                    {
                        {"match", match},
                        {"priority", priority}
                    };
                    var count = result.FillAll(
                        "SELECT title FROM addrobx WHERE MATCH(@match) AND priority=@priority",
                        dic, connection, limit: limit);
                    limit -= count;
                }

                return result;
            }
        }


        private bool GetPointByAddr(string[] addr, MySqlConnection connection, out string lon, out string lat)
        {
            var keys = new[]
            {
                "addr:region",
                "addr:district",
                "addr:city",
                "addr:town",
                "addr:village",
                "addr:subdistrict",
                "addr:suburb",
                "addr:hamlet",
                "addr:place",
                "addr:street",
                "addr:housenumber"
            };


            var match = string.Join("<<",
                addr.Where(x => !string.IsNullOrWhiteSpace(x)).Select(x =>
                    $"({string.Join(" NEAR/9 ", _spaceRegex.Split(x.Trim()).Select(y => y.Yo().ToLower().Escape()))})"));

            for (var priority = 0; priority < 20; priority++)
            {
                var dic = new Dictionary<string, object>
                {
                    {"match", match},
                    {"priority", priority}
                };

                connection.TryOpen();

                using (var command =
                    new MySqlCommand("SELECT lon,lat FROM addrx WHERE MATCH(@match) AND priority=@priority LIMIT 1",
                        connection))
                {
                    foreach (var pair in dic) command.Parameters.AddWithValue(pair.Key, pair.Value);

                    using (var reader = command.ExecuteReader())
                    {
                        if (reader.Read())
                        {
                            lon = reader.GetString(0);
                            lat = reader.GetString(1);
                            return true;
                        }
                    }
                }
            }

            lon = lat = string.Empty;

            return false;
        }
    }
}