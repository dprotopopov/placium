using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using MySql.Data.MySqlClient;
using Newtonsoft.Json;
using Npgsql;
using NpgsqlTypes;
using Placium.Common;

namespace Placium.Seeker
{
    public class AddressService : BaseService
    {
        private readonly List<string> _listAddrob = new List<string>();
        private readonly List<string> _listHouse = new List<string>();
        private readonly List<string> _listRoom = new List<string>();
        private readonly List<string> _listStead = new List<string>();
        private readonly string _parentAddrobSql;
        private readonly string _parentHouseSql;
        private readonly string _parentRoomSql;
        private readonly string _parentSteadSql;

        private readonly Regex _pointRegex = new Regex(@"POINT\s*\(\s*(?<lon>\d+(\.\d+))\s+(?<lat>\d+(\.\d+))\s*\)",
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
        }

        public async Task<IEnumerable<AddressEntry>> GetAddressInfo(string searchString, int limit = 20)
        {
            var guids = await GetFiasSuggestAsync(searchString, limit);
            var result = new List<AddressEntry>();

            foreach (var guid in guids)
            {
                var entry = await GetAddressEntryAsync(guid);

                var addr = entry.AddressString.Split(",").ToList();
                while (addr.Any())
                {
                    var points = await GetOsmByAddrAsync(addr.ToArray());

                    if (points.Any())
                    {
                        var match = _pointRegex.Match(points.First());
                        if (match.Success)
                        {
                            entry.GeoLon = match.Groups["lon"].Value;
                            entry.GeoLat = match.Groups["lat"].Value;
                            break;
                        }
                    }

                    addr.RemoveAt(addr.Count - 1);
                }

                result.Add(entry);
            }

            return result;
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
                                entry.Flat = new AddressLevelEntry
                                {
                                    FiasCode = guid,
                                    Name = string.Join(", ", list),
                                    Type = "кв",
                                    TypeFull = "квартира"
                                };
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
                                var list = new List<string> {name};
                                if (!string.IsNullOrEmpty(housenum)) list.Add($"{housenum}");
                                if (!string.IsNullOrEmpty(buildnum)) list.Add($"к{buildnum}");
                                if (!string.IsNullOrEmpty(strucnum)) list.Add($"с{strucnum}");
                                if (string.IsNullOrEmpty(entry.PostalCode)) entry.PostalCode = postalcode;
                                entry.House = new AddressLevelEntry
                                {
                                    FiasCode = guid,
                                    Name = string.Join(" ", list),
                                    Type = "д",
                                    TypeFull = name
                                };
                                result.Add(string.Join(", ", list));
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
                                entry.House = new AddressLevelEntry
                                {
                                    FiasCode = guid,
                                    Name = string.Join(" ", list),
                                    Type = "уч",
                                    TypeFull = "участок"
                                };
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
                                Console.WriteLine(JsonConvert.SerializeObject(levelEntry));
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

        private async Task<List<string>> GetFiasSuggestAsync(string search, int limit = 20)
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

        private async Task<List<string>> GetOsmByAddrAsync(string[] addr)
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

            var result = new List<string>();

            var match = string.Join("<<",
                addr.Where(x => !string.IsNullOrWhiteSpace(x)).Select(x =>
                    $"({string.Join(" NEAR/9 ", _spaceRegex.Split(x.Trim()).Select(y => y.Yo().ToLower().Escape()))})"));

            using (var npgsqlConnection = new NpgsqlConnection(GetOsmConnectionString()))
            using (var connection = new MySqlConnection(GetSphinxConnectionString()))
            {
                await npgsqlConnection.OpenAsync();

                npgsqlConnection.ReloadTypes();
                npgsqlConnection.TypeMapper.UseGeoJson();

                for (var priority = 0; priority < 20; priority++)
                {
                    var ids = new List<long>();
                    var dic = new Dictionary<string, object>
                    {
                        {"match", match},
                        {"priority", priority}
                    };
                    ids.FillAll(
                        "SELECT id FROM addrx WHERE MATCH(@match) AND priority=@priority",
                        dic, connection);

                    if (!ids.Any()) continue;

                    using (var command =
                        new NpgsqlCommand(
                            @"SELECT id,ST_AsText(ST_Centroid(location)) FROM placex WHERE tags?|@keys AND id=ANY(@ids) LIMIT 1",
                            npgsqlConnection))
                    {
                        command.Parameters.AddWithValue("keys", keys.ToArray());
                        command.Parameters.AddWithValue("ids", ids.ToArray());

                        command.Prepare();

                        using (var reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                                result.Add(reader.GetString(1));
                        }
                    }

                    if (result.Any()) break;
                }

                await npgsqlConnection.CloseAsync();

                return result;
            }
        }
    }
}