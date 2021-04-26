﻿using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Fastenshtein;
using GeoJSON.Net;
using Microsoft.Extensions.Configuration;
using MySql.Data.MySqlClient;
using Npgsql;
using NpgsqlTypes;
using Placium.Common;
using Placium.Models;

namespace Placium.Seeker
{
    public class DefaultSeeker : BaseService
    {
        private readonly string _addrobSql;
        private readonly List<string> _listAddrob = new List<string>();
        private readonly List<string> _listHouse = new List<string>();
        private readonly List<string> _listRoom = new List<string>();
        private readonly List<string> _listStead = new List<string>();
        private readonly Regex _spaceRegex = new Regex(@"\s+", RegexOptions.IgnoreCase);

        public DefaultSeeker(IConfiguration configuration) : base(configuration)
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

                _addrobSql = string.Join("\nUNION ALL\n", string.Join("\nUNION ALL\n",
                    _listRoom.Select(x =>
                        $@"SELECT roomguid FROM {x} WHERE record_id=ANY(@ids)")), string.Join("\nUNION ALL\n",
                    _listHouse.Select(x =>
                        $@"SELECT houseguid FROM {x} WHERE record_id=ANY(@ids)")), string.Join("\nUNION ALL\n",
                    _listStead.Select(x =>
                        $"SELECT steadguid FROM {x} WHERE record_id=ANY(@ids)")), string.Join("\nUNION ALL\n",
                    _listAddrob.Select(x =>
                        $@"SELECT aoguid FROM {x} WHERE record_id=ANY(@ids)")));
            }
        }

        /// <summary>
        ///     Получение списка кодов ФИАС для заданного словаря с элементами адреса
        /// </summary>
        /// <param name="dictionary">Словарь с элементами адреса</param>
        /// <returns>Список кодов ФИАС</returns>
        public async Task<List<string>> GetFiasByAddrAsync(Dictionary<string, string> dictionary)
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
                "addr:street"
            };

            var addr = new List<string>();

            var skipCity = dictionary.ContainsKey("addr:region") && dictionary.ContainsKey("addr:city") &&
                           dictionary["addr:region"] == dictionary["addr:city"];

            var skipTown = dictionary.ContainsKey("addr:city") && dictionary.ContainsKey("addr:town") &&
                           dictionary["addr:city"] == dictionary["addr:town"];

            var skipVillage = dictionary.ContainsKey("addr:city") && dictionary.ContainsKey("addr:village") &&
                           dictionary["addr:city"] == dictionary["addr:village"];

            foreach (var key in keys)
                if (dictionary.ContainsKey(key) && (key != "addr:city" || !skipCity) &&
                    (key != "addr:town" || !skipTown) &&
                    (key != "addr:village" || !skipVillage))
                    addr.Add(dictionary[key]);

            var housenumber = dictionary.ContainsKey("addr:housenumber")
                ? dictionary["addr:housenumber"]
                : string.Empty;

            return await GetFiasByAddrAsync(addr.ToArray(), housenumber);
        }

        /// <summary>
        ///     Получение списка кодов ФИАС для заданного адреса
        /// </summary>
        /// <param name="addr">Массив с элементами адреса (от старшего к младшему)</param>
        /// <param name="housenumber">Номер дома/участка</param>
        /// <returns>Список кодов ФИАС</returns>
        public async Task<List<string>> GetFiasByAddrAsync(string[] addr, string housenumber, string roomnumber = null)
        {
            var addrob = new List<List<long>>();
            var house = new List<long>();
            var stead = new List<long>();

            using (var connection = new MySqlConnection(GetSphinxConnectionString()))
            {
                var index = 0;

                foreach (var row in addr)
                {
                    var list = new List<long>();

                    for (var index2 = 0; index2 <= index; index2++)
                    {
                        var row2 = addr[index2];

                        if (!string.IsNullOrWhiteSpace(housenumber))
                        {
                            var match1 =
                                $"@housenumber ({housenumber.Yo().Escape()}) @title ({row.Yo().Escape()}) @title2 ({row2.Yo().Escape()})";
                            var dic1 = new Dictionary<string, object>
                            {
                                {"match", match1}
                            };
                            stead.FillAll(
                                "SELECT id FROM stead WHERE MATCH(@match)",
                                dic1, connection);

                            house.FillAll(
                                "SELECT id FROM house WHERE MATCH(@match)",
                                dic1, connection);
                        }

                        var match = $"@title ({row.Yo().Escape()}) @title2 ({row2.Yo().Escape()})";
                        var dic = new Dictionary<string, object>
                        {
                            {"match", match}
                        };

                        list.FillAll(
                            "SELECT id FROM addrob WHERE MATCH(@match)",
                            dic, connection);
                    }

                    if (index == 0)
                    {
                        var match = $"@title ({row.Yo().Escape()})";
                        var dic = new Dictionary<string, object>
                        {
                            {"match", match}
                        };

                        list.FillAll(
                            "SELECT id FROM addrob WHERE MATCH(@match)",
                            dic, connection);
                    }

                    if (list.Any()) addrob.Add(list);

                    index++;
                }
            }

            using (var connection = new NpgsqlConnection(GetFiasConnectionString()))
            {
                connection.Open();

                var guidaddrob = new List<List<string>>();
                var guidaddrobdic = new List<Dictionary<string, string>>();

                using (var command = new NpgsqlCommand(string.Join("\nUNION ALL\n",
                        _listAddrob.Select(x =>
                            $"SELECT aoguid,parentguid FROM {x} WHERE record_id=ANY(@ids) AND livestatus=1")),
                    connection))
                {
                    command.Parameters.Add("ids", NpgsqlDbType.Array | NpgsqlDbType.Bigint);

                    command.Prepare();

                    foreach (var list in addrob)
                    {
                        command.Parameters["ids"].Value = list.ToArray();

                        using (var reader = command.ExecuteReader())
                        {
                            var parentaddrob = new Dictionary<string, string>();
                            while (reader.Read())
                            {
                                var aoguid = reader.SafeGetString(0);
                                var parentguid = reader.SafeGetString(1);
                                parentaddrob[aoguid] = parentguid;
                            }

                            if (parentaddrob.Any()) guidaddrobdic.Add(parentaddrob);
                        }
                    }
                }

                var result = new List<string>();

                if (!guidaddrobdic.Any()) return result;

                guidaddrob.Add(guidaddrobdic[0].Keys.ToList());
                for (var index = 1; index < guidaddrobdic.Count; index++)
                    guidaddrob.Add((from pair in guidaddrobdic[index]
                        join parentguid in guidaddrob[index - 1] on pair.Value equals parentguid
                        select pair.Key).ToList());

                guidaddrob = guidaddrob.Where(x => x.Any()).ToList();

                if (!guidaddrob.Any()) return result;

                var guidaddrob_last = guidaddrob.Last().Distinct().ToList();
                var parenthouse = new Dictionary<string, string>();
                var parentstead = new Dictionary<string, string>();

                if (stead.Any())
                    using (var command = new NpgsqlCommand(string.Join("\nUNION ALL\n",
                            _listStead.Select(x =>
                                $"SELECT steadguid,parentguid FROM {x} WHERE record_id=ANY(@ids) AND livestatus=1")),
                        connection))
                    {
                        command.Parameters.AddWithValue("ids", stead.ToArray());

                        command.Prepare();

                        using (var reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                var steadguid = reader.SafeGetString(0);
                                var parentguid = reader.SafeGetString(1);
                                parentstead[steadguid] = parentguid;
                            }
                        }
                    }

                if (house.Any())
                    using (var command = new NpgsqlCommand(string.Join("\nUNION ALL\n",
                            _listHouse.Select(x =>
                                $@"SELECT houseguid,aoguid FROM {x}
                                JOIN (SELECT now() as n) as q ON startdate<=n AND n<enddate
                                WHERE record_id=ANY(@ids)")),
                        connection))
                    {
                        command.Parameters.AddWithValue("ids", house.ToArray());

                        command.Prepare();

                        using (var reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                var houseguid = reader.SafeGetString(0);
                                var aoguid = reader.SafeGetString(1);
                                parenthouse[houseguid] = aoguid;
                            }
                        }
                    }

                var guidhouse = (from pair in parenthouse
                    join parentguid in guidaddrob_last on pair.Value equals parentguid
                    select pair.Key).ToList();

                var guidstead = (from pair in parentstead
                    join parentguid in guidaddrob_last on pair.Value equals parentguid
                    select pair.Key).ToList();

                if (guidhouse.Any() || guidstead.Any())
                {
                    result.AddRange(guidhouse);
                    result.AddRange(guidstead);

                    if (!string.IsNullOrEmpty(roomnumber))
                        using (var command = new NpgsqlCommand(string.Join("\nUNION ALL\n",
                                _listRoom.Select(x => $@"SELECT roomguid,flatnumber,roomnumber FROM {x}
                                WHERE houseguid=ANY(@guids) AND livestatus=1")),
                            connection))
                        {
                            command.Parameters.AddWithValue("guids", result.ToArray());

                            command.Prepare();

                            var list = new List<Doc>();

                            var lev = new Levenshtein(roomnumber.Yo());

                            using (var reader = command.ExecuteReader())
                            {
                                while (reader.Read())
                                {
                                    var roomguid = reader.SafeGetString(0);
                                    var flatnumber = reader.SafeGetString(1);
                                    var roomnumber1 = reader.SafeGetString(2);
                                    var list1 = new List<string>();
                                    if (!string.IsNullOrEmpty(flatnumber)) list1.Add($"Квартира {flatnumber}");
                                    if (!string.IsNullOrEmpty(roomnumber)) list1.Add($"Комната {roomnumber1}");
                                    list.Add(new Doc
                                    {
                                        guid = roomguid,
                                        distance = lev.DistanceFrom(string.Join(" ", list1))
                                    });
                                }
                            }

                            if (!list.Any()) return result;

                            var min = list.Min(x => x.distance);
                            result = list.Where(x => x.distance == min).Select(x => x.guid).ToList();
                        }

                    return result;
                }

                result.AddRange(guidaddrob_last);

                return result;
            }
        }

        /// <summary>
        ///     Получение словаря элементов адреса по координатам точки
        /// </summary>
        /// <param name="latitude">Широта</param>
        /// <param name="longitude">Долгота</param>
        /// <param name="tolerance">Точность (в метрах)</param>
        /// <returns></returns>
        public async Task<Dictionary<string, string>> GetAddrByCoordsAsync(double latitude, double longitude,
            double tolerance = 100.0)
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

            using (var connection = new NpgsqlConnection(GetOsmConnectionString()))
            {
                await connection.OpenAsync();

                var result = new Dictionary<string, string>();

                using (var command =
                    new NpgsqlCommand(
                        @"SELECT tags->@key AS tag  FROM placex
                        WHERE tags?@key AND ST_DWithin(ST_SetSRID(ST_Point(@longitude,@latitude),4326),location,@tolerance/100000)
                        ORDER BY ST_SetSRID(ST_Point(@longitude,@latitude),4326)<->location LIMIT 1",
                        connection))
                {
                    command.Parameters.Add("longitude", NpgsqlDbType.Double);
                    command.Parameters.Add("latitude", NpgsqlDbType.Double);
                    command.Parameters.Add("key", NpgsqlDbType.Varchar);
                    command.Parameters.Add("tolerance", NpgsqlDbType.Double);

                    command.Prepare();

                    foreach (var key in keys)
                    {
                        command.Parameters["longitude"].Value = longitude;
                        command.Parameters["latitude"].Value = latitude;
                        command.Parameters["key"].Value = key;
                        command.Parameters["tolerance"].Value = tolerance;

                        using (var reader = command.ExecuteReader())
                        {
                            if (reader.Read()) result.Add(key, reader.GetString(0));
                        }
                    }
                }

                using (var command =
                    new NpgsqlCommand(
                        @"SELECT concat('addr:',tags->'place'),tags->'name' FROM placex WHERE tags?'place'
                        AND ST_DWithin(ST_SetSRID(ST_Point(@longitude,@latitude),4326),location,0)
                        AND ST_Within(ST_SetSRID(ST_Point(@longitude,@latitude),4326),location)",
                        connection))
                {
                    command.Parameters.AddWithValue("longitude", longitude);
                    command.Parameters.AddWithValue("latitude", latitude);

                    command.Prepare();

                    using (var reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                            if (!result.ContainsKey(reader.GetString(0)))
                                result.Add(reader.GetString(0), reader.GetString(1));
                    }
                }

                await connection.CloseAsync();

                return result;
            }
        }

        /// <summary>
        ///     Получение списка мест с координатами для заданного словаря с элементами адреса
        /// </summary>
        /// <param name="dictionary">Словарь с элементами адреса</param>
        /// <returns>Список мест с координатами</returns>
        public async Task<List<Placex>> GetOsmByAddrAsync(Dictionary<string, string> dictionary)
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
                "addr:street"
            };

            var addr = new List<string>();

            var skipCity = dictionary.ContainsKey("addr:region") && dictionary.ContainsKey("addr:city") &&
                           dictionary["addr:region"] == dictionary["addr:city"];

            var skipTown = dictionary.ContainsKey("addr:city") && dictionary.ContainsKey("addr:town") &&
                           dictionary["addr:city"] == dictionary["addr:town"];

            var skipVillage = dictionary.ContainsKey("addr:city") && dictionary.ContainsKey("addr:village") &&
                              dictionary["addr:city"] == dictionary["addr:village"];

            foreach (var key in keys)
                if (dictionary.ContainsKey(key) && (key != "addr:city" || !skipCity) &&
                    (key != "addr:town" || !skipTown) &&
                    (key != "addr:village" || !skipVillage))
                    addr.Add(dictionary[key]);

            var housenumber = dictionary.ContainsKey("addr:housenumber")
                ? dictionary["addr:housenumber"]
                : string.Empty;

            return await GetOsmByAddrAsync(addr.ToArray(), housenumber);
        }

        /// <summary>
        ///     Получение списка мест с координатами для заданного адреса
        /// </summary>
        /// <param name="addr">Массив с элементами адреса (от старшего к младшему)</param>
        /// <param name="housenumber">Номер дома/участка</param>
        /// <returns>Список мест с координатами</returns>
        public async Task<List<Placex>> GetOsmByAddrAsync(string[] addr, string housenumber)
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

            var result = new List<Placex>();

            var list = new List<string>(addr.Length + 1);
            list.AddRange(addr);
            if (!string.IsNullOrWhiteSpace(housenumber)) list.Add(housenumber);

            var match = string.Join(" NOTNEAR/99 ", list.Select(x => $"({string.Join(" NEAR/9 ", _spaceRegex.Split(x).Select(y => y.Yo().Escape()))})"));

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
                            @"SELECT id,tags,location FROM placex WHERE tags?|@keys AND id=ANY(@ids)",
                            npgsqlConnection))
                    {
                        command.Parameters.AddWithValue("keys", keys.ToArray());
                        command.Parameters.AddWithValue("ids", ids.ToArray());

                        command.Prepare();

                        using (var reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                                result.Add(new Placex
                                {
                                    id = reader.GetInt64(0),
                                    tags = (Dictionary<string, string>) reader.GetValue(1),
                                    location = (GeoJSONObject) reader.GetValue(2)
                                });
                        }
                    }

                    if (result.Any()) break;
                }

                await npgsqlConnection.CloseAsync();

                return result;
            }
        }

        /// <summary>
        ///     Получение списка кодов ФИАС для заданного адреса
        /// </summary>
        /// <param name="addr">Массив с элементами адреса (от старшего к младшему)</param>
        /// <param name="housenumber">Номер дома/участка</param>
        /// <param name="roomnumber">Номер квартиры</param>
        /// <returns>Список кодов ФИАС</returns>
        public async Task<List<string>> GetFiasByAddr2Async(string[] addr, string housenumber, string roomnumber)
        {
            var result = new List<string>();

            var list = new List<string>(addr.Length + 2);
            list.AddRange(addr);
            if (!string.IsNullOrWhiteSpace(housenumber)) list.Add(housenumber);
            if (!string.IsNullOrWhiteSpace(roomnumber)) list.Add(roomnumber);

            var match = string.Join(" NOTNEAR/99 ", list.Select(x => $"({string.Join(" NEAR/9 ", _spaceRegex.Split(x).Select(y => y.Yo().Escape()))})"));

            using (var npgsqlConnection = new NpgsqlConnection(GetFiasConnectionString()))
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
                        "SELECT id FROM addrobx WHERE MATCH(@match) AND priority=@priority",
                        dic, connection);

                    if (!ids.Any()) continue;

                    using (var command = new NpgsqlCommand(_addrobSql, npgsqlConnection))
                    {
                        command.Parameters.AddWithValue("ids", ids.ToArray());

                        command.Prepare();

                        using (var reader = command.ExecuteReader())
                        {
                            result.Fill(reader);
                        }
                    }

                    if (result.Any()) break;
                }

                await npgsqlConnection.CloseAsync();

                return result;
            }
        }

        public async Task<List<string>> GetOsmSuggestAsync(string search, int limit = 20)
        {
            var list = search.Split(",");
            var result = new List<string>();

            var match = string.Join(" NOTNEAR/99 ", list.Select(x => $"({string.Join(" NEAR/9 ", _spaceRegex.Split(x).Select(y => y.Yo().Escape()))})"));

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
                        "SELECT title FROM addrx WHERE MATCH(@match) AND priority=@priority",
                        dic, connection, limit: limit);
                    limit -= count;
                }

                return result;
            }
        }

        public async Task<List<string>> GetFiasSuggestAsync(string search, int limit = 20)
        {
            var list = search.Split(",");
            var result = new List<string>();

            var match = string.Join(" NOTNEAR/99 ", list.Select(x => $"({string.Join(" NEAR/9 ", _spaceRegex.Split(x).Select(y => y.Yo().Escape()))})"));

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
    }
}