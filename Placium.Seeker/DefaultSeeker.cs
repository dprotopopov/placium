using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using MySql.Data.MySqlClient;
using Npgsql;
using NpgsqlTypes;
using Placium.Common;

namespace Placium.Seeker
{
    public class DefaultSeeker : BaseService
    {
        public DefaultSeeker(IConfiguration configuration) : base(configuration)
        {
        }

        public async Task<List<string>> GetFiasByAddrAsync(Dictionary<string, string> dictionary)
        {
            var keys = new[]
            {
                "addr:region",
                "addr:district",
                "addr:subdistrict",
                "addr:city",
                "addr:suburb",
                "addr:hamlet",
                "addr:street"
            };

            var addr = new List<string>();

            var skipCity = dictionary.ContainsKey("addr:region") && dictionary.ContainsKey("addr:city") &&
                           dictionary["addr:region"] == dictionary["addr:city"];

            foreach (var key in keys)
                if (dictionary.ContainsKey(key) && (key != "addr:city" || !skipCity))
                    addr.Add(dictionary[key]);

            var housenumber = dictionary.ContainsKey("addr:housenumber")
                ? dictionary["addr:housenumber"]
                : string.Empty;

            return await GetFiasByAddrAsync(addr.ToArray(), housenumber);
        }

        public async Task<List<string>> GetFiasByAddrAsync(string[] addr, string housenumber)
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
                            stead.FillAll(
                                $"SELECT id FROM stead WHERE MATCH('({housenumber.Yo().Escape()})<<({row.Yo().Escape()})<<({row2.Yo().Escape()})')",
                                connection);

                            house.FillAll(
                                $"SELECT id FROM house WHERE MATCH('({housenumber.Yo().Escape()})<<({row.Yo().Escape()})<<({row2.Yo().Escape()})')",
                                connection);
                        }

                        list.FillAll(
                            $"SELECT id FROM addrob WHERE MATCH('({row.Yo().Escape()})<<({row2.Yo().Escape()})')",
                            connection);
                    }

                    if (list.Any()) addrob.Add(list);

                    index++;
                }
            }

            using (var connection = new NpgsqlConnection(GetFiasConnectionString()))
            {
                connection.Open();

                var listAddrob = new List<string>();
                var listHouse = new List<string>();
                var listStead = new List<string>();

                using (var command = new NpgsqlCommand(
                    string.Join(";", new[] {@"addrob\d+", @"house\d+", @"stead\d+"}.Select(x =>
                        $"SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' and table_name similar to '{x}'")),
                    connection))
                {
                    command.Prepare();

                    using (var reader = command.ExecuteReader())
                    {
                        listAddrob.Fill(reader);
                        reader.NextResult();
                        listHouse.Fill(reader);
                        reader.NextResult();
                        listStead.Fill(reader);
                    }
                }

                var guidaddrob = new List<List<string>>();
                var parentaddrob = new Dictionary<string, string>();

                using (var command = new NpgsqlCommand(string.Join("\nUNION ALL\n",
                        listAddrob.Select(x =>
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
                            var guidlist = new List<string>();
                            while (reader.Read())
                            {
                                var aoguid = reader.SafeGetString(0);
                                var parentguid = reader.SafeGetString(1);
                                guidlist.Add(aoguid);
                                parentaddrob[aoguid] = parentguid;
                            }

                            if (guidlist.Any()) guidaddrob.Add(guidlist);
                        }
                    }
                }

                for (var index = 1; index < guidaddrob.Count; index++)
                    guidaddrob[index] = (from guid in guidaddrob[index].Distinct()
                        join pair in parentaddrob on guid equals pair.Key
                        join parentguid in guidaddrob[index - 1].Distinct() on pair.Value equals parentguid
                        select guid).ToList();

                guidaddrob = guidaddrob.Where(x => x.Any()).ToList();

                var result = new List<string>();
                if (!guidaddrob.Any()) return result;

                var guidaddrob_last = guidaddrob.Last().Distinct().ToList();
                var parenthouse = new Dictionary<string, string>();
                var parentstead = new Dictionary<string, string>();

                if (stead.Any())
                    using (var command = new NpgsqlCommand(string.Join("\nUNION ALL\n",
                            listStead.Select(x =>
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
                            listHouse.Select(x =>
                                $"SELECT houseguid,aoguid FROM {x} WHERE record_id=ANY(@ids) AND startdate<=now() AND now()<enddate")),
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

                    return result;
                }

                result.AddRange(guidaddrob_last);

                return result;
            }
        }

        public async Task<Dictionary<string, string>> GetAddrByCoordsAsync(double latitude, double longitude,
            double tolerance = 100.0)
        {
            var keys = new[]
            {
                "addr:region",
                "addr:district",
                "addr:subdistrict",
                "addr:city",
                "addr:suburb",
                "addr:hamlet",
                "addr:street",
                "addr:housenumber"
            };

            using (var connection = new NpgsqlConnection(GetOsmConnectionString()))
            {
                await connection.OpenAsync();

                var result = new Dictionary<string, string>();

                using (var command =
                    new NpgsqlCommand(
                        "SELECT tag FROM (SELECT tags->@key AS tag,ST_Distance(location,ST_SetSRID(ST_Point(@longitude,@latitude),4326)::geography) AS distance FROM place WHERE tags?@key) AS query WHERE distance<=@tolerance ORDER BY distance LIMIT 1",
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

                await connection.CloseAsync();

                return result;
            }
        }
    }
}