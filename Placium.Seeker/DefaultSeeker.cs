using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using MySql.Data.MySqlClient;
using Npgsql;
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

            var addrob = new List<List<long>>();
            var house = new List<long>();
            var stead = new List<long>();

            using (var connection = new MySqlConnection(GetSphinxConnectionString()))
            {
                if (!string.IsNullOrWhiteSpace(housenumber))
                {
                    stead.Fill($"SELECT id FROM stead WHERE MATCH('{housenumber.TextEscape()}')", connection);

                    house.Fill($"SELECT id FROM house WHERE MATCH('{housenumber.TextEscape()}')", connection);
                }

                foreach (var row in addr)
                {
                    var list = new List<long>();

                    list.Fill($"SELECT id FROM addrob WHERE MATCH('{row.TextEscape()}')", connection);

                    if (list.Any()) addrob.Add(list);
                }
            }

            using (var connection = new NpgsqlConnection(GetFiasConnectionString()))
            {
                connection.Open();

                var listAddrob = new List<string>();
                var listHouse = new List<string>();
                var listStead = new List<string>();

                listAddrob.Fill(
                    @"SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' and table_name similar to 'addrob\d+'",
                    connection);
                listHouse.Fill(
                    @"SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' and table_name similar to 'house\d+'",
                    connection);
                listStead.Fill(
                    @"SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' and table_name similar to 'stead\d+'",
                    connection);

                var guidaddrob = new List<List<string>>();
                var guidhouse = new List<string>();
                var guidstead = new List<string>();
                var parent = new Dictionary<string, string>();

                if (stead.Any())
                    using (var command = new NpgsqlCommand(string.Join("\nUNION ALL\n",
                            listStead.Select(x =>
                                $"SELECT steadguid,parentguid FROM {x} WHERE record_id=ANY(@ids)")),
                        connection))
                    {
                        command.Parameters.AddWithValue("ids", stead.ToArray());
                        using (var reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                var steadguid = reader.SafeGetString(0);
                                var parentguid = reader.SafeGetString(1);
                                guidstead.Add(steadguid);
                                parent[steadguid] = parentguid;
                            }
                        }
                    }

                if (house.Any())
                    using (var command = new NpgsqlCommand(string.Join("\nUNION ALL\n",
                            listHouse.Select(x =>
                                $"SELECT houseguid,aoguid FROM {x} WHERE record_id=ANY(@ids)")),
                        connection))
                    {
                        command.Parameters.AddWithValue("ids", house.ToArray());
                        using (var reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                var houseguid = reader.SafeGetString(0);
                                var aoguid = reader.SafeGetString(1);
                                guidhouse.Add(houseguid);
                                parent[houseguid] = aoguid;
                            }
                        }
                    }

                foreach (var list in addrob)
                    using (var command = new NpgsqlCommand(string.Join("\nUNION ALL\n",
                            listAddrob.Select(x =>
                                $"SELECT aoguid,parentguid FROM {x} WHERE record_id=ANY(@ids)")),
                        connection))
                    {
                        command.Parameters.AddWithValue("ids", list.ToArray());
                        using (var reader = command.ExecuteReader())
                        {
                            var guidlist = new List<string>();
                            while (reader.Read())
                            {
                                var aoguid = reader.SafeGetString(0);
                                var parentguid = reader.SafeGetString(1);
                                guidlist.Add(aoguid);
                                parent[aoguid] = parentguid;
                            }

                            if (guidlist.Any()) guidaddrob.Add(guidlist);
                        }
                    }

                for (var index = 1; index < guidaddrob.Count; index++)
                    guidaddrob[index] = guidaddrob[index].Where(x => guidaddrob[index - 1].Any(y => parent[x] == y))
                        .ToList();

                guidaddrob = guidaddrob.Where(x => x.Any()).ToList();

                var result = new List<string>();
                if (!guidaddrob.Any()) return result;

                guidhouse = guidhouse.Where(x => guidaddrob.Last().Any(y => parent[x] == y))
                    .ToList();
                guidstead = guidstead.Where(x => guidaddrob.Last().Any(y => parent[x] == y))
                    .ToList();

                if (guidhouse.Any() || guidstead.Any())
                {
                    result.AddRange(guidhouse);
                    result.AddRange(guidstead);
                    return result;
                }

                result.AddRange(guidaddrob.Last());
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

                foreach (var key in keys)
                    using (var command =
                        new NpgsqlCommand(
                            "SELECT tag FROM (SELECT tags->@key AS tag,ST_Distance(location,ST_SetSRID(ST_Point(@longitude,@latitude),4326)::geography) AS distance FROM place WHERE tags?@key) AS query WHERE distance<=@tolerance ORDER BY distance LIMIT 1",
                            connection))
                    {
                        command.Parameters.AddWithValue("longitude", (float) longitude);
                        command.Parameters.AddWithValue("latitude", (float) latitude);
                        command.Parameters.AddWithValue("key", key);
                        command.Parameters.AddWithValue("tolerance", tolerance);
                        using (var reader = command.ExecuteReader())
                        {
                            if (reader.Read()) result.Add(key, reader.GetString(0));
                        }
                    }

                await connection.CloseAsync();

                return result;
            }
        }
    }
}