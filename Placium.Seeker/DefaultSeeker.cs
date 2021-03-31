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
        private readonly List<string> _keys = new List<string>
        {
            "addr:district",
            "addr:subdistrict",
            "addr:suburb",
            "addr:hamlet",
            "addr:city",
            "addr:place",
            "addr:street"
        };

        private readonly List<string> _listAddrob = new List<string>();
        private readonly List<string> _listHouse = new List<string>();
        private readonly List<string> _listStead = new List<string>();

        public DefaultSeeker(IConfiguration configuration) : base(configuration)
        {
            using (var connection = new NpgsqlConnection(GetFiasConnectionString()))
            {
                connection.Open();

                _listAddrob.Fill(
                    @"SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' and table_name similar to 'addrob\d+'",
                    connection);
                _listHouse.Fill(
                    @"SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' and table_name similar to 'house\d+'",
                    connection);
                _listStead.Fill(
                    @"SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' and table_name similar to 'stead\d+'",
                    connection);
            }
        }

        public async Task<List<string>> AddrToFias(Dictionary<string, string> dictionary)
        {
            var addr = new List<string>();
            foreach (var key in _keys)
                if (dictionary.ContainsKey(key))
                    addr.Add(dictionary[key]);
            var housenumber = dictionary["addr:housenumber"];


            var addrob = new List<List<long>>();
            var house = new List<long>();
            var stead = new List<long>();

            using (var connection = new MySqlConnection(GetSphinxConnectionString()))
            {
                connection.TryOpen();
                stead.Fill($"SELECT id FROM stead WHERE MATCH('{housenumber.TextEscape()}')", connection);

                connection.TryOpen();
                house.Fill($"SELECT id FROM house WHERE MATCH('{housenumber.TextEscape()}')", connection);

                foreach (var row in addr)
                {
                    var list = new List<long>();

                    connection.TryOpen();
                    list.Fill($"SELECT id FROM addrob WHERE MATCH('{row.TextEscape()}')", connection);

                    if (list.Any()) addrob.Add(list);
                }
            }

            using (var connection = new NpgsqlConnection(GetFiasConnectionString()))
            {
                connection.Open();

                var guidaddrob = new List<List<string>>();
                var guidhouse = new List<string>();
                var guidstead = new List<string>();
                var parent = new Dictionary<string, string>();

                if (stead.Any())
                    using (var command = new NpgsqlCommand(string.Join("\nUNION ALL\n",
                            _listStead.Select(x =>
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
                            _listHouse.Select(x =>
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
                            _listAddrob.Select(x =>
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
    }
}