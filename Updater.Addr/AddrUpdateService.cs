using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Npgsql;
using NpgsqlTypes;
using Placium.Common;

namespace Updater.Addr
{
    public class AddrUpdateService : BaseService, IUpdateService
    {
        private readonly ProgressHub _progressHub;

        public AddrUpdateService(IConfiguration configuration, ProgressHub progressHub) : base(configuration)
        {
            _progressHub = progressHub;
        }

        public async Task UpdateAsync(string session)
        {
            var id = Guid.NewGuid().ToString();
            await _progressHub.InitAsync(id, session);

            using (var connection = new NpgsqlConnection(GetOsmConnectionString()))
            using (var connection2 = new NpgsqlConnection(GetOsmConnectionString()))
            {
                await connection.OpenAsync();
                await connection2.OpenAsync();

                await ExecuteResourceAsync(Assembly.GetExecutingAssembly(), "Updater.Addr.CreateTable.sql",
                    connection);

                var total = 0L;

                using (var command = new NpgsqlCommand(string.Join(";",
                    "SELECT COUNT(*) FROM placex", "SELECT id FROM placex"), connection))
                using (var command2 = new NpgsqlCommand("INSERT INTO addr(id,tags)" +
                                                        " SELECT c.id, hstore(array_agg(p.key), array_agg(p.val)) as tags" +
                                                        " FROM placex c, (SELECT id, unnest(akeys(tags)) as key, unnest(avals(tags)) as val, location FROM placex) as p" +
                                                        " WHERE c.id=ANY(@ids) AND key like 'addr%' AND ST_Within(c.location::geometry, p.location::geometry)" +
                                                        " GROUP BY c.id" +
                                                        " ON CONFLICT(id) DO UPDATE SET tags = EXCLUDED.tags,record_number = nextval('record_number_seq')",
                    connection2))
                {
                    command.Prepare();

                    command2.Parameters.Add("ids", NpgsqlDbType.Array | NpgsqlDbType.Bigint);

                    command2.Prepare();

                    using (var reader = command.ExecuteReader())
                    {
                        if (reader.Read())
                            total = reader.GetInt64(0);

                        reader.NextResult();

                        var current = 0L;
                        var take = 10000;

                        while (true)
                        {
                            var list = GetLongs(reader, take);
                            
                            if (list.Any())
                            {
                                command2.Parameters["ids"].Value = list.ToArray();

                                command2.ExecuteNonQuery();

                                current += list.Count();

                                await _progressHub.ProgressAsync(100f * current / total, id, session);
                            }

                            if (list.Count() < take) break;
                        }
                    }
                }

                await connection2.CloseAsync();
                await connection.CloseAsync();

                await _progressHub.ProgressAsync(100f, id, session);
            }
        }

        private List<long> GetLongs(NpgsqlDataReader reader, int take)
        {
            var list = new List<long>(take);
            for (var i = 0; i < take && reader.Read(); i++) list.Add(reader.GetInt64(0));
            return list;
        }
    }
}