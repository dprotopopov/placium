using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Npgsql;
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
            {
                await connection.OpenAsync();

                await ExecuteResourceAsync(Assembly.GetExecutingAssembly(), "Updater.Addr.CreateTable.sql",
                    connection);

                var total = 0L;

                using (var command = new NpgsqlCommand(string.Join(";",
                    "SELECT COUNT(*) FROM placex", "SELECT id FROM placex"), connection))
                {
                    command.Prepare();

                    using (var reader = command.ExecuteReader())
                    {
                        if (reader.Read())
                            total = reader.GetInt64(0);

                        reader.NextResult();

                        var current = 0L;
                        var take = 1000;

                        var obj = new object();

                        Parallel.For(0, (total + take - 1) / take, new ParallelOptions
                            {
                                MaxDegreeOfParallelism = 4
                            },
                            i =>
                            {
                                List<long> list;
                                lock (obj)
                                {
                                    list = GetLongs(reader, take);
                                }

                                using (var connection2 = new NpgsqlConnection(GetOsmConnectionString()))
                                {
                                    connection2.Open();

                                    using (var command2 = new NpgsqlCommand(@"INSERT INTO addr(id,tags)
                                        SELECT c.id, hstore(array_agg(p.key), array_agg(p.val)) as tags
                                        FROM placex c, (SELECT id, unnest(akeys(tags)) as key, unnest(avals(tags)) as val, location FROM placex) as p
                                        WHERE c.id=ANY(@ids) AND key like 'addr%' AND ST_CoveredBy(c.location, p.location)
                                        GROUP BY c.id
                                        ON CONFLICT(id) DO UPDATE SET tags = EXCLUDED.tags,record_number = nextval('record_number_seq')",
                                        connection2))
                                    {
                                        command2.Parameters.AddWithValue("ids", list.ToArray());

                                        command2.Prepare();

                                        command2.ExecuteNonQuery();

                                        lock (obj)
                                        {
                                            current += list.Count();
                                        }

                                        _progressHub.ProgressAsync(100f * current / total, id, session).GetAwaiter().GetResult();
                                    }

                                    connection2.Close();
                                }
                            });
                    }
                }

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