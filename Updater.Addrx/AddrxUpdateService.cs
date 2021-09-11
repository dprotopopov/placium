using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using Placium.Common;
using Placium.Types;

namespace Updater.Addrx
{
    public class AddrxUpdateService : BaseAppService, IUpdateService
    {
        private readonly IProgressClient _progressClient;

        public AddrxUpdateService(IConnectionsConfig configuration, IProgressClient progressClient) : base(
            configuration)
        {
            _progressClient = progressClient;
        }

        public async Task UpdateAsync(string session, bool full)
        {
            if (full)
                using (var npgsqlConnection = new NpgsqlConnection(GetOsmConnectionString()))
                {
                    await npgsqlConnection.OpenAsync();

                    npgsqlConnection.ReloadTypes();
                    npgsqlConnection.TypeMapper.MapEnum<OsmServiceType>("service_type");

                    SetLastRecordNumber(npgsqlConnection, OsmServiceType.Placex, 0);

                    await npgsqlConnection.CloseAsync();
                }

            await UpdatePlacexAsync(session, full);
        }

        public async Task UpdatePlacexAsync(string session, bool full)
        {
            var id = Guid.NewGuid().ToString();
            await _progressClient.Init(id, session);

            using (var connection = new NpgsqlConnection(GetOsmConnectionString()))
            {
                await connection.OpenAsync();

                connection.ReloadTypes();
                connection.TypeMapper.MapComposite<OsmRelationMember>("relation_member");
                connection.TypeMapper.MapEnum<OsmType>("osm_type");
                connection.TypeMapper.MapEnum<OsmServiceType>("service_type");

                var last_record_number = GetLastRecordNumber(connection, OsmServiceType.Placex, full);
                var next_last_record_number = GetNextLastRecordNumber(connection);

                var keys = new[]
                {
                    "addr:region",
                    "addr:district",
                    "addr:city",
                    "addr:subdistrict",
                    "addr:suburb",
                    "addr:hamlet",
                    "addr:street",
                    "addr:housenumber"
                };

                var keys1 = new[]
                {
                    "addr:region",
                    "addr:district",
                    "addr:city",
                    "addr:subdistrict",
                    "addr:suburb",
                    "addr:hamlet",
                    "addr:street",
                    "addr:housenumber",
                    "place"
                };

                var total = 0L;

                using (var command = new NpgsqlCommand(string.Join(";",
                        "SELECT COUNT(*) FROM placex WHERE tags?|@keys AND record_number>@last_record_number",
                        "SELECT id FROM placex WHERE tags?|@keys AND record_number>@last_record_number"),
                    connection))
                {
                    command.Parameters.AddWithValue("keys", keys1.ToArray());
                    command.Parameters.AddWithValue("last_record_number", last_record_number);

                    command.Prepare();

                    using (var reader = command.ExecuteReader())
                    {
                        if (reader.Read())
                            total = reader.GetInt64(0);

                        reader.NextResult();

                        var current = 0L;
                        var take = 1000;

                        var obj = new object();
                        var reader_is_empty = false;

                        Parallel.For(0, 12,
                            i =>
                            {
                                using (var connection2 = new NpgsqlConnection(GetOsmConnectionString()))
                                {
                                    connection2.Open();

                                    using (var command2 = new NpgsqlCommand(@"INSERT INTO addrx(id,tags)
                                        SELECT id, hstore(array_agg(key), array_agg(val)) as tags
                                        FROM (SELECT c.id, unnest(akeys(p.tags)) as key, unnest(avals(p.tags)) as val FROM placex c
                                        JOIN placex p ON c.location@p.location AND ST_Within(c.location,p.location)
                                        WHERE c.id=ANY(@ids) AND p.tags?|@keys UNION ALL
                                        SELECT c.id, concat('addr:',p.tags->'place') as key, p.tags->'name' as val FROM placex c
                                        JOIN placex p ON c.location@p.location AND ST_Within(c.location,p.location)
                                        WHERE c.id=ANY(@ids) AND p.tags?'place' UNION ALL
                                        SELECT c.id, 'addr:region' as key, p.tags->'name' as val FROM placex c
                                        JOIN placex p ON c.location@p.location AND ST_Within(c.location,p.location)
                                        WHERE c.id=ANY(@ids) AND p.tags->'type'='boundary' and p.tags->'admin_level'='4' UNION ALL
                                        SELECT c.id, 'addr:district' as key, p.tags->'name' as val FROM placex c
                                        JOIN placex p ON c.location@p.location AND ST_Within(c.location,p.location)
                                        WHERE c.id=ANY(@ids) AND p.tags->'type'='boundary' and p.tags->'admin_level'='5') as q
                                        WHERE key like 'addr%' GROUP BY id
                                        ON CONFLICT(id) DO UPDATE SET tags = EXCLUDED.tags,record_number = nextval('record_number_seq')",
                                        connection2))
                                    {
                                        command2.Parameters.AddWithValue("keys", keys.ToArray());
                                        command2.Parameters.Add("ids", NpgsqlDbType.Array | NpgsqlDbType.Bigint);

                                        command2.Prepare();

                                        while (true)
                                        {
                                            List<long> list;
                                            lock (obj)
                                            {
                                                if (reader_is_empty) break;
                                                list = GetLongs(reader, take);
                                                reader_is_empty = list.Count() < take;
                                                if (!list.Any()) break;
                                            }

                                            command2.Parameters["ids"].Value = list.ToArray();

                                            command2.ExecuteNonQuery();

                                            lock (obj)
                                            {
                                                current += list.Count();

                                                _progressClient.Progress(100f * current / total, id, session)
                                                    .GetAwaiter()
                                                    .GetResult();
                                            }
                                        }
                                    }

                                    connection2.Close();
                                }
                            });
                    }
                }

                SetLastRecordNumber(connection, OsmServiceType.Placex, next_last_record_number);

                await connection.CloseAsync();

                await _progressClient.Progress(100f, id, session);
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