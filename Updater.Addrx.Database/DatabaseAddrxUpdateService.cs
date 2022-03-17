using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using Placium.Common;
using Placium.Types;

namespace Updater.Addrx.Database
{
    public class DatabaseAddrxUpdateService : BaseAppService, IUpdateService
    {
        private readonly IParallelConfig _parallelConfig;
        private readonly IProgressClient _progressClient;

        public DatabaseAddrxUpdateService(IConnectionsConfig configuration, IProgressClient progressClient,
            IParallelConfig parallelConfig) : base(
            configuration)
        {
            _progressClient = progressClient;
            _parallelConfig = parallelConfig;
        }

        public async Task UpdateAsync(string session, bool full)
        {
            if (full)
            {
                await using var npgsqlConnection = new NpgsqlConnection(GetOsmConnectionString());
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

            await using var connection = new NpgsqlConnection(GetOsmConnectionString());

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
                "admin_level",
                "place",
                "landuse",
                "highway",
                "railway",
                "bridge",
                "tunnel",
                "building",
                "shop",
                "station",
                "platform",
                "man_made",
                "natural"
            };

            var total = 0L;

            await using (var command = new NpgsqlCommand(string.Join(";",
                                 "SELECT COUNT(*) FROM placex WHERE tags?|@keys AND record_number>@last_record_number",
                                 "SELECT id FROM placex WHERE tags?|@keys AND record_number>@last_record_number"),
                             connection))
            {
                command.Parameters.AddWithValue("keys", keys1.ToArray());
                command.Parameters.AddWithValue("last_record_number", last_record_number);

                await command.PrepareAsync();

                await using (var reader = command.ExecuteReader())
                {
                    if (reader.Read())
                        total = reader.GetInt64(0);

                    reader.NextResult();

                    var current = 0L;
                    var take = 1000;

                    var obj = new object();
                    var reader_is_empty = false;

                    Parallel.For(0, _parallelConfig.GetNumberOfThreads(),
                        i =>
                        {
                            using var connection2 = new NpgsqlConnection(GetOsmConnectionString());
                            connection2.Open();

                            using (var command2 = new NpgsqlCommand(@"INSERT INTO addrx(id,tags)
                                        SELECT id, hstore(array_agg(key), array_agg(val)) as tags
                                        FROM (SELECT c.id, unnest(akeys(c.tags)) as key, unnest(avals(c.tags)) as val FROM placex c WHERE c.id=ANY(@ids) UNION ALL
                                        SELECT id,key,val FROM (SELECT c.id as id, unnest(akeys(p.tags)) as key, unnest(avals(p.tags)) as val FROM placex c
                                        JOIN placex p ON c.location@p.location AND ST_Within(c.location,p.location)
                                        WHERE c.id=ANY(@ids) AND p.tags?|@keys) as q1 WHERE key like 'addr%' UNION ALL " +
                                                                    string.Join(" UNION ALL ", new[] { "place" }.Select(
                                                                        x => $@"
                                        SELECT c.id, concat('addr:',p.tags->'{x}') as key, CASE WHEN p.tags?'name' THEN p.tags->'name' ELSE p.tags->'ref' END as val FROM placex c
                                        JOIN placex p ON c.location@p.location AND ST_Within(c.location,p.location)
                                        WHERE c.id=ANY(@ids) AND p.tags?'{x}' AND (p.tags?'name' OR p.tags?'ref')")) +
                                                                    " UNION ALL " +
                                                                    string.Join(" UNION ALL ",
                                                                        new[] { "highway", "railway" }.Select(x => $@"
                                        SELECT c.id, concat('addr:{x}:',p.tags->'{x}') as key, CASE WHEN p.tags?'name' THEN p.tags->'name' ELSE p.tags->'ref' END as val FROM placex c
                                        JOIN placex p ON c.location@p.location AND ST_Within(c.location,p.location)
                                        WHERE c.id=ANY(@ids) AND p.tags?'{x}' AND (p.tags?'name' OR p.tags?'ref')")) +
                                                                    " UNION ALL " +
                                                                    string.Join(" UNION ALL ", new[]
                                                                    {
                                                                        "building", "man_made", "natural",
                                                                        "landuse", "shop", "bridge", "tunnel",
                                                                        "station", "platform"
                                                                    }.Select(x => $@"
                                        SELECT c.id, 'addr:{x}' as key, CASE WHEN p.tags?'name' THEN p.tags->'name' ELSE p.tags->'ref' END as val FROM placex c
                                        JOIN placex p ON c.location@p.location AND ST_Within(c.location,p.location)
                                        WHERE c.id=ANY(@ids) AND p.tags?'{x}' AND (p.tags?'name' OR p.tags?'ref')")) +
                                                                    " UNION ALL " +
                                                                    string.Join(" UNION ALL ",
                                                                        new Dictionary<string, string>
                                                                        {
                                                                            { "4", "region" },
                                                                            { "5", "district" },
                                                                            { "6", "municipality" },
                                                                            { "8", "subdistrict" }
                                                                        }.Select(x => $@"
                                        SELECT c.id, 'addr:{x.Value}' as key, CASE WHEN p.tags?'name' THEN p.tags->'name' ELSE p.tags->'ref' END as val FROM placex c
                                        JOIN placex p ON c.location@p.location AND ST_Within(c.location,p.location)
                                        WHERE c.id=ANY(@ids) AND p.tags->'type'='boundary' AND p.tags->'admin_level'='{x.Key}' AND (p.tags?'name' OR p.tags?'ref')")) +
                                                                    @") as q
                                        GROUP BY id ON CONFLICT(id) DO UPDATE SET tags = EXCLUDED.tags,record_number = nextval('record_number_seq')",
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
                        });
                }
            }

            SetLastRecordNumber(connection, OsmServiceType.Placex, next_last_record_number);

            await connection.CloseAsync();

            await _progressClient.Finalize(id, session);
        }

        private List<long> GetLongs(NpgsqlDataReader reader, int take)
        {
            var list = new List<long>(take);
            for (var i = 0; i < take && reader.Read(); i++) list.Add(reader.GetInt64(0));
            return list;
        }
    }
}