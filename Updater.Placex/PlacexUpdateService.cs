using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Npgsql;
using NpgsqlTypes;
using Placium.Common;
using Placium.Types;

namespace Updater.Placex
{
    public class PlacexUpdateService : BaseService, IUpdateService
    {
        private readonly NumberFormatInfo _nfi = new NumberFormatInfo {NumberDecimalSeparator = "."};
        private readonly ProgressHub _progressHub;

        public PlacexUpdateService(ProgressHub progressHub, IConfiguration configuration) : base(configuration)
        {
            _progressHub = progressHub;
        }

        public async Task UpdateAsync(string session)
        {
            await UpdateFromNodeAsync(session);
            await UpdateFromWayAsync(session);
            await UpdateFromRelationAsync(session);
        }

        public async Task UpdateFromNodeAsync(string session)
        {
            var current = 0L;
            var total = 0L;

            var id = Guid.NewGuid().ToString();
            await _progressHub.InitAsync(id, session);

            using (var connection = new NpgsqlConnection(GetOsmConnectionString()))
            using (var connection2 = new NpgsqlConnection(GetOsmConnectionString()))
            {
                await connection.OpenAsync();
                await connection2.OpenAsync();

                connection.ReloadTypes();
                connection.TypeMapper.MapComposite<OsmRelationMember>("relation_member");
                connection.TypeMapper.MapEnum<OsmType>("osm_type");
                connection.TypeMapper.MapEnum<OsmServiceType>("service_type");

                var last_record_number = GetLastRecordNumber(connection, OsmServiceType.Node);
                var next_last_record_number = GetNextLastRecordNumber(connection);

                var keys = new List<string> {"name"};

                using (var command = new NpgsqlCommand(
                    "SELECT key FROM (SELECT DISTINCT unnest(akeys(tags)) AS key FROM node WHERE record_number>@last_record_number) AS keys WHERE key LIKE 'addr%'"
                    , connection))
                {
                    command.Parameters.AddWithValue("last_record_number", last_record_number);

                    command.Prepare();

                    using (var reader = command.ExecuteReader())
                    {
                        keys.Fill(reader);
                    }
                }

                using (var command =
                    new NpgsqlCommand(
                        string.Join(";", "DROP TABLE IF EXISTS temp_placex_node",
                            "CREATE TEMP TABLE temp_placex_node (osm_id BIGINT,tags hstore,location GEOMETRY)"),
                        connection2))
                {
                    command.Prepare();

                    command.ExecuteNonQuery();
                }

                using (var writer = connection2.BeginTextImport(
                    "COPY temp_placex_node (osm_id,tags,location) FROM STDIN WITH NULL AS ''"))
                using (var command = new NpgsqlCommand(string.Join(";",
                        "SELECT COUNT(*) FROM node WHERE tags?|@keys AND record_number>@last_record_number",
                        "SELECT id,cast(tags as text),longitude,latitude FROM node WHERE tags?|@keys AND record_number>@last_record_number"),
                    connection))
                {
                    command.Parameters.AddWithValue("keys", keys.ToArray());
                    command.Parameters.AddWithValue("last_record_number", last_record_number);

                    command.Prepare();

                    using (var reader = command.ExecuteReader())
                    {
                        if (reader.Read())
                            total = reader.GetInt64(0);

                        reader.NextResult();

                        while (reader.Read())
                        {
                            current++;

                            var values = new List<string>
                            {
                                reader.GetInt64(0).ToString(),
                                reader.GetString(1).TextEscape(),
                                $"SRID=4326;POINT({reader.GetDouble(2).ToString(_nfi)} {reader.GetDouble(3).ToString(_nfi)})"
                            };

                            writer.WriteLine(string.Join("\t", values));

                            if (current % 1000 == 0)
                                await _progressHub.ProgressAsync(100f * current / total, id, session);
                        }
                    }

                    await _progressHub.ProgressAsync(100f, id, session);
                }

                using (var command = new NpgsqlCommand(
                    string.Join(";",
                        @"INSERT INTO placex(osm_id,osm_type,tags,location)
                        SELECT osm_id,'node',tags,location FROM temp_placex_node
                        ON CONFLICT (osm_id,osm_type) DO UPDATE SET location=EXCLUDED.location,tags=EXCLUDED.tags,record_number=EXCLUDED.record_number",
                        "DROP TABLE temp_placex_node"), connection2))
                {
                    command.Prepare();

                    command.ExecuteNonQuery();
                }

                SetLastRecordNumber(connection, OsmServiceType.Node, next_last_record_number);

                await connection2.CloseAsync();
                await connection.CloseAsync();
            }
        }

        public async Task UpdateFromWayAsync(string session)
        {
            var current = 0L;
            var total = 0L;

            var id1 = Guid.NewGuid().ToString();
            await _progressHub.InitAsync(id1, session);

            using (var connection = new NpgsqlConnection(GetOsmConnectionString()))
            using (var connection2 = new NpgsqlConnection(GetOsmConnectionString()))
            {
                await connection.OpenAsync();
                await connection2.OpenAsync();

                connection.ReloadTypes();
                connection.TypeMapper.MapComposite<OsmRelationMember>("relation_member");
                connection.TypeMapper.MapEnum<OsmType>("osm_type");
                connection.TypeMapper.MapEnum<OsmServiceType>("service_type");

                var last_record_number = GetLastRecordNumber(connection, OsmServiceType.Way);
                var next_last_record_number = GetNextLastRecordNumber(connection);

                var keys = new List<string> {"name"};

                using (var command = new NpgsqlCommand(
                    "SELECT key FROM (SELECT DISTINCT unnest(akeys(tags)) AS key FROM way WHERE record_number>@last_record_number) AS keys WHERE key LIKE 'addr%'"
                    , connection))
                {
                    command.Parameters.AddWithValue("last_record_number", last_record_number);

                    command.Prepare();

                    using (var reader = command.ExecuteReader())
                    {
                        keys.Fill(reader);
                    }
                }

                using (var command =
                    new NpgsqlCommand(
                        string.Join(";", "DROP TABLE IF EXISTS temp_placex_way",
                            "CREATE TEMP TABLE temp_placex_way (osm_id BIGINT,tags hstore,location GEOMETRY)"),
                        connection2))
                {
                    command.Prepare();

                    command.ExecuteNonQuery();
                }

                using (var writer = connection2.BeginTextImport(
                    "COPY temp_placex_way (osm_id,tags,location) FROM STDIN WITH NULL AS ''"))
                using (var command = new NpgsqlCommand(string.Join(";",
                        "SELECT COUNT(*) FROM way WHERE tags?|@keys AND record_number>@last_record_number",
                        "SELECT id,cast(tags as text),nodes,tags?|ARRAY['area','building'] FROM way WHERE tags?|@keys AND record_number>@last_record_number")
                    , connection))
                {
                    command.Parameters.AddWithValue("keys", keys.ToArray());
                    command.Parameters.AddWithValue("last_record_number", last_record_number);

                    command.Prepare();

                    using (var reader = command.ExecuteReader())
                    {
                        if (reader.Read())
                            total = reader.GetInt64(0);

                        reader.NextResult();

                        var obj = new object();
                        var reader_is_empty = false;

                        Parallel.For(0, 12,
                            i =>
                            {
                                using (var connection3 = new NpgsqlConnection(GetOsmConnectionString()))
                                {
                                    connection3.Open();

                                    using (var command3 = new NpgsqlCommand(
                                        "SELECT id,longitude,latitude FROM node WHERE id=ANY(@ids)"
                                        , connection3))
                                    {
                                        command3.Parameters.Add("ids", NpgsqlDbType.Array | NpgsqlDbType.Bigint);

                                        command3.Prepare();

                                        while (true)
                                        {
                                            long id0;
                                            string tags;
                                            long[] nodes;
                                            bool area;

                                            lock (obj)
                                            {
                                                if (reader_is_empty) break;
                                                reader_is_empty = !reader.Read();
                                                if (reader_is_empty) break;
                                                id0 = reader.GetInt64(0);
                                                tags = reader.GetString(1);
                                                nodes = (long[]) reader.GetValue(2);
                                                area = reader.GetBoolean(3);
                                            }

                                            if (nodes.Any())
                                            {
                                                var dic = new Dictionary<long, Point>(nodes.Length);

                                                command3.Parameters["ids"].Value = nodes;

                                                using (var reader3 = command3.ExecuteReader())
                                                {
                                                    while (reader3.Read())
                                                        dic.Add(reader3.GetInt64(0), new Point
                                                        {
                                                            longitude = reader3.GetDouble(1),
                                                            latitude = reader3.GetDouble(2)
                                                        });
                                                }

                                                var cleanNodes = nodes.Where(id => dic.ContainsKey(id)).ToArray();

                                                if (cleanNodes.Any())
                                                {
                                                    var sb = new StringBuilder("SRID=4326;");
                                                    sb.Append(
                                                        cleanNodes.Length > 1
                                                            ? area && cleanNodes.Length > 2 ? "POLYGON" : "LINESTRING"
                                                            : "POINT");
                                                    sb.Append(cleanNodes.Length > 1
                                                        ? area && cleanNodes.Length > 2 ? "((" : "("
                                                        : "(");
                                                    sb.Append(string.Join(",",
                                                        nodes.Where(id => dic.ContainsKey(id)).Select(id =>
                                                            $"{dic[id].longitude.ToString(_nfi)} {dic[id].latitude.ToString(_nfi)}")));

                                                    if (area && cleanNodes.Length > 2 &&
                                                        (cleanNodes.Length < 4 ||
                                                         cleanNodes.First() != cleanNodes.Last()))
                                                    {
                                                        var id = cleanNodes.First();
                                                        sb.Append(
                                                            $",{dic[id].longitude.ToString(_nfi)} {dic[id].latitude.ToString(_nfi)}");
                                                    }

                                                    sb.Append(cleanNodes.Length > 1
                                                        ? area && cleanNodes.Length > 2 ? "))" : ")"
                                                        : ")");

                                                    var values = new List<string>
                                                    {
                                                        id0.ToString(),
                                                        tags.TextEscape(),
                                                        sb.ToString()
                                                    };

                                                    lock (obj)
                                                    {
                                                        writer.WriteLine(string.Join("\t", values));
                                                    }
                                                }
                                            }

                                            lock (obj)
                                            {
                                                current++;

                                                if (current % 1000 == 0)
                                                    _progressHub.ProgressAsync(100f * current / total, id1, session)
                                                        .GetAwaiter()
                                                        .GetResult();
                                            }
                                        }
                                    }

                                    connection3.Close();
                                }
                            });
                    }

                    await _progressHub.ProgressAsync(100f, id1, session);
                }

                using (var command = new NpgsqlCommand(
                    string.Join(";",
                        @"INSERT INTO placex(osm_id,osm_type,tags,location)
                        SELECT osm_id,'way',tags,location FROM temp_placex_way
                        ON CONFLICT (osm_id,osm_type) DO UPDATE SET location=EXCLUDED.location,tags=EXCLUDED.tags,record_number=EXCLUDED.record_number",
                        "DROP TABLE temp_placex_way"), connection2))
                {
                    command.Prepare();

                    command.ExecuteNonQuery();
                }

                SetLastRecordNumber(connection, OsmServiceType.Way, next_last_record_number);

                await connection2.CloseAsync();
                await connection.CloseAsync();
            }
        }

        public async Task UpdateFromRelationAsync(string session)
        {
            var current = 0L;
            var total = 0L;

            var id1 = Guid.NewGuid().ToString();
            await _progressHub.InitAsync(id1, session);

            using (var connection = new NpgsqlConnection(GetOsmConnectionString()))
            using (var connection2 = new NpgsqlConnection(GetOsmConnectionString()))
            {
                await connection.OpenAsync();
                await connection2.OpenAsync();

                connection.ReloadTypes();
                connection.TypeMapper.MapComposite<OsmRelationMember>("relation_member");
                connection.TypeMapper.MapEnum<OsmType>("osm_type");
                connection.TypeMapper.MapEnum<OsmServiceType>("service_type");

                var last_record_number = GetLastRecordNumber(connection, OsmServiceType.Relation);
                var next_last_record_number = GetNextLastRecordNumber(connection);

                var keys = new List<string> {"name"};

                using (var command = new NpgsqlCommand(
                    "SELECT key FROM (SELECT DISTINCT unnest(akeys(tags)) AS key FROM relation WHERE record_number>@last_record_number) AS keys WHERE key LIKE 'addr%'"
                    , connection))
                {
                    command.Parameters.AddWithValue("last_record_number", last_record_number);

                    command.Prepare();

                    using (var reader = command.ExecuteReader())
                    {
                        keys.Fill(reader);
                    }
                }

                using (var command =
                    new NpgsqlCommand(
                        string.Join(";", "DROP TABLE IF EXISTS temp_placex_relation",
                            "CREATE TEMP TABLE temp_placex_relation (osm_id BIGINT,tags hstore,location GEOMETRY)"),
                        connection2))
                {
                    command.Prepare();

                    command.ExecuteNonQuery();
                }

                using (var writer = connection2.BeginTextImport(
                    "COPY temp_placex_relation (osm_id,tags,location) FROM STDIN WITH NULL AS ''"))
                using (var command = new NpgsqlCommand(string.Join(";",
                        "SELECT COUNT(*) FROM relation WHERE tags?|@keys AND tags->'type'='multipolygon' AND record_number>@last_record_number"
                        , "SELECT id,cast(tags as text),members FROM relation WHERE tags?|@keys AND tags->'type'='multipolygon' AND record_number>@last_record_number")
                    , connection))
                {
                    command.Parameters.AddWithValue("keys", keys.ToArray());
                    command.Parameters.AddWithValue("last_record_number", last_record_number);

                    command.Prepare();

                    using (var reader = command.ExecuteReader())
                    {
                        if (reader.Read())
                            total = reader.GetInt64(0);

                        reader.NextResult();
                        var obj = new object();
                        var reader_is_empty = false;

                        Parallel.For(0, 12,
                            i =>
                            {
                                using (var connection3 = new NpgsqlConnection(GetOsmConnectionString()))
                                using (var connection4 = new NpgsqlConnection(GetOsmConnectionString()))
                                {
                                    connection3.Open();
                                    connection4.Open();

                                    using (var command3 = new NpgsqlCommand(
                                        "SELECT id,nodes FROM way WHERE id=ANY(@ids)"
                                        , connection3))
                                    using (var command4 = new NpgsqlCommand(
                                        "SELECT id,longitude,latitude FROM node WHERE id=ANY(@ids)"
                                        , connection4))
                                    {
                                        command3.Parameters.Add("ids", NpgsqlDbType.Array | NpgsqlDbType.Bigint);

                                        command3.Prepare();

                                        command4.Parameters.Add("ids", NpgsqlDbType.Array | NpgsqlDbType.Bigint);

                                        command4.Prepare();

                                        while (true)
                                        {
                                            long id0;
                                            string tags;
                                            OsmRelationMember[] members;

                                            lock (obj)
                                            {
                                                if (reader_is_empty) break;
                                                reader_is_empty = !reader.Read();
                                                if (reader_is_empty) break;
                                                id0 = reader.GetInt64(0);
                                                tags = reader.GetString(1);
                                                members = (OsmRelationMember[]) reader.GetValue(2);
                                            }

                                            if (members.Any())
                                            {
                                                var cleanMember = members.Where(x => x.Type == 1 && x.Role == "outer")
                                                    .ToArray();
                                                if (cleanMember.Any())
                                                {
                                                    var outers = new List<long[]>();
                                                    var outerRings = new List<long[]>();

                                                    command3.Parameters["ids"].Value =
                                                        cleanMember.Select(x => x.Id).ToArray();

                                                    using (var reader3 = command3.ExecuteReader())
                                                    {
                                                        while (reader3.Read())
                                                        {
                                                            var id = reader3.GetInt64(0);
                                                            var nodes = (long[]) reader3.GetValue(1);
                                                            if (!nodes.Any()) continue;
                                                            outers.Add(nodes);
                                                        }
                                                    }

                                                    while (ConnectToRings(outers, outerRings))
                                                    {
                                                    }

                                                    var any = false;
                                                    var sb = new StringBuilder("SRID=4326;");
                                                    sb.Append("MULTIPOLYGON");
                                                    sb.Append("(");
                                                    var first = true;
                                                    foreach (var nodes in outerRings)
                                                    {
                                                        if (nodes.Length < 4) continue;

                                                        var dic = new Dictionary<long, Point>(nodes.Length);

                                                        command4.Parameters["ids"].Value = nodes;

                                                        using (var reader4 = command4.ExecuteReader())
                                                        {
                                                            while (reader4.Read())
                                                                dic.Add(reader4.GetInt64(0), new Point
                                                                {
                                                                    longitude = reader4.GetDouble(1),
                                                                    latitude = reader4.GetDouble(2)
                                                                });
                                                        }

                                                        var cleanNodes = nodes.Where(id => dic.ContainsKey(id))
                                                            .ToArray();
                                                        if (cleanNodes.Length < 4) continue;
                                                        if (first) first = false;
                                                        else sb.Append(",");
                                                        sb.Append("((");
                                                        sb.Append(string.Join(",",
                                                            cleanNodes.Select(id =>
                                                                $"{dic[id].longitude.ToString(_nfi)} {dic[id].latitude.ToString(_nfi)}")));
                                                        if (cleanNodes.First() != cleanNodes.Last())
                                                        {
                                                            var id = cleanNodes.First();
                                                            sb.Append(
                                                                $",{dic[id].longitude.ToString(_nfi)} {dic[id].latitude.ToString(_nfi)}");
                                                        }

                                                        sb.Append("))");
                                                        any = true;
                                                    }

                                                    sb.Append(")");

                                                    if (any)
                                                    {
                                                        var values = new List<string>
                                                        {
                                                            id0.ToString(),
                                                            tags.TextEscape(),
                                                            sb.ToString()
                                                        };

                                                        lock (obj)
                                                        {
                                                            writer.WriteLine(string.Join("\t", values));
                                                        }
                                                    }
                                                }
                                            }

                                            lock (obj)
                                            {
                                                current++;

                                                if (current % 1000 == 0)
                                                    _progressHub.ProgressAsync(100f * current / total, id1, session)
                                                        .GetAwaiter()
                                                        .GetResult();
                                            }
                                        }
                                    }

                                    connection4.Close();
                                    connection3.Close();
                                }
                            });
                    }

                    await _progressHub.ProgressAsync(100f, id1, session);
                }


                using (var command = new NpgsqlCommand(
                    string.Join(";",
                        @"INSERT INTO placex(osm_id,osm_type,tags,location)
                        SELECT osm_id,'relation',tags,location FROM temp_placex_relation
                        ON CONFLICT (osm_id,osm_type) DO UPDATE SET location=EXCLUDED.location,tags=EXCLUDED.tags,record_number=EXCLUDED.record_number",
                        "DROP TABLE temp_placex_relation"), connection2))
                {
                    command.Prepare();

                    command.ExecuteNonQuery();
                }

                SetLastRecordNumber(connection, OsmServiceType.Relation, next_last_record_number);

                await connection2.CloseAsync();
                await connection.CloseAsync();
            }
        }

        private bool ConnectToRings(List<long[]> list, List<long[]> rings)
        {
            for (var index1 = 0; index1 < list.Count; index1++)
            {
                if (list[index1].Last() == list[index1].First())
                {
                    rings.Add(list[index1]);
                    list.RemoveAt(index1);
                    return true;
                }

                for (var index2 = index1 + 1; index2 < list.Count; index2++)
                {
                    if (list[index1].Last() == list[index2].First())
                    {
                        list[index1] = list[index1].Concat(list[index2]).ToArray();
                        list.RemoveAt(index2);
                        return true;
                    }

                    if (list[index1].Last() == list[index2].Last())
                    {
                        list[index1] = list[index1].Concat(list[index2].Reverse()).ToArray();
                        list.RemoveAt(index2);
                        return true;
                    }

                    if (list[index1].First() == list[index2].First())
                    {
                        list[index1] = list[index1].Reverse().Concat(list[index2]).ToArray();
                        list.RemoveAt(index2);
                        return true;
                    }

                    if (list[index1].First() == list[index2].Last())
                    {
                        list[index2] = list[index2].Concat(list[index1]).ToArray();
                        list.RemoveAt(index1);
                        return true;
                    }
                }
            }

            return false;
        }

        private long GetLastRecordNumber(NpgsqlConnection connection, OsmServiceType service_type)
        {
            using (var command = new NpgsqlCommand(
                "SELECT last_record_number FROM service_history WHERE service_type=@service_type LIMIT 1"
                , connection))
            {
                command.Parameters.AddWithValue("service_type", service_type);

                command.Prepare();

                using (var reader = command.ExecuteReader())
                {
                    if (reader.Read())
                        return reader.GetInt64(0);
                }
            }

            return 0;
        }

        public void SetLastRecordNumber(NpgsqlConnection connection, OsmServiceType service_type,
            long last_record_number)
        {
            using (var command = new NpgsqlCommand(
                "INSERT INTO service_history(service_type,last_record_number) VALUES (@service_type, @last_record_number) ON CONFLICT (service_type) DO UPDATE SET last_record_number=EXCLUDED.last_record_number"
                , connection))
            {
                command.Parameters.AddWithValue("service_type", service_type);
                command.Parameters.AddWithValue("last_record_number", last_record_number);

                command.Prepare();

                command.ExecuteNonQuery();
            }
        }

        public class Point
        {
            public double latitude { get; set; }
            public double longitude { get; set; }
        }
    }
}