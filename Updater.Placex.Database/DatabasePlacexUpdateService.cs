using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using NetTopologySuite.Geometries;
using NetTopologySuite.IO;
using Npgsql;
using NpgsqlTypes;
using Placium.Common;
using Placium.Types;

namespace Updater.Placex.Database;

public class DatabasePlacexUpdateService : BaseAppService, IUpdateService
{
    private readonly GeometryFactory _geometryFactory = new();
    private readonly NumberFormatInfo _nfi = new() { NumberDecimalSeparator = "." };
    private readonly IParallelConfig _parallelConfig;
    private readonly IProgressClient _progressClient;
    private readonly WKTReader _wktReader = new();
    private readonly WKTWriter _wktWriter = new();

    public DatabasePlacexUpdateService(IProgressClient progressClient,
        IConnectionsConfig configuration, IParallelConfig parallelConfig) : base(configuration)
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

            SetLastRecordNumber(npgsqlConnection, OsmServiceType.Node, 0);
            SetLastRecordNumber(npgsqlConnection, OsmServiceType.Way, 0);
            SetLastRecordNumber(npgsqlConnection, OsmServiceType.Relation, 0);

            await npgsqlConnection.CloseAsync();
        }

        await UpdateNodeAsync(session, full);
        await UpdateWayAsync(session, full);
        await UpdateRelationAsync(session, full);
    }

    public async Task UpdateNodeAsync(string session, bool full)
    {
        var current = 0L;
        var total = 0L;

        var id = Guid.NewGuid().ToString();
        await _progressClient.Init(id, session);

        await using var connection = new NpgsqlConnection(GetOsmConnectionString());
        await using var connection2 = new NpgsqlConnection(GetOsmConnectionString());

        await connection.OpenAsync();
        await connection2.OpenAsync();

        connection.ReloadTypes();
        connection.TypeMapper.MapComposite<OsmRelationMember>("relation_member");
        connection.TypeMapper.MapEnum<OsmType>("osm_type");
        connection.TypeMapper.MapEnum<OsmServiceType>("service_type");

        var last_record_number = GetLastRecordNumber(connection, OsmServiceType.Node, full);
        var next_last_record_number = GetNextLastRecordNumber(connection);

        var keys = new List<string> { "name" };

        await using (var command = new NpgsqlCommand(
                         "SELECT key FROM (SELECT DISTINCT unnest(akeys(tags)) AS key FROM node WHERE record_number>@last_record_number) AS keys WHERE key LIKE 'addr%'"
                         , connection))
        {
            command.Parameters.AddWithValue("last_record_number", last_record_number);

            await command.PrepareAsync();

            await using var reader = command.ExecuteReader();
            keys.Fill(reader);
        }

        await using (var command =
                     new NpgsqlCommand(
                         string.Join(";", "DROP TABLE IF EXISTS temp_placex_node",
                             "CREATE TEMP TABLE temp_placex_node (osm_id BIGINT,tags hstore,location GEOMETRY)"),
                         connection2))
        {
            await command.PrepareAsync();

            command.ExecuteNonQuery();
        }

        await using (var writer = await connection2.BeginTextImportAsync(
                         "COPY temp_placex_node (osm_id,tags,location) FROM STDIN WITH NULL AS ''"))
        await using (var command = new NpgsqlCommand(string.Join(";",
                             "SELECT COUNT(*) FROM node WHERE tags?|@keys AND record_number>@last_record_number",
                             "SELECT id,cast(tags as text),longitude,latitude FROM node WHERE tags?|@keys AND record_number>@last_record_number"),
                         connection))
        {
            command.Parameters.AddWithValue("keys", keys.ToArray());
            command.Parameters.AddWithValue("last_record_number", last_record_number);

            await command.PrepareAsync();

            await using (var reader = command.ExecuteReader())
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
                        $"SRID=4326;POINT({reader.GetFloat(2).ToString(_nfi)} {reader.GetFloat(3).ToString(_nfi)})"
                    };

                    writer.WriteLine(string.Join("\t", values));

                    if (current % 1000 == 0)
                        await _progressClient.Progress(100f * current / total, id, session);
                }
            }

            await _progressClient.Finalize(id, session);
        }

        await using (var command = new NpgsqlCommand(
                         string.Join(";",
                             @"INSERT INTO placex(osm_id,osm_type,tags,location)
                        SELECT osm_id,'node',tags,ST_MakeValid(location) FROM temp_placex_node
                        ON CONFLICT (osm_id,osm_type) DO UPDATE SET location=EXCLUDED.location,tags=EXCLUDED.tags,record_number=EXCLUDED.record_number",
                             "DROP TABLE temp_placex_node"), connection2))
        {
            await command.PrepareAsync();

            command.ExecuteNonQuery();
        }

        SetLastRecordNumber(connection, OsmServiceType.Node, next_last_record_number);

        await connection2.CloseAsync();
        await connection.CloseAsync();
    }

    public async Task UpdateWayAsync(string session, bool full)
    {
        var current = 0L;
        var total = 0L;

        var id1 = Guid.NewGuid().ToString();
        await _progressClient.Init(id1, session);

        await using var connection = new NpgsqlConnection(GetOsmConnectionString());
        await using var connection2 = new NpgsqlConnection(GetOsmConnectionString());

        await connection.OpenAsync();
        await connection2.OpenAsync();

        connection.ReloadTypes();
        connection.TypeMapper.MapComposite<OsmRelationMember>("relation_member");
        connection.TypeMapper.MapEnum<OsmType>("osm_type");
        connection.TypeMapper.MapEnum<OsmServiceType>("service_type");

        var last_record_number = GetLastRecordNumber(connection, OsmServiceType.Way, full);
        var next_last_record_number = GetNextLastRecordNumber(connection);

        var keys = new List<string> { "name" };

        await using (var command = new NpgsqlCommand(
                         "SELECT key FROM (SELECT DISTINCT unnest(akeys(tags)) AS key FROM way WHERE record_number>@last_record_number) AS keys WHERE key LIKE 'addr%'"
                         , connection))
        {
            command.Parameters.AddWithValue("last_record_number", last_record_number);

            await command.PrepareAsync();

            await using var reader = command.ExecuteReader();
            keys.Fill(reader);
        }

        await using (var command =
                     new NpgsqlCommand(
                         string.Join(";", "DROP TABLE IF EXISTS temp_placex_way",
                             "CREATE TEMP TABLE temp_placex_way (osm_id BIGINT,tags hstore,location GEOMETRY)"),
                         connection2))
        {
            await command.PrepareAsync();

            command.ExecuteNonQuery();
        }

        await using (var writer = await connection2.BeginTextImportAsync(
                         "COPY temp_placex_way (osm_id,tags,location) FROM STDIN WITH NULL AS ''"))
        await using (var command = new NpgsqlCommand(string.Join(";",
                             "SELECT COUNT(*) FROM way WHERE tags?|@keys AND record_number>@last_record_number",
                             "SELECT id,cast(tags as text),nodes,tags?|ARRAY['area','building'] FROM way WHERE tags?|@keys AND record_number>@last_record_number")
                         , connection))
        {
            command.Parameters.AddWithValue("keys", keys.ToArray());
            command.Parameters.AddWithValue("last_record_number", last_record_number);

            await command.PrepareAsync();

            await using (var reader = command.ExecuteReader())
            {
                if (reader.Read())
                    total = reader.GetInt64(0);

                reader.NextResult();

                var obj = new object();
                var reader_is_empty = false;

                Parallel.For(0, _parallelConfig.GetNumberOfThreads(),
                    i =>
                    {
                        using var connection3 = new NpgsqlConnection(GetOsmConnectionString());
                        connection3.Open();

                        using (var command3 = new NpgsqlCommand(
                                   "SELECT id,longitude,latitude FROM node WHERE id=ANY(@ids)"
                                   , connection3))
                        {
                            command3.Parameters.Add("ids", NpgsqlDbType.Array | NpgsqlDbType.Bigint);

                            command3.Prepare();

                            for (;;)
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
                                    nodes = (long[])reader.GetValue(2);
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
                                                longitude = reader3.GetFloat(1),
                                                latitude = reader3.GetFloat(2)
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
                                        _progressClient.Progress(100f * current / total, id1,
                                                session)
                                            .GetAwaiter()
                                            .GetResult();
                                }
                            }
                        }

                        connection3.Close();
                    });
            }

            await _progressClient.Finalize(id1, session);
        }

        await using (var command = new NpgsqlCommand(
                         string.Join(";",
                             @"INSERT INTO placex(osm_id,osm_type,tags,location)
                        SELECT osm_id,'way',tags,ST_MakeValid(location) FROM temp_placex_way
                        ON CONFLICT (osm_id,osm_type) DO UPDATE SET location=EXCLUDED.location,tags=EXCLUDED.tags,record_number=EXCLUDED.record_number",
                             "DROP TABLE temp_placex_way"), connection2))
        {
            await command.PrepareAsync();

            command.ExecuteNonQuery();
        }

        SetLastRecordNumber(connection, OsmServiceType.Way, next_last_record_number);

        await connection2.CloseAsync();
        await connection.CloseAsync();
    }

    public async Task UpdateRelationAsync(string session, bool full)
    {
        var current = 0L;
        var total = 0L;

        var id1 = Guid.NewGuid().ToString();
        await _progressClient.Init(id1, session);

        await using var connection = new NpgsqlConnection(GetOsmConnectionString());
        await using var connection2 = new NpgsqlConnection(GetOsmConnectionString());

        await connection.OpenAsync();
        await connection2.OpenAsync();

        connection.ReloadTypes();
        connection.TypeMapper.MapComposite<OsmRelationMember>("relation_member");
        connection.TypeMapper.MapEnum<OsmType>("osm_type");
        connection.TypeMapper.MapEnum<OsmServiceType>("service_type");

        var last_record_number = GetLastRecordNumber(connection, OsmServiceType.Relation, full);
        var next_last_record_number = GetNextLastRecordNumber(connection);

        var keys = new List<string> { "name" };

        await using (var command = new NpgsqlCommand(
                         "SELECT key FROM (SELECT DISTINCT unnest(akeys(tags)) AS key FROM relation WHERE record_number>@last_record_number) AS keys WHERE key LIKE 'addr%'"
                         , connection))
        {
            command.Parameters.AddWithValue("last_record_number", last_record_number);

            await command.PrepareAsync();

            await using var reader = command.ExecuteReader();
            keys.Fill(reader);
        }

        await using (var command =
                     new NpgsqlCommand(
                         string.Join(";", "DROP TABLE IF EXISTS temp_placex_relation",
                             "CREATE TEMP TABLE temp_placex_relation (osm_id BIGINT,tags hstore,location GEOMETRY)"),
                         connection2))
        {
            await command.PrepareAsync();

            command.ExecuteNonQuery();
        }

        await using (var writer = await connection2.BeginTextImportAsync(
                         "COPY temp_placex_relation (osm_id,tags,location) FROM STDIN WITH NULL AS ''"))
        await using (var command = new NpgsqlCommand(string.Join(";",
                             "SELECT COUNT(*) FROM relation WHERE tags?|@keys AND record_number>@last_record_number"
                             , "SELECT id,cast(tags as text),members FROM relation WHERE tags?|@keys AND record_number>@last_record_number")
                         , connection))
        {
            command.Parameters.AddWithValue("keys", keys.ToArray());
            command.Parameters.AddWithValue("last_record_number", last_record_number);

            await command.PrepareAsync();

            await using (var reader = command.ExecuteReader())
            {
                if (reader.Read())
                    total = reader.GetInt64(0);

                reader.NextResult();
                var obj = new object();
                var reader_is_empty = false;

                Parallel.For(0, _parallelConfig.GetNumberOfThreads(),
                    i =>
                    {
                        using var connection3 = new NpgsqlConnection(GetOsmConnectionString());
                        using var connection4 = new NpgsqlConnection(GetOsmConnectionString());
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

                            for (;;)
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
                                    members = (OsmRelationMember[])reader.GetValue(2);
                                }

                                if (members.Any())
                                {
                                    var cleanMember = members.Where(x => x.Type == 1)
                                        .ToArray();
                                    if (cleanMember.Any())
                                    {
                                        var g = _geometryFactory.CreateEmpty(Dimension.Surface);

                                        var role = cleanMember.First().Role;
                                        var currentMembers = new List<OsmRelationMember>
                                            { cleanMember.First() };
                                        for (var index = 1; index <= cleanMember.Length; index++)
                                            if (index == cleanMember.Length ||
                                                cleanMember[index].Role != role)
                                            {
                                                if (ProcessMembers(currentMembers, command3,
                                                        command4, out var g1))
                                                {
                                                    switch (role)
                                                    {
                                                        case "outer":
                                                            g = g.Union(g1);
                                                            break;
                                                        case "inner":
                                                            g = g.Difference(g1);
                                                            break;
                                                    }

                                                    if (!g.IsValid) g = g.Buffer(0);
                                                }

                                                if (index == cleanMember.Length) break;

                                                role = cleanMember[index].Role;
                                                currentMembers = new List<OsmRelationMember>
                                                    { cleanMember[index] };
                                            }
                                            else
                                            {
                                                currentMembers.Add(cleanMember[index]);
                                            }


                                        var values = new List<string>
                                        {
                                            id0.ToString(),
                                            tags.TextEscape(),
                                            "SRID=4326;" + _wktWriter.Write(g)
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
                                        _progressClient.Progress(100f * current / total, id1,
                                                session)
                                            .GetAwaiter()
                                            .GetResult();
                                }
                            }
                        }

                        connection4.Close();
                        connection3.Close();
                    });
            }

            await _progressClient.Finalize(id1, session);
        }


        await using (var command = new NpgsqlCommand(
                         string.Join(";",
                             @"INSERT INTO placex(osm_id,osm_type,tags,location)
                        SELECT osm_id,'relation',tags,ST_MakeValid(location) FROM temp_placex_relation
                        ON CONFLICT (osm_id,osm_type) DO UPDATE SET location=EXCLUDED.location,tags=EXCLUDED.tags,record_number=EXCLUDED.record_number",
                             "DROP TABLE temp_placex_relation"), connection2))
        {
            await command.PrepareAsync();

            command.ExecuteNonQuery();
        }

        SetLastRecordNumber(connection, OsmServiceType.Relation, next_last_record_number);

        await connection2.CloseAsync();
        await connection.CloseAsync();
    }

    private bool ProcessMembers(List<OsmRelationMember> members, NpgsqlCommand command3,
        NpgsqlCommand command4, out Geometry g)
    {
        g = _geometryFactory.CreateEmpty(Dimension.Surface);

        var ids = new List<long>();
        var ways = new List<long[]>();
        var rings = new List<long[]>();

        command3.Parameters["ids"].Value =
            members.Select(x => x.Id).ToArray();

        using (var reader3 = command3.ExecuteReader())
        {
            while (reader3.Read())
            {
                var id = reader3.GetInt64(0);
                ids.Add(id);
                var nodes = (long[])reader3.GetValue(1);
                if (!nodes.Any()) continue;
                ways.Add(nodes);
            }
        }

        while (ConnectToRings(ways, rings))
        {
        }

        foreach (var way in ways)
            if (way.Length >= 2)
            {
                var list = new List<long>(way.Length + 1);
                list.AddRange(way);
                list.Add(way.First());
                rings.Add(list.ToArray());
            }

        var any = false;

        foreach (var ring in rings)
        {
            if (ring.Length < 4) continue;

            var dic = new Dictionary<long, Point>(ring.Length);

            command4.Parameters["ids"].Value = ring;

            using (var reader4 = command4.ExecuteReader())
            {
                while (reader4.Read())
                    dic.Add(reader4.GetInt64(0), new Point
                    {
                        longitude = reader4.GetFloat(1),
                        latitude = reader4.GetFloat(2)
                    });
            }

            var cleanNodes = ring.Where(id => dic.ContainsKey(id))
                .ToArray();
            if (cleanNodes.Length < 4) continue;

            var sb = new StringBuilder("SRID=4326;");
            sb.Append("POLYGON");

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

            var g1 = _wktReader.Read(sb.ToString());
            if (!g1.IsValid) g1 = g1.Buffer(0);

            g = g.Union(g1);

            any = true;
        }

        return any;
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


    public class Point
    {
        public float latitude { get; set; }
        public float longitude { get; set; }
    }
}