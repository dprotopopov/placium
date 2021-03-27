using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using Placium.Common;
using Placium.Types;

namespace Updater.Place
{
    public class PlaceUpdateService : IUpdateService
    {
        private readonly NumberFormatInfo _nfi = new NumberFormatInfo {NumberDecimalSeparator = "."};
        private readonly ProgressHub _progressHub;

        public PlaceUpdateService(ProgressHub progressHub)
        {
            _progressHub = progressHub;
        }

        public async Task UpdateAsync(string connectionString, string session)
        {
            await UpdateFromNodeAsync(connectionString, session);
            await UpdateFromWayAsync(connectionString, session);
            await UpdateFromRelationAsync(connectionString, session);
        }

        public async Task UpdateFromNodeAsync(string connectionString, string session)
        {
            long count = 0;
            using (var connection = new NpgsqlConnection(connectionString))
            using (var connection2 = new NpgsqlConnection(connectionString))
            {
                await connection.OpenAsync();
                await connection2.OpenAsync();

                connection.TypeMapper.MapComposite<OsmRelationMember>("relation_member");
                connection.TypeMapper.MapEnum<OsmType>("osm_type");
                connection.TypeMapper.MapEnum<OsmServiceType>("service_type");

                var last_record_number = GetLastRecordNumber(connection, OsmServiceType.Node);
                var next_last_record_number = NextLastRecordNumber(connection);

                using (var command = new NpgsqlCommand(
                    "DROP TABLE IF EXISTS temp_place_node"
                    , connection))
                {
                    command.ExecuteNonQuery();
                }

                using (var command = new NpgsqlCommand(
                    "CREATE TABLE temp_place_node (osm_id BIGINT,tags hstore,location GEOGRAPHY)"
                    , connection))
                {
                    command.ExecuteNonQuery();
                }

                using (var writer = connection2.BeginTextImport(
                    "COPY temp_place_node (osm_id,tags,location) FROM STDIN WITH NULL AS '';"))
                using (var command = new NpgsqlCommand(
                    "SELECT id,cast(tags as text),longitude,latitude FROM node WHERE tags?'name' AND record_number>@last_record_number AND record_number<=@next_last_record_number"
                    , connection))
                {
                    command.Parameters.AddWithValue("last_record_number", last_record_number);
                    command.Parameters.AddWithValue("next_last_record_number", next_last_record_number);
                    using (var reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            var values = new List<string>
                            {
                                reader.GetInt64(0).ToString(),
                                reader.GetString(1).TextEscape(),
                                $"POINT({reader.GetDouble(2).ToString(_nfi)} {reader.GetDouble(3).ToString(_nfi)})"
                            };

                            writer.WriteLine(string.Join("\t", values));
                        }
                    }
                }

                using (var command = new NpgsqlCommand(
                    "INSERT INTO place(osm_id,osm_type,tags,location) SELECT osm_id,'node',tags,location FROM temp_place_node ON CONFLICT (osm_id,osm_type) DO UPDATE SET location=EXCLUDED.location,tags=EXCLUDED.tags,record_number=EXCLUDED.record_number"
                    , connection))
                {
                    command.ExecuteNonQuery();
                }

                using (var command = new NpgsqlCommand(
                    "DROP TABLE temp_place_node"
                    , connection))
                {
                    command.ExecuteNonQuery();
                }

                SetLastRecordNumber(connection, OsmServiceType.Node, next_last_record_number);

                await connection2.CloseAsync();
                await connection.CloseAsync();
            }
        }

        public async Task UpdateFromWayAsync(string connectionString, string session)
        {
            long count = 0;
            using (var connection = new NpgsqlConnection(connectionString))
            using (var connection2 = new NpgsqlConnection(connectionString))
            using (var connection3 = new NpgsqlConnection(connectionString))
            {
                await connection.OpenAsync();
                await connection2.OpenAsync();
                await connection3.OpenAsync();

                connection.TypeMapper.MapComposite<RelationMember>("relation_member");
                connection.TypeMapper.MapEnum<OsmType>("osm_type");
                connection.TypeMapper.MapEnum<OsmServiceType>("service_type");

                var last_record_number = GetLastRecordNumber(connection, OsmServiceType.Way);
                var next_last_record_number = NextLastRecordNumber(connection);

                using (var command = new NpgsqlCommand(
                    "DROP TABLE IF EXISTS temp_place_way"
                    , connection))
                {
                    command.ExecuteNonQuery();
                }

                using (var command = new NpgsqlCommand(
                    "CREATE TABLE temp_place_way (osm_id BIGINT,tags hstore,location GEOGRAPHY)"
                    , connection))
                {
                    command.ExecuteNonQuery();
                }

                using (var writer = connection2.BeginTextImport(
                    "COPY temp_place_way (osm_id,tags,location) FROM STDIN WITH NULL AS '';"))
                using (var command = new NpgsqlCommand(
                    "SELECT id,cast(tags as text),nodes,tags?'area' FROM way WHERE tags?'name' AND record_number>@last_record_number AND record_number<=@next_last_record_number"
                    , connection))
                {
                    command.Parameters.AddWithValue("last_record_number", last_record_number);
                    command.Parameters.AddWithValue("next_last_record_number", next_last_record_number);
                    using (var reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            var nodes = (long[]) reader.GetValue(2);
                            if (nodes.Length == 0) continue;
                            var area = reader.GetBoolean(3);

                            var dic = new Dictionary<long, Point>(nodes.Length);
                            using (var command3 = new NpgsqlCommand(
                                "SELECT id,longitude,latitude FROM node WHERE id=ANY(@nodes)"
                                , connection3))
                            {
                                command3.Parameters.AddWithValue("nodes", nodes);
                                using (var reader3 = command3.ExecuteReader())
                                {
                                    while (reader3.Read())
                                        dic.Add(reader3.GetInt64(0), new Point
                                        {
                                            longitude = reader3.GetDouble(1),
                                            latitude = reader3.GetDouble(2)
                                        });
                                }
                            }

                            var cleanNodes = nodes.Where(id => dic.ContainsKey(id)).ToArray();
                            if (cleanNodes.Length == 0) continue;

                            var sb = new StringBuilder(
                                cleanNodes.Length > 1 ? area ? "POLYGON" : "LINESTRING" : "POINT");
                            sb.Append(cleanNodes.Length > 1 ? area ? "((" : "(" : "(");
                            sb.Append(string.Join(",",
                                nodes.Where(id => dic.ContainsKey(id)).Select(id =>
                                    $"{dic[id].longitude.ToString(_nfi)} {dic[id].latitude.ToString(_nfi)}")));

                            if (cleanNodes.Length > 1 && area)
                            {
                                var id = cleanNodes.First();
                                sb.Append($",{dic[id].longitude.ToString(_nfi)} {dic[id].latitude.ToString(_nfi)}");
                            }

                            sb.Append(cleanNodes.Length > 1 ? area ? "))" : ")" : ")");

                            var values = new List<string>
                            {
                                reader.GetInt64(0).ToString(),
                                reader.GetString(1).TextEscape(),
                                sb.ToString()
                            };

                            writer.WriteLine(string.Join("\t", values));

                            if (count++ % 1000 == 0)
                                await _progressHub.Progress(100f * count / (count + 1000000), session);
                        }
                    }
                }

                using (var command = new NpgsqlCommand(
                    "INSERT INTO place(osm_id,osm_type,tags,location) SELECT osm_id,'way',tags,location FROM temp_place_way ON CONFLICT (osm_id,osm_type) DO UPDATE SET location=EXCLUDED.location,tags=EXCLUDED.tags,record_number=EXCLUDED.record_number"
                    , connection))
                {
                    command.ExecuteNonQuery();
                }

                using (var command = new NpgsqlCommand(
                    "DROP TABLE temp_place_way"
                    , connection))
                {
                    command.ExecuteNonQuery();
                }

                SetLastRecordNumber(connection, OsmServiceType.Way, next_last_record_number);

                await connection3.CloseAsync();
                await connection2.CloseAsync();
                await connection.CloseAsync();
            }
        }

        public async Task UpdateFromRelationAsync(string connectionString, string session)
        {
            long count = 0;
            using (var connection = new NpgsqlConnection(connectionString))
            using (var connection2 = new NpgsqlConnection(connectionString))
            using (var connection3 = new NpgsqlConnection(connectionString))
            {
                await connection.OpenAsync();
                await connection2.OpenAsync();
                await connection3.OpenAsync();

                connection.TypeMapper.MapComposite<RelationMember>("relation_member");
                connection.TypeMapper.MapEnum<OsmType>("osm_type");
                connection.TypeMapper.MapEnum<OsmServiceType>("service_type");

                var last_record_number = GetLastRecordNumber(connection, OsmServiceType.Relation);
                var next_last_record_number = NextLastRecordNumber(connection);

                using (var command = new NpgsqlCommand(
                    "DROP TABLE IF EXISTS temp_place_relation"
                    , connection))
                {
                    command.ExecuteNonQuery();
                }

                using (var command = new NpgsqlCommand(
                    "CREATE TABLE temp_place_relation (osm_id BIGINT,tags hstore,location GEOGRAPHY)"
                    , connection))
                {
                    command.ExecuteNonQuery();
                }

                using (var writer = connection2.BeginTextImport(
                    "COPY temp_place_relation (osm_id,tags,location) FROM STDIN WITH NULL AS '';"))
                using (var command = new NpgsqlCommand(
                    "SELECT id,cast(tags as text),members FROM relation WHERE tags?'name' AND tags->'type'='multipolygon' AND record_number>@last_record_number AND record_number<=@next_last_record_number"
                    , connection))
                {
                    command.Parameters.AddWithValue("last_record_number", last_record_number);
                    command.Parameters.AddWithValue("next_last_record_number", next_last_record_number);
                    using (var reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            var members = (RelationMember[]) reader.GetValue(2);
                            if (members.Length == 0) continue;
                            var cleanMember = members.Where(x => x.Type == 1 && x.Role == "outer")
                                .ToArray();
                            if (cleanMember.Length == 0) continue;
                            var outers = new List<long[]>();
                            var outerRings = new List<long[]>();
                            using (var command3 = new NpgsqlCommand(
                                "SELECT id,nodes FROM way WHERE id=ANY(@ids)"
                                , connection3))
                            {
                                command3.Parameters.AddWithValue("ids", cleanMember.Select(x => x.Id).ToArray());
                                using (var reader3 = command3.ExecuteReader())
                                {
                                    while (reader3.Read())
                                    {
                                        var id = reader3.GetInt64(0);
                                        var nodes = (long[]) reader3.GetValue(1);
                                        if (nodes.Length == 0) continue;
                                        outers.Add(nodes);
                                    }
                                }
                            }

                            while (ConnectToRings(outers, outerRings))
                            {
                            }

                            var any = false;
                            var sb = new StringBuilder("MULTIPOLYGON");
                            sb.Append("(");
                            var first = true;
                            foreach (var nodes in outerRings)
                            {
                                if (nodes.Length < 4) continue;

                                var dic = new Dictionary<long, Point>(nodes.Length);
                                using (var command3 = new NpgsqlCommand(
                                    "SELECT id,longitude,latitude FROM node WHERE id=ANY(@nodes)"
                                    , connection3))
                                {
                                    command3.Parameters.AddWithValue("nodes", nodes);
                                    using (var reader3 = command3.ExecuteReader())
                                    {
                                        while (reader3.Read())
                                            dic.Add(reader3.GetInt64(0), new Point
                                            {
                                                longitude = reader3.GetDouble(1),
                                                latitude = reader3.GetDouble(2)
                                            });
                                    }
                                }

                                var cleanNodes = nodes.Where(id => dic.ContainsKey(id)).ToArray();
                                if (cleanNodes.Length < 4) continue;
                                if (first) first = false;
                                else sb.Append(",");
                                sb.Append("((");
                                sb.Append(string.Join(",",
                                    nodes.Where(id => dic.ContainsKey(id)).Select(id =>
                                        $"{dic[id].longitude.ToString(_nfi)} {dic[id].latitude.ToString(_nfi)}")));

                                sb.Append("))");
                                any = true;
                            }

                            sb.Append(")");

                            if(!any) continue;

                            var values = new List<string>
                            {
                                reader.GetInt64(0).ToString(),
                                reader.GetString(1).TextEscape(),
                                sb.ToString()
                            };

                            writer.WriteLine(string.Join("\t", values));

                            if (count++ % 1000 == 0)
                                await _progressHub.Progress(100f * count / (count + 1000000), session);
                        }
                    }
                }

                using (var command = new NpgsqlCommand(
                    "INSERT INTO place(osm_id,osm_type,tags,location) SELECT osm_id,'relation',tags,location FROM temp_place_relation ON CONFLICT (osm_id,osm_type) DO UPDATE SET location=EXCLUDED.location,tags=EXCLUDED.tags,record_number=EXCLUDED.record_number"
                    , connection))
                {
                    command.ExecuteNonQuery();
                }

                using (var command = new NpgsqlCommand(
                    "DROP TABLE temp_place_relation"
                    , connection))
                {
                    command.ExecuteNonQuery();
                }

                SetLastRecordNumber(connection, OsmServiceType.Relation, next_last_record_number);

                await connection3.CloseAsync();
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
                using (var reader = command.ExecuteReader())
                {
                    if (reader.Read())
                        return reader.GetInt64(0);
                }
            }

            return 0;
        }

        public void SetLastRecordNumber(NpgsqlConnection connection, OsmServiceType service_type, long last_record_number)
        {
            using (var command = new NpgsqlCommand(
                "INSERT INTO service_history(service_type,last_record_number) VALUES (@service_type, @last_record_number) ON CONFLICT (service_type) DO UPDATE SET last_record_number=EXCLUDED.last_record_number"
                , connection))
            {
                command.Parameters.AddWithValue("service_type", service_type);
                command.Parameters.AddWithValue("last_record_number", last_record_number);
                command.ExecuteNonQuery();
            }
        }

        private long NextLastRecordNumber(NpgsqlConnection connection)
        {
            using (var command = new NpgsqlCommand(
                $"SELECT last_value FROM record_number_seq"
                , connection))
            {
                using (var reader = command.ExecuteReader())
                {
                    if (reader.Read())
                        return reader.GetInt64(0);
                }
            }

            return 0;
        }

        public class RelationMember
        {
            [PgName("type")] public int Type { get; set; }

            [PgName("id")] public long Id { get; set; }

            [PgName("role")] public string Role { get; set; }
        }

        public class Point
        {
            public double latitude { get; set; }
            public double longitude { get; set; }
        }
    }
}