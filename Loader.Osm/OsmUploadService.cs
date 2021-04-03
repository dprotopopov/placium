using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Npgsql;
using OsmSharp;
using OsmSharp.Streams;
using Placium.Common;

namespace Loader.Osm
{
    public class OsmUploadService : BaseService, IUploadService
    {
        public enum ElementType
        {
            None,
            Node,
            Way,
            Relation
        }

        private readonly ProgressHub _progressHub;

        private readonly List<string> nodeKeys = new List<string>
        {
            "id",
            "version",
            "latitude",
            "longitude",
            "change_set_id",
            "time_stamp",
            "user_id",
            "user_name",
            "visible",
            "tags"
        };

        private readonly List<string> relationKeys = new List<string>
        {
            "id",
            "version",
            "change_set_id",
            "time_stamp",
            "user_id",
            "user_name",
            "visible",
            "tags",
            "members"
        };

        private readonly List<string> wayKeys = new List<string>
        {
            "id",
            "version",
            "change_set_id",
            "time_stamp",
            "user_id",
            "user_name",
            "visible",
            "tags",
            "nodes"
        };

        public OsmUploadService(ProgressHub progressHub, IConfiguration configuration) : base(configuration)
        {
            _progressHub = progressHub;
        }

        public async Task InstallAsync(Stream uploadStream, Dictionary<string, string> options, string session)
        {
            using (var connection = new NpgsqlConnection(GetOsmConnectionString()))
            {
                await connection.OpenAsync();

                DropTables(connection);

                await ExecuteResourceAsync(Assembly.GetExecutingAssembly(), "Loader.Osm.CreateTables.sql", connection);

                long count = 0;
                using (var source = new PBFOsmStreamSource(uploadStream))
                {
                    TextWriter writer = null;
                    var lastType = ElementType.None;

                    var id = Guid.NewGuid().ToString();
                    await _progressHub.InitAsync(id, session);

                    foreach (var element in source)
                    {
                        switch (element)
                        {
                            case Node node:
                                if (lastType != ElementType.Node)
                                {
                                    lastType = ElementType.Node;
                                    writer?.Dispose();
                                    writer = connection.BeginTextImport(
                                        $"COPY node ({string.Join(",", nodeKeys)}) FROM STDIN WITH NULL AS ''");
                                }

                                var nodeValues = new List<string>
                                {
                                    node.Id.ToString(),
                                    node.Version.ToString(),
                                    node.Latitude.ValueAsText(),
                                    node.Longitude.ValueAsText(),
                                    node.ChangeSetId.ToString(),
                                    node.TimeStamp.ValueAsText(),
                                    node.UserId.ToString(),
                                    node.UserName.ValueAsText(),
                                    node.Visible.ToString(),
                                    $"{string.Join(",", node.Tags.Select(t => $"\"{t.Key.TextEscape(2)}\"=>\"{t.Value.TextEscape(2)}\""))}"
                                };

                                writer.WriteLine(string.Join("\t", nodeValues));

                                break;
                            case Way way:
                                if (lastType != ElementType.Way)
                                {
                                    lastType = ElementType.Way;
                                    writer?.Dispose();
                                    writer = connection.BeginTextImport(
                                        $"COPY way ({string.Join(",", wayKeys)}) FROM STDIN WITH NULL AS ''");
                                }

                                var wayValues = new List<string>
                                {
                                    way.Id.ToString(),
                                    way.Version.ToString(),
                                    way.ChangeSetId.ToString(),
                                    way.TimeStamp.ValueAsText(),
                                    way.UserId.ToString(),
                                    way.UserName.ValueAsText(),
                                    way.Visible.ToString(),
                                    $"{string.Join(",", way.Tags.Select(t => $"\"{t.Key.TextEscape(2)}\"=>\"{t.Value.TextEscape(2)}\""))}",
                                    $"{{{string.Join(",", way.Nodes.Select(t => $"{t.ToString()}"))}}}"
                                };

                                writer.WriteLine(string.Join("\t", wayValues));

                                break;
                            case Relation relation:
                                if (lastType != ElementType.Relation)
                                {
                                    lastType = ElementType.Relation;
                                    writer?.Dispose();
                                    writer = connection.BeginTextImport(
                                        $"COPY relation ({string.Join(",", relationKeys)}) FROM STDIN WITH NULL AS ''");
                                }

                                var relationValues = new List<string>
                                {
                                    relation.Id.ToString(),
                                    relation.Version.ToString(),
                                    relation.ChangeSetId.ToString(),
                                    relation.TimeStamp.ValueAsText(),
                                    relation.UserId.ToString(),
                                    relation.UserName.ValueAsText(),
                                    relation.Visible.ToString(),
                                    $"{string.Join(",", relation.Tags.Select(t => $"\"{t.Key.TextEscape(2)}\"=>\"{t.Value.TextEscape(2)}\""))}",
                                    $"{{{string.Join(",", relation.Members.Select(t => $"\\\"({t.Id.ToString()},\\\\\\\"{t.Role.TextEscape(4)}\\\\\\\",{((int) t.Type).ToString()})\\\""))}}}"
                                };

                                writer.WriteLine(string.Join("\t", relationValues));

                                break;
                            default:
                                throw new NotImplementedException();
                        }

                        if (count++ % 1000 == 0)
                            await _progressHub.ProgressAsync(100f * uploadStream.Position / uploadStream.Length, id,
                                session);
                    }

                    writer?.Dispose();
                    await _progressHub.ProgressAsync(100f, id, session);
                }

                BuildIndices(connection);

                await connection.CloseAsync();
            }
        }

        public async Task UpdateAsync(Stream uploadStream, Dictionary<string, string> options, string session)
        {
            using (var connection = new NpgsqlConnection(GetOsmConnectionString()))
            {
                await connection.OpenAsync();

                await ExecuteResourceAsync(Assembly.GetExecutingAssembly(), "Loader.Osm.CreateTempTables.sql",
                    connection);

                long count = 0;
                using (var source = new PBFOsmStreamSource(uploadStream))
                {
                    TextWriter writer = null;
                    var lastType = ElementType.None;

                    var id = Guid.NewGuid().ToString();
                    await _progressHub.InitAsync(id, session);

                    foreach (var element in source)
                    {
                        switch (element)
                        {
                            case Node node:
                                if (lastType != ElementType.Node)
                                {
                                    lastType = ElementType.Node;
                                    writer?.Dispose();
                                    writer = connection.BeginTextImport(
                                        $"COPY temp_node ({string.Join(",", nodeKeys)}) FROM STDIN WITH NULL AS ''");
                                }

                                var nodeValues = new List<string>
                                {
                                    node.Id.ToString(),
                                    node.Version.ToString(),
                                    node.Latitude.ValueAsText(),
                                    node.Longitude.ValueAsText(),
                                    node.ChangeSetId.ToString(),
                                    node.TimeStamp.ValueAsText(),
                                    node.UserId.ToString(),
                                    node.UserName.ValueAsText(),
                                    node.Visible.ToString(),
                                    $"{string.Join(",", node.Tags.Select(t => $"\"{t.Key.TextEscape(2)}\"=>\"{t.Value.TextEscape(2)}\""))}"
                                };

                                writer.WriteLine(string.Join("\t", nodeValues));

                                break;
                            case Way way:
                                if (lastType != ElementType.Way)
                                {
                                    lastType = ElementType.Way;
                                    writer?.Dispose();
                                    writer = connection.BeginTextImport(
                                        $"COPY temp_way ({string.Join(",", wayKeys)}) FROM STDIN WITH NULL AS ''");
                                }

                                var wayValues = new List<string>
                                {
                                    way.Id.ToString(),
                                    way.Version.ToString(),
                                    way.ChangeSetId.ToString(),
                                    way.TimeStamp.ValueAsText(),
                                    way.UserId.ToString(),
                                    way.UserName.ValueAsText(),
                                    way.Visible.ToString(),
                                    $"{string.Join(",", way.Tags.Select(t => $"\"{t.Key.TextEscape(2)}\"=>\"{t.Value.TextEscape(2)}\""))}",
                                    $"{{{string.Join(",", way.Nodes.Select(t => $"{t.ToString()}"))}}}"
                                };

                                writer.WriteLine(string.Join("\t", wayValues));

                                break;
                            case Relation relation:
                                if (lastType != ElementType.Relation)
                                {
                                    lastType = ElementType.Relation;
                                    writer?.Dispose();
                                    writer = connection.BeginTextImport(
                                        $"COPY temp_relation ({string.Join(",", relationKeys)}) FROM STDIN WITH NULL AS ''");
                                }

                                var relationValues = new List<string>
                                {
                                    relation.Id.ToString(),
                                    relation.Version.ToString(),
                                    relation.ChangeSetId.ToString(),
                                    relation.TimeStamp.ValueAsText(),
                                    relation.UserId.ToString(),
                                    relation.UserName.ValueAsText(),
                                    relation.Visible.ToString(),
                                    $"{string.Join(",", relation.Tags.Select(t => $"\"{t.Key.TextEscape(2)}\"=>\"{t.Value.TextEscape(2)}\""))}",
                                    $"{{{string.Join(",", relation.Members.Select(t => $"\\\"({t.Id.ToString()},\\\\\\\"{t.Role.TextEscape(4)}\\\\\\\",{((int) t.Type).ToString()})\\\""))}}}"
                                };

                                writer.WriteLine(string.Join("\t", relationValues));

                                break;
                            default:
                                throw new NotImplementedException();
                        }

                        if (count++ % 1000 == 0)
                            await _progressHub.ProgressAsync(100f * uploadStream.Position / uploadStream.Length, id,
                                session);
                    }

                    writer?.Dispose();
                    await _progressHub.ProgressAsync(100f, id, session);
                }

                await ExecuteResourceAsync(Assembly.GetExecutingAssembly(), "Loader.Osm.InsertFromTempTables.sql",
                    connection);

                await connection.CloseAsync();
            }
        }

        private void BuildIndices(NpgsqlConnection conn)
        {
            var sqls = new[]
            {
                new[]
                {
                    "SELECT CONCAT('ALTER TABLE ', table_name, ' ADD PRIMARY KEY (id);') FROM information_schema.tables WHERE table_schema = 'public' AND table_name IN ('node','way','relation')",
                    "SELECT CONCAT('CREATE INDEX ON ', table_name, ' USING HASH (', column_name, ');') FROM information_schema.columns WHERE table_schema = 'public' AND column_name in ('tags') AND table_name IN ('place')",
                    "SELECT CONCAT('CREATE INDEX ON ', table_name, ' USING GIST (', column_name, ');') FROM information_schema.columns WHERE table_schema = 'public' AND column_name in ('location') AND table_name IN ('place','node','way','relation')",
                    "SELECT CONCAT('CREATE UNIQUE INDEX ON ', table_name, ' (', column_name, ');') FROM information_schema.columns WHERE table_schema = 'public' AND column_name in ('record_number','record_id') AND table_name IN ('place','node','way','relation')",
                    "SELECT CONCAT('CREATE UNIQUE INDEX ON ', table_name, ' (osm_id,osm_type);') FROM information_schema.tables WHERE table_schema = 'public' AND table_name IN ('place')"
                }
            };

            SelectAndExecute(sqls, conn, GetOsmConnectionString());
        }

        private void DropTables(NpgsqlConnection conn)
        {
            var sqls = new[]
            {
                new[]
                {
                    "SELECT CONCAT('DROP TABLE ', table_name) FROM information_schema.tables WHERE table_schema = 'public' AND table_name IN ('place','node','way','relation')"
                }
            };

            SelectAndExecute(sqls, conn, GetOsmConnectionString());
        }
    }
}