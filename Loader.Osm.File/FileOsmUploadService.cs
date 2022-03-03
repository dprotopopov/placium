using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using Npgsql;
using OsmSharp;
using OsmSharp.Streams;
using Placium.Common;

namespace Loader.Osm.File
{
    public class FileOsmUploadService : BaseAppService, IUploadService
    {
        public enum ElementType
        {
            None,
            Node,
            Way,
            Relation
        }

        private readonly string[] _nodeKeys =
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

        private readonly IProgressClient _progressClient;

        private readonly string[] _relationKeys =
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

        private readonly string[] _wayKeys =
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

        public FileOsmUploadService(IProgressClient progressClient, IConnectionsConfig configuration) : base(
            configuration)
        {
            _progressClient = progressClient;
        }

        public async Task InstallAsync(Stream uploadStream, Dictionary<string, string> options, string session)
        {
            await using var connection = new NpgsqlConnection(GetOsmConnectionString());
            await connection.OpenAsync();

            var id = Guid.NewGuid().ToString();
            await _progressClient.Init(id, session);

            DropTables(connection);

            await ExecuteResourceAsync(Assembly.GetExecutingAssembly(), "Loader.Osm.File.CreateTables.pgsql",
                connection);

            long count = 0;
            using (var source = new PBFOsmStreamSource(uploadStream))
            {
                TextWriter writer = null;
                var lastType = ElementType.None;

                foreach (var element in source)
                {
                    switch (element)
                    {
                        case Node node:
                            if (lastType != ElementType.Node)
                            {
                                lastType = ElementType.Node;
                                writer?.Dispose();
                                writer = await connection.BeginTextImportAsync(
                                    $"COPY node ({string.Join(",", _nodeKeys)}) FROM STDIN WITH NULL AS ''");
                            }

                            var nodeValues = new[]
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

                            Debug.Assert(writer != null);

                            writer.WriteLine(string.Join("\t", nodeValues));

                            break;
                        case Way way:
                            if (lastType != ElementType.Way)
                            {
                                lastType = ElementType.Way;
                                writer?.Dispose();
                                writer = await connection.BeginTextImportAsync(
                                    $"COPY way ({string.Join(",", _wayKeys)}) FROM STDIN WITH NULL AS ''");
                            }

                            var wayValues = new[]
                            {
                                way.Id.ToString(),
                                way.Version.ToString(),
                                way.ChangeSetId.ToString(),
                                way.TimeStamp.ValueAsText(),
                                way.UserId.ToString(),
                                way.UserName.ValueAsText(),
                                way.Visible.ToString(),
                                $"{string.Join(",", way.Tags.Select(t => $"\"{t.Key.TextEscape(2)}\"=>\"{t.Value.TextEscape(2)}\""))}",
                                $"{{{string.Join(",", way.Nodes.Select(t => $"{t}"))}}}"
                            };

                            Debug.Assert(writer != null);

                            writer.WriteLine(string.Join("\t", wayValues));

                            break;
                        case Relation relation:
                            if (lastType != ElementType.Relation)
                            {
                                lastType = ElementType.Relation;
                                writer?.Dispose();
                                writer = await connection.BeginTextImportAsync(
                                    $"COPY relation ({string.Join(",", _relationKeys)}) FROM STDIN WITH NULL AS ''");
                            }

                            var relationValues = new[]
                            {
                                relation.Id.ToString(),
                                relation.Version.ToString(),
                                relation.ChangeSetId.ToString(),
                                relation.TimeStamp.ValueAsText(),
                                relation.UserId.ToString(),
                                relation.UserName.ValueAsText(),
                                relation.Visible.ToString(),
                                $"{string.Join(",", relation.Tags.Select(t => $"\"{t.Key.TextEscape(2)}\"=>\"{t.Value.TextEscape(2)}\""))}",
                                $"{{{string.Join(",", relation.Members.Select(t => $"\\\"({t.Id},\\\\\\\"{t.Role.TextEscape(4)}\\\\\\\",{(int)t.Type})\\\""))}}}"
                            };

                            Debug.Assert(writer != null);

                            writer.WriteLine(string.Join("\t", relationValues));

                            break;
                        default:
                            throw new NotImplementedException();
                    }

                    if (count++ % 1000 == 0)
                        await _progressClient.Progress(100f * uploadStream.Position / uploadStream.Length, id,
                            session);
                }

                writer?.Dispose();
            }

            BuildIndices(connection);

            await _progressClient.Finalize(id, session);

            await connection.CloseAsync();
        }

        public async Task UpdateAsync(Stream uploadStream, Dictionary<string, string> options, string session)
        {
            await using var connection = new NpgsqlConnection(GetOsmConnectionString());
            await connection.OpenAsync();

            var id = Guid.NewGuid().ToString();
            await _progressClient.Init(id, session);

            await ExecuteResourceAsync(Assembly.GetExecutingAssembly(), "Loader.Osm.File.CreateTempTables.pgsql",
                connection);

            long count = 0;
            using (var source = new PBFOsmStreamSource(uploadStream))
            {
                TextWriter writer = null;
                var lastType = ElementType.None;

                foreach (var element in source)
                {
                    switch (element)
                    {
                        case Node node:
                            if (lastType != ElementType.Node)
                            {
                                lastType = ElementType.Node;
                                writer?.Dispose();
                                writer = await connection.BeginTextImportAsync(
                                    $"COPY temp_node ({string.Join(",", _nodeKeys)}) FROM STDIN WITH NULL AS ''");
                            }

                            var nodeValues = new[]
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

                            Debug.Assert(writer != null);

                            writer.WriteLine(string.Join("\t", nodeValues));

                            break;
                        case Way way:
                            if (lastType != ElementType.Way)
                            {
                                lastType = ElementType.Way;
                                writer?.Dispose();
                                writer = await connection.BeginTextImportAsync(
                                    $"COPY temp_way ({string.Join(",", _wayKeys)}) FROM STDIN WITH NULL AS ''");
                            }

                            var wayValues = new[]
                            {
                                way.Id.ToString(),
                                way.Version.ToString(),
                                way.ChangeSetId.ToString(),
                                way.TimeStamp.ValueAsText(),
                                way.UserId.ToString(),
                                way.UserName.ValueAsText(),
                                way.Visible.ToString(),
                                $"{string.Join(",", way.Tags.Select(t => $"\"{t.Key.TextEscape(2)}\"=>\"{t.Value.TextEscape(2)}\""))}",
                                $"{{{string.Join(",", way.Nodes.Select(t => $"{t}"))}}}"
                            };

                            Debug.Assert(writer != null);

                            writer.WriteLine(string.Join("\t", wayValues));

                            break;
                        case Relation relation:
                            if (lastType != ElementType.Relation)
                            {
                                lastType = ElementType.Relation;
                                writer?.Dispose();
                                writer = await connection.BeginTextImportAsync(
                                    $"COPY temp_relation ({string.Join(",", _relationKeys)}) FROM STDIN WITH NULL AS ''");
                            }

                            var relationValues = new[]
                            {
                                relation.Id.ToString(),
                                relation.Version.ToString(),
                                relation.ChangeSetId.ToString(),
                                relation.TimeStamp.ValueAsText(),
                                relation.UserId.ToString(),
                                relation.UserName.ValueAsText(),
                                relation.Visible.ToString(),
                                $"{string.Join(",", relation.Tags.Select(t => $"\"{t.Key.TextEscape(2)}\"=>\"{t.Value.TextEscape(2)}\""))}",
                                $"{{{string.Join(",", relation.Members.Select(t => $"\\\"({t.Id},\\\\\\\"{t.Role.TextEscape(4)}\\\\\\\",{(int)t.Type})\\\""))}}}"
                            };

                            Debug.Assert(writer != null);

                            writer.WriteLine(string.Join("\t", relationValues));

                            break;
                        default:
                            throw new NotImplementedException();
                    }

                    if (count++ % 1000 == 0)
                        await _progressClient.Progress(100f * uploadStream.Position / uploadStream.Length, id,
                            session);
                }

                writer?.Dispose();
            }

            await ExecuteResourceAsync(Assembly.GetExecutingAssembly(), "Loader.Osm.File.InsertFromTempTables.pgsql",
                connection);

            await _progressClient.Finalize(id, session);

            await connection.CloseAsync();
        }

        private void BuildIndices(NpgsqlConnection conn)
        {
            var sqls = new[]
            {
                new[]
                {
                    "SELECT CONCAT('ALTER TABLE ', table_name, ' ADD PRIMARY KEY (id);') FROM information_schema.tables WHERE table_schema = 'public' AND table_name IN ('node','way','relation')",
                    "SELECT CONCAT('CREATE INDEX ON ', table_name, ' (', column_name, ');') FROM information_schema.columns WHERE table_schema = 'public' AND column_name in ('latitude','longitude') AND table_name IN ('node')",
                    "SELECT CONCAT('CREATE INDEX ON ', table_name, ' USING GIN (', column_name, ');') FROM information_schema.columns WHERE table_schema = 'public' AND column_name in ('nodes') AND table_name IN ('way')",
                    "SELECT CONCAT('CREATE INDEX ON ', table_name, ' USING HASH (', column_name, ');') FROM information_schema.columns WHERE table_schema = 'public' AND column_name in ('tags') AND table_name IN ('addrx','placex')",
                    "SELECT CONCAT('CREATE INDEX ON ', table_name, ' USING GIST (', column_name, ');') FROM information_schema.columns WHERE table_schema = 'public' AND column_name in ('location') AND table_name IN ('addrx','placex')",
                    "SELECT CONCAT('CREATE UNIQUE INDEX ON ', table_name, ' (', column_name, ');') FROM information_schema.columns WHERE table_schema = 'public' AND column_name in ('record_number','record_id') AND table_name IN ('addrx','placex','node','way','relation')",
                    "SELECT CONCAT('CREATE UNIQUE INDEX ON ', table_name, ' (osm_id,osm_type);') FROM information_schema.tables WHERE table_schema = 'public' AND table_name IN ('placex')"
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
                    "SELECT CONCAT('DROP TABLE ', table_name) FROM information_schema.tables WHERE table_schema = 'public' AND table_name IN ('addrx','placex','node','way','relation')"
                }
            };

            SelectAndExecute(sqls, conn, GetOsmConnectionString());
        }
    }
}