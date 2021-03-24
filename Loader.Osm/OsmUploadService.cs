using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Npgsql;
using OsmSharp;
using OsmSharp.Streams;
using Placium.Common;

namespace Loader.Osm
{
    public class OsmUploadService : IUploadService
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

        public OsmUploadService(ProgressHub progressHub)
        {
            _progressHub = progressHub;
        }

        public async Task InstallAsync(Stream uploadStream, NpgsqlConnection connection, string session)
        {
            using (var stream = Assembly.GetExecutingAssembly()
                .GetManifestResourceStream("Loader.Osm.CreateTables.sql"))
            using (var sr = new StreamReader(stream, Encoding.UTF8))
            using (var command = new NpgsqlCommand(await sr.ReadToEndAsync(), connection))
            {
                command.ExecuteNonQuery();
            }

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
                                writer = connection.BeginTextImport(
                                    $"COPY Node ({string.Join(", ", nodeKeys)}) FROM STDIN WITH NULL AS '';");
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
                                $"{string.Join(", ", node.Tags.Select(t => $"\"{PgExtensions.TextEscape(t.Key, 2)}\" => \"{PgExtensions.TextEscape(t.Value, 2)}\""))}"
                            };

                            writer.WriteLine(string.Join("\t", nodeValues));

                            break;
                        case Way way:
                            if (lastType != ElementType.Way)
                            {
                                lastType = ElementType.Way;
                                writer?.Dispose();
                                writer = connection.BeginTextImport(
                                    $"COPY Way ({string.Join(", ", wayKeys)}) FROM STDIN WITH NULL AS '';");
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
                                $"{string.Join(", ", way.Tags.Select(t => $"\"{t.Key.TextEscape(2)}\" => \"{t.Value.TextEscape(2)}\""))}",
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
                                    $"COPY Relation ({string.Join(", ", relationKeys)}) FROM STDIN WITH NULL AS '';");
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
                                $"{string.Join(", ", relation.Tags.Select(t => $"\"{t.Key.TextEscape(2)}\" => \"{t.Value.TextEscape(2)}\""))}",
                                $"{{{string.Join(",", relation.Members.Select(t => $"\\\"({t.Id.ToString()},\\\\\\\"{t.Role.TextEscape(4)}\\\\\\\",{((int) t.Type).ToString()})\\\""))}}}"
                            };

                            writer.WriteLine(string.Join("\t", relationValues));

                            break;
                        default:
                            throw new NotImplementedException();
                    }

                    if (count++ % 1000 == 0)
                        await _progressHub.Progress(100f * count / (count + 1000000), session);
                }

                writer?.Dispose();
            }

            using (var stream = Assembly.GetExecutingAssembly()
                .GetManifestResourceStream("Loader.Osm.CreateIndeces.sql"))
            using (var sr = new StreamReader(stream, Encoding.UTF8))
            using (var command = new NpgsqlCommand(await sr.ReadToEndAsync(), connection))
            {
                command.ExecuteNonQuery();
            }
        }

        public async Task UpdateAsync(Stream uploadStream, NpgsqlConnection connection, string session)
        {
            using (var stream = Assembly.GetExecutingAssembly()
                .GetManifestResourceStream("Loader.Osm.CreateTempTables.sql"))
            using (var sr = new StreamReader(stream, Encoding.UTF8))
            using (var command = new NpgsqlCommand(await sr.ReadToEndAsync(), connection))
            {
                command.ExecuteNonQuery();
            }

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
                                writer = connection.BeginTextImport(
                                    $"COPY temp_node ({string.Join(", ", nodeKeys)}) FROM STDIN WITH NULL AS '';");
                            }

                            var nodeValues = new List<string>
                            {
                                node.Id.ToString(),
                                node.Version.ToString(),
                                node.Latitude.ValueAsText(),
                                node.Latitude.ValueAsText(),
                                node.ChangeSetId.ToString(),
                                node.TimeStamp.ValueAsText(),
                                node.UserId.ToString(),
                                node.UserName.ValueAsText(),
                                node.Visible.ToString(),
                                $"{string.Join(", ", node.Tags.Select(t => $"\"{t.Key.TextEscape(2)}\" => \"{t.Value.TextEscape(2)}\""))}"
                            };

                            writer.WriteLine(string.Join("\t", nodeValues));

                            break;
                        case Way way:
                            if (lastType != ElementType.Way)
                            {
                                lastType = ElementType.Way;
                                writer?.Dispose();
                                writer = connection.BeginTextImport(
                                    $"COPY temp_way ({string.Join(", ", wayKeys)}) FROM STDIN WITH NULL AS '';");
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
                                $"{string.Join(", ", way.Tags.Select(t => $"\"{t.Key.TextEscape(2)}\" => \"{t.Value.TextEscape(2)}\""))}",
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
                                    $"COPY temp_relation ({string.Join(", ", relationKeys)}) FROM STDIN WITH NULL AS '';");
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
                                $"{string.Join(", ", relation.Tags.Select(t => $"\"{t.Key.TextEscape(2)}\" => \"{t.Value.TextEscape(2)}\""))}",
                                $"{{{string.Join(",", relation.Members.Select(t => $"\\\"({t.Id.ToString()},\\\\\\\"{t.Role.TextEscape(4)}\\\\\\\",{((int) t.Type).ToString()})\\\""))}}}"
                            };

                            writer.WriteLine(string.Join("\t", relationValues));

                            break;
                        default:
                            throw new NotImplementedException();
                    }

                    if (count++ % 1000 == 0)
                        await _progressHub.Progress(100f * count / (count + 1000000), session);
                }

                writer?.Dispose();
            }

            using (var stream = Assembly.GetExecutingAssembly()
                .GetManifestResourceStream("Loader.Osm.InsertFromTempTables.sql"))
            using (var sr = new StreamReader(stream, Encoding.UTF8))
            using (var command = new NpgsqlCommand(await sr.ReadToEndAsync(), connection))
            {
                command.ExecuteNonQuery();
            }
        }
    }
}