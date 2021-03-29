using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using NDbfReader;
using Npgsql;
using Placium.Common;

namespace Loader.Fias
{
    public class FiasUploadService : IUploadService
    {
        private readonly Dictionary<string, string> _deleted = new Dictionary<string, string>
        {
            {"addrob", "aoid"},
            {"house", "houseid"},
            {"room", "roomid"},
            {"stead", "steadid"}
        };

        private readonly Encoding _encoding = Encoding.GetEncoding("cp866");

        private readonly Dictionary<Regex, string> _masks = new Dictionary<Regex, string>
        {
            {new Regex(@"^actstat$", RegexOptions.IgnoreCase), "actstatid"},
            {new Regex(@"^centerst$", RegexOptions.IgnoreCase), "centerstid"},
            {new Regex(@"^curentst$", RegexOptions.IgnoreCase), "curentstid"},
            {new Regex(@"^eststat$", RegexOptions.IgnoreCase), "eststatid"},
            {new Regex(@"^flattype$", RegexOptions.IgnoreCase), "fltypeid"},
            {new Regex(@"^hststat$", RegexOptions.IgnoreCase), "housestid"},
            {new Regex(@"^intvstat$", RegexOptions.IgnoreCase), "intvstatid"},
            {new Regex(@"^operstat$", RegexOptions.IgnoreCase), "operstatid"},
            {new Regex(@"^roomtype$", RegexOptions.IgnoreCase), "rmtypeid"},
            {new Regex(@"^socrbase$", RegexOptions.IgnoreCase), "kod_t_st"},
            {new Regex(@"^strstat$", RegexOptions.IgnoreCase), "strstatid"},
            {new Regex(@"^ndoctype$", RegexOptions.IgnoreCase), "ndtypeid"},
            {new Regex(@"^daddrob$", RegexOptions.IgnoreCase), "aoid"},
            {new Regex(@"^dhouse$", RegexOptions.IgnoreCase), "houseid"},
            {new Regex(@"^droom$", RegexOptions.IgnoreCase), "roomid"},
            {new Regex(@"^dstead$", RegexOptions.IgnoreCase), "steadid"},
            {new Regex(@"^dnordoc$", RegexOptions.IgnoreCase), "docimgid"},
            {new Regex(@"^addrob[0-9]+$", RegexOptions.IgnoreCase), "aoid"},
            {new Regex(@"^house[0-9]+$", RegexOptions.IgnoreCase), "houseid"},
            {new Regex(@"^room[0-9]+$", RegexOptions.IgnoreCase), "roomid"},
            {new Regex(@"^stead[0-9]+$", RegexOptions.IgnoreCase), "steadid"}
        };

        private readonly ProgressHub _progressHub;

        public FiasUploadService(ProgressHub progressHub)
        {
            _progressHub = progressHub;
        }

        public async Task InstallAsync(Stream uploadStream, NpgsqlConnection connection, string session)
        {
            using (var stream = Assembly.GetExecutingAssembly()
                .GetManifestResourceStream("Loader.Fias.CreateSequence.sql"))
            using (var sr = new StreamReader(stream, Encoding.UTF8))
            using (var command = new NpgsqlCommand(await sr.ReadToEndAsync(), connection))
            {
                command.ExecuteNonQuery();
            }

            var tableNames = new List<string>();

            using (var archive = new ZipArchive(uploadStream))
            {
                var current = 0;
                var count = archive.Entries.Count;

                var id = Guid.NewGuid().ToString();
                await _progressHub.Init(id, session);

                foreach (var entry in archive.Entries)
                {
                    if (entry.FullName.EndsWith(".dbf", StringComparison.OrdinalIgnoreCase))
                    {
                        var tableName = Path.GetFileNameWithoutExtension(entry.Name).ToLower();
                        using (var stream = entry.Open())
                        {
                            using (var table = Table.Open(stream, HeaderLoader.Default))
                            {
                                var reader = table.OpenReader(_encoding);
                                var columns = table.Columns;

                                var names = columns.Select(x => x.Name.ToLower()).ToList();

                                TextWriter writer = null;

                                while (reader.Read())
                                    if (writer == null)
                                    {
                                        try
                                        {
                                            using (var command = new NpgsqlCommand(
                                                $"DROP TABLE IF EXISTS {tableName}"
                                                , connection))
                                            {
                                                command.ExecuteNonQuery();
                                            }
                                        }
                                        catch
                                        {
                                        }

                                        using (var command = new NpgsqlCommand(
                                            $"CREATE TABLE {tableName} ({string.Join(",", columns.Select(x => $"{x.Name} {x.TypeAsText()}"))})"
                                            , connection))
                                        {
                                            command.ExecuteNonQuery();
                                            tableNames.Add(tableName);
                                        }

                                        writer = connection.BeginTextImport(
                                            $"COPY {tableName} ({string.Join(", ", names)}) FROM STDIN WITH NULL AS ''");
                                    }
                                    else
                                    {
                                        var values = columns.Select(x => x.ValueAsText(reader)).ToList();
                                        writer.WriteLine(string.Join("\t", values));
                                    }

                                writer?.Dispose();
                            }
                        }
                    }

                    await _progressHub.Progress(100f * ++current / count, id, session);
                }

                await _progressHub.Progress(100f, id, session);
            }

            BuildIndices(tableNames, connection);
        }

        public async Task UpdateAsync(Stream uploadStream, NpgsqlConnection connection, string session)
        {
            var tableNames = new List<string>();

            using (var archive = new ZipArchive(uploadStream))
            {
                var current = 0;
                var count = archive.Entries.Count;

                var id = Guid.NewGuid().ToString();
                await _progressHub.Init(id, session);

                foreach (var entry in archive.Entries)
                {
                    if (entry.FullName.EndsWith(".dbf", StringComparison.OrdinalIgnoreCase))
                    {
                        var tableName = Path.GetFileNameWithoutExtension(entry.Name).ToLower();
                        var key = FindKey(tableName);

                        if (!string.IsNullOrEmpty(key))
                            using (var stream = entry.Open())
                            {
                                using (var table = Table.Open(stream, HeaderLoader.Default))
                                {
                                    var reader = table.OpenReader(_encoding);
                                    var columns = table.Columns;


                                    var names = columns.Select(x => x.Name.ToLower()).ToList();

                                    TextWriter writer = null;

                                    while (reader.Read())
                                        if (writer == null)
                                        {
                                            if (!TableIsExists(tableName, connection))
                                            {
                                                using (var command = new NpgsqlCommand(
                                                    $"CREATE TABLE {tableName} ({string.Join(",", columns.Select(x => $"{x.Name} {x.TypeAsText()}"))});"
                                                    , connection))
                                                {
                                                    command.ExecuteNonQuery();
                                                    tableNames.Add(tableName);
                                                }

                                                writer = connection.BeginTextImport(
                                                    $"COPY {tableName} ({string.Join(", ", names)}) FROM STDIN WITH NULL AS '';");
                                            }
                                            else
                                            {
                                                try
                                                {
                                                    using (var command = new NpgsqlCommand(
                                                        $"DROP TABLE IF EXISTS temp_{tableName};"
                                                        , connection))
                                                    {
                                                        command.ExecuteNonQuery();
                                                    }
                                                }
                                                catch
                                                {
                                                }

                                                using (var command = new NpgsqlCommand(
                                                    $"CREATE TABLE temp_{tableName} ({string.Join(",", columns.Select(x => $"{x.Name} {x.TypeAsText()}"))});"
                                                    , connection))
                                                {
                                                    command.ExecuteNonQuery();
                                                }

                                                writer = connection.BeginTextImport(
                                                    $"COPY temp_{tableName} ({string.Join(", ", names)}) FROM STDIN WITH NULL AS '';");
                                            }
                                        }
                                        else
                                        {
                                            var values = columns.Select(x => x.ValueAsText(reader)).ToList();
                                            writer.WriteLine(string.Join("\t", values));
                                        }

                                    writer?.Dispose();

                                    if (TableIsExists($"temp_{tableName}", connection))
                                    {
                                        using (var command = new NpgsqlCommand(
                                            $"INSERT INTO {tableName} ({string.Join(", ", names)}) SELECT {string.Join(", ", names)} FROM temp_{tableName} ON CONFLICT ({key}) DO UPDATE SET {string.Join(", ", names.Select(x => $"{x}=EXCLUDED.{x}"))}, record_number=nextval('record_number_seq');"
                                            , connection))
                                        {
                                            command.ExecuteNonQuery();
                                        }

                                        try
                                        {
                                            using (var command = new NpgsqlCommand(
                                                $"DROP TABLE temp_{tableName};"
                                                , connection))
                                            {
                                                command.ExecuteNonQuery();
                                            }
                                        }
                                        catch
                                        {
                                        }
                                    }
                                }
                            }
                    }

                    await _progressHub.Progress(100f * ++current / count, id, session);
                }

                await _progressHub.Progress(100f, id, session);
            }

            BuildIndices(tableNames, connection);

            foreach (var pair in _deleted) ExcludeDeleted(pair.Key, pair.Value, connection);
        }

        private string FindKey(string tableName)
        {
            foreach (var mask in _masks)
                if (mask.Key.IsMatch(tableName))
                    return mask.Value;
            return null;
        }

        private void BuildIndices(List<string> tableNames, NpgsqlConnection conn)
        {
            if (!tableNames.Any()) return;

            var sqls = new[]
            {
                $"SELECT CONCAT('ALTER TABLE ', table_name, ' ADD COLUMN record_number BIGINT DEFAULT nextval(''record_number_seq'');') FROM information_schema.columns WHERE table_schema = 'public' AND (CONCAT(table_name, 'id')=column_name OR column_name IN ('aoid', 'houseid', 'roomid', 'steadid', 'rmtypeid', 'fltypeid', 'housestid', 'kod_t_st', 'ndtypeid')) AND table_name IN ({string.Join(",", tableNames.Select(x => $"'{x}'"))})",
                $"SELECT CONCAT('ALTER TABLE ', table_name, ' ADD PRIMARY KEY (', column_name, ');') FROM information_schema.columns WHERE table_schema = 'public' AND (CONCAT(table_name, 'id')=column_name OR column_name IN ('aoid', 'houseid', 'roomid', 'steadid', 'rmtypeid', 'fltypeid', 'housestid', 'kod_t_st', 'ndtypeid')) AND table_name IN ({string.Join(",", tableNames.Select(x => $"'{x}'"))})",
                $"SELECT CONCAT('CREATE INDEX ON ', table_name, ' (', column_name, ');') FROM information_schema.columns WHERE table_schema = 'public' AND (column_name like '%guid' OR column_name like '%status' or column_name in ('shortname', 'record_number')) AND table_name IN ({string.Join(",", tableNames.Select(x => $"'{x}'"))})"
            };

            SelectAndExecute(sqls, conn);
        }

        private void SelectAndExecute(string[] sqls, NpgsqlConnection conn)
        {
            var cmds = new List<string>();

            using (var command = new NpgsqlCommand(string.Join("\nUNION\n", sqls), conn))
            {
                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                        cmds.Add(reader.GetString(0));
                }
            }

            using (var command = new NpgsqlCommand(string.Join(";\n", cmds), conn))
            {
                command.ExecuteNonQuery();
            }
        }

        private bool TableIsExists(string tableName, NpgsqlConnection conn)
        {
            using (var command = new NpgsqlCommand(
                $"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema='public' AND table_name='{tableName}');"
                , conn))
            {
                using (var reader = command.ExecuteReader())
                {
                    if (reader.Read())
                        return reader.GetBoolean(0);
                }
            }

            throw new NotImplementedException();
        }

        private void ExcludeDeleted(string tableName, string key, NpgsqlConnection conn)
        {
            if (!TableIsExists($"d{tableName}", conn)) return;

            var sqls = new[]
            {
                $"SELECT CONCAT('DELETE FROM ', table_name, ' USING d{tableName} WHERE d{tableName}.{key}=', table_name, '.{key}') FROM information_schema.tables WHERE table_schema='public' AND table_name LIKE '{tableName}%'"
            };

            SelectAndExecute(sqls, conn);
        }
    }
}