using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using NDbfReader;
using Npgsql;
using Placium.Common;

namespace Loader.Fias
{
    public class FiasUploadService : BaseService, IUploadService
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
            {new Regex(@"^addrob\d+$", RegexOptions.IgnoreCase), "aoid"},
            {new Regex(@"^house\d+$", RegexOptions.IgnoreCase), "houseid"},
            {new Regex(@"^room\d+$", RegexOptions.IgnoreCase), "roomid"},
            {new Regex(@"^stead\d+$", RegexOptions.IgnoreCase), "steadid"}
        };

        private readonly ProgressHub _progressHub;

        public FiasUploadService(ProgressHub progressHub, IConfiguration configuration) : base(configuration)
        {
            _progressHub = progressHub;
        }

        public async Task InstallAsync(Stream uploadStream, Dictionary<string, string> options, string session)
        {
            try
            {
                var region = options.ContainsKey("region") ? options["region"] : string.Empty;

                var regionMask = RegionMasks(region);

                using (var connection = new NpgsqlConnection(GetFiasConnectionString()))
                {
                    await connection.OpenAsync();

                    var id = Guid.NewGuid().ToString();
                    await _progressHub.InitAsync(id, session);

                    DropTables(connection);

                    await ExecuteResourceAsync(Assembly.GetExecutingAssembly(), "Loader.Fias.CreateSequence.sql",
                        connection);

                    using (var archive = new ZipArchive(uploadStream))
                    {
                        foreach (var entry in archive.Entries)
                        {
                            if (entry.FullName.EndsWith(".dbf", StringComparison.OrdinalIgnoreCase))
                            {
                                var tableName = Path.GetFileNameWithoutExtension(entry.Name).ToLower();

                                var skip = false;
                                foreach (var pair in regionMask)
                                    skip = skip || pair.Key.IsMatch(tableName) && !pair.Value.IsMatch(tableName);

                                if (!skip)
                                {
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
                                                var buildIndices = false;

                                                while (reader.Read())
                                                {
                                                    if (writer == null)
                                                    {
                                                        using (var command = new NpgsqlCommand(string.Join(";",
                                                                $"DROP TABLE IF EXISTS {tableName}",
                                                                $"CREATE TABLE {tableName} ({string.Join(",", columns.Select(x => $"{x.Name} {x.TypeAsText()}"))})")
                                                            , connection))
                                                        {
                                                            command.Prepare();

                                                            command.ExecuteNonQuery();
                                                        }

                                                        writer = connection.BeginTextImport(
                                                            $"COPY {tableName} ({string.Join(",", names)}) FROM STDIN WITH NULL AS ''");

                                                        buildIndices = true;
                                                    }

                                                    var values = columns.Select(x => x.ValueAsText(reader)).ToList();
                                                    writer.WriteLine(string.Join("\t", values));
                                                }

                                                writer?.Dispose();

                                                if (buildIndices)
                                                    BuildIndices(new[] {tableName}, connection);
                                            }
                                        }
                                }
                            }

                            await _progressHub.ProgressAsync(100f * uploadStream.Position / uploadStream.Length, id,
                                session);
                        }
                    }

                    await _progressHub.ProgressAsync(100f, id, session);
                    await _progressHub.CompleteAsync(session);

                    await connection.CloseAsync();
                }
            }
            catch (Exception ex)
            {
                await _progressHub.ErrorAsync(ex.Message, session);
                throw;
            }
        }

        public async Task UpdateAsync(Stream uploadStream, Dictionary<string, string> options, string session)
        {
            try
            {
                var region = options.ContainsKey("region") ? options["region"] : string.Empty;

                var regionMask = RegionMasks(region);

                using (var connection = new NpgsqlConnection(GetFiasConnectionString()))
                {
                    await connection.OpenAsync();

                    var id = Guid.NewGuid().ToString();
                    await _progressHub.InitAsync(id, session);

                    using (var archive = new ZipArchive(uploadStream))
                    {
                        foreach (var entry in archive.Entries)
                        {
                            if (entry.FullName.EndsWith(".dbf", StringComparison.OrdinalIgnoreCase))
                            {
                                var tableName = Path.GetFileNameWithoutExtension(entry.Name).ToLower();

                                var skip = false;
                                foreach (var pair in regionMask)
                                    skip = skip || pair.Key.IsMatch(tableName) && !pair.Value.IsMatch(tableName);

                                if (!skip)
                                {
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
                                                var buildIndices = false;
                                                var insertFromTemp = false;

                                                while (reader.Read())
                                                {
                                                    if (writer == null)
                                                    {
                                                        if (!TableIsExists(tableName, connection))
                                                        {
                                                            using (var command = new NpgsqlCommand(
                                                                $"CREATE TABLE {tableName} ({string.Join(",", columns.Select(x => $"{x.Name} {x.TypeAsText()}"))});"
                                                                , connection))
                                                            {
                                                                command.Prepare();

                                                                command.ExecuteNonQuery();
                                                            }

                                                            writer = connection.BeginTextImport(
                                                                $"COPY {tableName} ({string.Join(",", names)}) FROM STDIN WITH NULL AS ''");

                                                            buildIndices = true;
                                                        }
                                                        else
                                                        {
                                                            using (var command = new NpgsqlCommand(string.Join(";",
                                                                    $"DROP TABLE IF EXISTS temp_{tableName}",
                                                                    $"CREATE TEMP TABLE temp_{tableName} ({string.Join(",", columns.Select(x => $"{x.Name} {x.TypeAsText()}"))})")
                                                                , connection))
                                                            {
                                                                command.Prepare();

                                                                command.ExecuteNonQuery();
                                                            }

                                                            writer = connection.BeginTextImport(
                                                                $"COPY temp_{tableName} ({string.Join(",", names)}) FROM STDIN WITH NULL AS ''");

                                                            insertFromTemp = true;
                                                        }
                                                    }

                                                    var values = columns.Select(x => x.ValueAsText(reader)).ToList();
                                                    writer.WriteLine(string.Join("\t", values));
                                                }

                                                writer?.Dispose();

                                                if (insertFromTemp)
                                                    using (var command = new NpgsqlCommand(string.Join(";",
                                                            $"INSERT INTO {tableName} ({string.Join(",", names)}) SELECT {string.Join(",", names)} FROM temp_{tableName} ON CONFLICT ({key}) DO UPDATE SET {string.Join(",", names.Select(x => $"{x}=EXCLUDED.{x}"))}, record_number=nextval('record_number_seq')",
                                                            $"DROP TABLE temp_{tableName}")
                                                        , connection))
                                                    {
                                                        command.Prepare();

                                                        command.ExecuteNonQuery();
                                                    }

                                                if (buildIndices)
                                                    BuildIndices(new[] {tableName}, connection);
                                            }
                                        }
                                }
                            }

                            await _progressHub.ProgressAsync(100f * uploadStream.Position / uploadStream.Length, id,
                                session);
                        }
                    }

                    foreach (var pair in _deleted) ExcludeDeleted(pair.Key, pair.Value, connection);

                    await _progressHub.ProgressAsync(100f, id, session);
                    await _progressHub.CompleteAsync(session);

                    await connection.CloseAsync();
                }
            }
            catch (Exception ex)
            {
                await _progressHub.ErrorAsync(ex.Message, session);
                throw;
            }
        }

        private Dictionary<Regex, Regex> RegionMasks(string region)
        {
            return new Dictionary<Regex, Regex>
            {
                {
                    new Regex(@"^addrob\d+$", RegexOptions.IgnoreCase),
                    new Regex($@"^addrob{region}\d*$", RegexOptions.IgnoreCase)
                },
                {
                    new Regex(@"^house\d+$", RegexOptions.IgnoreCase),
                    new Regex($@"^house{region}\d*$", RegexOptions.IgnoreCase)
                },
                {
                    new Regex(@"^room\d+$", RegexOptions.IgnoreCase),
                    new Regex($@"^room{region}\d*$", RegexOptions.IgnoreCase)
                },
                {
                    new Regex(@"^stead\d+$", RegexOptions.IgnoreCase),
                    new Regex($@"^stead{region}\d*$", RegexOptions.IgnoreCase)
                }
            };
        }

        private void DropTables(NpgsqlConnection conn)
        {
            var sqls = new[]
            {
                new[]
                {
                    "SELECT CONCAT('DROP TABLE ', table_name) FROM information_schema.tables WHERE table_schema = 'public'"
                }
            };

            SelectAndExecute(sqls, conn, GetFiasConnectionString());
        }

        private string FindKey(string tableName)
        {
            foreach (var mask in _masks)
                if (mask.Key.IsMatch(tableName))
                    return mask.Value;
            return null;
        }

        private void BuildIndices(string[] tableNames, NpgsqlConnection conn)
        {
            if (!tableNames.Any()) return;

            var sqls = new[]
            {
                new[]
                {
                    $@"SELECT CONCAT('ALTER TABLE ', table_name, ' ADD COLUMN record_number BIGINT DEFAULT nextval(''record_number_seq'');')
                    FROM information_schema.tables WHERE table_schema = 'public' AND table_name IN ({string.Join(",", tableNames.Select(x => $"'{x}'"))})",
                    $@"SELECT CONCAT('ALTER TABLE ', table_name, ' ADD COLUMN record_id BIGINT DEFAULT nextval(''record_id_seq'');')
                    FROM information_schema.tables WHERE table_schema = 'public' AND table_name IN ({string.Join(",", tableNames.Select(x => $"'{x}'"))})",
                    $@"SELECT CONCAT('ALTER TABLE ', table_name, ' ADD PRIMARY KEY (', column_name, ');')
                    FROM information_schema.columns WHERE table_schema = 'public' AND (CONCAT(table_name, 'id')=column_name
                    OR column_name IN ({string.Join(",", _masks.Values.Select(x => $"'{x}'"))}))
                    AND table_name IN ({string.Join(",", tableNames.Select(x => $"'{x}'"))})"
                },
                new[]
                {
                    $@"SELECT CONCAT('CREATE INDEX ON ', table_name, ' (', column_name, ');')
                    FROM information_schema.columns WHERE table_schema = 'public' AND column_name like ANY(ARRAY['%guid','%status','%level','%date','shortname','scname'])
                    AND table_name IN ({string.Join(",", tableNames.Select(x => $"'{x}'"))})",
                    $@"SELECT CONCAT('CREATE UNIQUE INDEX ON ', table_name, ' (', column_name, ');')
                    FROM information_schema.columns WHERE table_schema = 'public' AND column_name in ('record_number','record_id')
                    AND table_name IN ({string.Join(",", tableNames.Select(x => $"'{x}'"))})"
                }
            };

            SelectAndExecute(sqls, conn, GetFiasConnectionString());
        }


        private void ExcludeDeleted(string tableName, string key, NpgsqlConnection conn)
        {
            if (!TableIsExists($"d{tableName}", conn)) return;

            var sqls = new[]
            {
                new[]
                {
                    $@"SELECT CONCAT('DELETE FROM ', table_name, ' USING d{tableName} WHERE d{tableName}.{key}=', table_name, '.{key}')
                    FROM information_schema.tables WHERE table_schema='public' AND table_name LIKE '{tableName}%'"
                }
            };

            SelectAndExecute(sqls, conn, GetFiasConnectionString());
        }
    }
}