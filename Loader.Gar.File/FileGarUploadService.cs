using Npgsql;
using Placium.Common;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using System.Xml;
using System.Xml.Serialization;

namespace Loader.Gar.File
{
    public class FileGarUploadService : BaseAppService, IUploadService
    {
        private readonly IProgressClient _progressClient;
        private readonly IParallelConfig _parallelConfig;

        private readonly Dictionary<string, Tuple<string, Type>> _files = new Dictionary<string, Tuple<string, Type>>()
        {
            { @"^(AS_ADDR_OBJ)_\d{8}_[0-9A-F]{8}[-]?([0-9A-F]{4}[-]?){3}[0-9A-F]{12}\.XML$", Tuple.Create("ID",typeof(AS_ADDR_OBJ_2_251_01_04_01_01.xsd.ADDRESSOBJECTSOBJECT)) },
            { @"^(AS_ADDR_OBJ_DIVISION)_\d{8}_[0-9A-F]{8}[-]?([0-9A-F]{4}[-]?){3}[0-9A-F]{12}\.XML$", Tuple.Create("ID",typeof(AS_ADDR_OBJ_DIVISION_2_251_19_04_01_01.xsd.ITEMSITEM)) },
            { @"^(AS_ADDR_OBJ_TYPES)_\d{8}_[0-9A-F]{8}[-]?([0-9A-F]{4}[-]?){3}[0-9A-F]{12}\.XML$", Tuple.Create("ID",typeof(AS_ADDR_OBJ_TYPES_2_251_03_04_01_01.xsd.ADDRESSOBJECTTYPESADDRESSOBJECTTYPE)) },
            { @"^(AS_ADM_HIERARCHY)_\d{8}_[0-9A-F]{8}[-]?([0-9A-F]{4}[-]?){3}[0-9A-F]{12}\.XML$", Tuple.Create("ID",typeof(AS_ADM_HIERARCHY_2_251_04_04_01_01.xsd.ITEMSITEM)) },
            { @"^(AS_APARTMENT_TYPES)_\d{8}_[0-9A-F]{8}[-]?([0-9A-F]{4}[-]?){3}[0-9A-F]{12}\.XML$", Tuple.Create("ID",typeof(AS_APARTMENT_TYPES_2_251_07_04_01_01.xsd.APARTMENTTYPESAPARTMENTTYPE)) },
            { @"^(AS_APARTMENTS)_\d{8}_[0-9A-F]{8}[-]?([0-9A-F]{4}[-]?){3}[0-9A-F]{12}\.XML$", Tuple.Create("ID",typeof(AS_APARTMENTS_2_251_05_04_01_01.xsd.APARTMENTSAPARTMENT)) },
            { @"^(AS_CARPLACES)_\d{8}_[0-9A-F]{8}[-]?([0-9A-F]{4}[-]?){3}[0-9A-F]{12}\.XML$", Tuple.Create("ID",typeof(AS_CARPLACES_2_251_06_04_01_01.xsd.CARPLACESCARPLACE)) },
            { @"^(AS_CHANGE_HISTORY)_\d{8}_[0-9A-F]{8}[-]?([0-9A-F]{4}[-]?){3}[0-9A-F]{12}\.XML$", Tuple.Create(nameof(AS_CHANGE_HISTORY_251_21_04_01_01.xsd.ITEMSITEM.CHANGEID),typeof(AS_CHANGE_HISTORY_251_21_04_01_01.xsd.ITEMSITEM)) },
            { @"^(AS_HOUSE_TYPES)_\d{8}_[0-9A-F]{8}[-]?([0-9A-F]{4}[-]?){3}[0-9A-F]{12}\.XML$", Tuple.Create("ID",typeof(AS_HOUSE_TYPES_2_251_13_04_01_01.xsd.HOUSETYPESHOUSETYPE)) },
            { @"^(AS_HOUSES)_\d{8}_[0-9A-F]{8}[-]?([0-9A-F]{4}[-]?){3}[0-9A-F]{12}\.XML$", Tuple.Create("ID",typeof(AS_HOUSES_2_251_08_04_01_01.xsd.HOUSESHOUSE)) },
            { @"^(AS_MUN_HIERARCHY)_\d{8}_[0-9A-F]{8}[-]?([0-9A-F]{4}[-]?){3}[0-9A-F]{12}\.XML$", Tuple.Create("ID",typeof(AS_MUN_HIERARCHY_2_251_10_04_01_01.xsd.ITEMSITEM)) },
            { @"^(AS_NORMATIVE_DOCS)_\d{8}_[0-9A-F]{8}[-]?([0-9A-F]{4}[-]?){3}[0-9A-F]{12}\.XML$", Tuple.Create("ID",typeof(AS_NORMATIVE_DOCS_2_251_11_04_01_01.xsd.NORMDOCSNORMDOC)) },
            { @"^(AS_NORMATIVE_DOCS_KINDS)_\d{8}_[0-9A-F]{8}[-]?([0-9A-F]{4}[-]?){3}[0-9A-F]{12}\.XML$", Tuple.Create("ID",typeof(AS_NORMATIVE_DOCS_KINDS_2_251_09_04_01_01.xsd.NDOCKIND)) },
            { @"^(AS_NORMATIVE_DOCS_TYPES)_\d{8}_[0-9A-F]{8}[-]?([0-9A-F]{4}[-]?){3}[0-9A-F]{12}\.XML$", Tuple.Create("ID",typeof(AS_NORMATIVE_DOCS_TYPES_2_251_16_04_01_01.xsd.NDOCTYPE)) },
            { @"^(AS_OBJECT_LEVELS)_\d{8}_[0-9A-F]{8}[-]?([0-9A-F]{4}[-]?){3}[0-9A-F]{12}\.XML$", Tuple.Create(nameof(AS_OBJECT_LEVELS_2_251_12_04_01_01.xsd.OBJECTLEVELSOBJECTLEVEL.LEVEL),typeof(AS_OBJECT_LEVELS_2_251_12_04_01_01.xsd.OBJECTLEVELSOBJECTLEVEL)) },
            { @"^(AS_OPERATION_TYPES)_\d{8}_[0-9A-F]{8}[-]?([0-9A-F]{4}[-]?){3}[0-9A-F]{12}\.XML$", Tuple.Create("ID",typeof(AS_OPERATION_TYPES_2_251_14_04_01_01.xsd.OPERATIONTYPESOPERATIONTYPE)) },
            { @"^(AS_PARAM_TYPES)_\d{8}_[0-9A-F]{8}[-]?([0-9A-F]{4}[-]?){3}[0-9A-F]{12}\.XML$", Tuple.Create("ID",typeof(AS_PARAM_TYPES_2_251_20_04_01_01.xsd.PARAMTYPESPARAMTYPE)) },
            { @"^(AS_REESTR_OBJECTS)_\d{8}_[0-9A-F]{8}[-]?([0-9A-F]{4}[-]?){3}[0-9A-F]{12}\.XML$", Tuple.Create(nameof(AS_REESTR_OBJECTS_2_251_22_04_01_01.xsd.REESTR_OBJECTSOBJECT.OBJECTID),typeof(AS_REESTR_OBJECTS_2_251_22_04_01_01.xsd.REESTR_OBJECTSOBJECT)) },
            { @"^(AS_ROOM_TYPES)_\d{8}_[0-9A-F]{8}[-]?([0-9A-F]{4}[-]?){3}[0-9A-F]{12}\.XML$", Tuple.Create("ID",typeof(AS_ROOM_TYPES_2_251_17_04_01_01.xsd.ROOMTYPESROOMTYPE)) },
            { @"^(AS_ROOMS)_\d{8}_[0-9A-F]{8}[-]?([0-9A-F]{4}[-]?){3}[0-9A-F]{12}\.XML$", Tuple.Create("ID",typeof(AS_ROOMS_2_251_15_04_01_01.xsd.ROOMSROOM)) },
            { @"^(AS_STEADS)_\d{8}_[0-9A-F]{8}[-]?([0-9A-F]{4}[-]?){3}[0-9A-F]{12}\.XML$", Tuple.Create("ID",typeof(AS_STEADS_2_251_18_04_01_01.xsd.STEADSSTEAD)) },
            { @"^(AS_ADDR_OBJ_PARAMS)_\d{8}_[0-9A-F]{8}[-]?([0-9A-F]{4}[-]?){3}[0-9A-F]{12}\.XML$", Tuple.Create("ID",typeof(AS_PARAM_2_251_02_04_01_01.xsd.PARAMSPARAM)) },
            { @"^(AS_APARTMENTS_PARAMS)_\d{8}_[0-9A-F]{8}[-]?([0-9A-F]{4}[-]?){3}[0-9A-F]{12}\.XML$", Tuple.Create("ID",typeof(AS_PARAM_2_251_02_04_01_01.xsd.PARAMSPARAM)) },
            { @"^(AS_HOUSES_PARAMS)_\d{8}_[0-9A-F]{8}[-]?([0-9A-F]{4}[-]?){3}[0-9A-F]{12}\.XML$", Tuple.Create("ID",typeof(AS_PARAM_2_251_02_04_01_01.xsd.PARAMSPARAM)) },
            { @"^(AS_STEADS_PARAMS)_\d{8}_[0-9A-F]{8}[-]?([0-9A-F]{4}[-]?){3}[0-9A-F]{12}\.XML$", Tuple.Create("ID",typeof(AS_PARAM_2_251_02_04_01_01.xsd.PARAMSPARAM)) },
            { @"^(AS_ROOMS_PARAMS)_\d{8}_[0-9A-F]{8}[-]?([0-9A-F]{4}[-]?){3}[0-9A-F]{12}\.XML$", Tuple.Create("ID",typeof(AS_PARAM_2_251_02_04_01_01.xsd.PARAMSPARAM)) },
            { @"^(AS_CARPLACES_PARAMS)_\d{8}_[0-9A-F]{8}[-]?([0-9A-F]{4}[-]?){3}[0-9A-F]{12}\.XML$", Tuple.Create("ID",typeof(AS_PARAM_2_251_02_04_01_01.xsd.PARAMSPARAM)) },
        };

        public FileGarUploadService(IConnectionsConfig configuration, IProgressClient progressClient, IParallelConfig parallelConfig) : base(configuration)
        {
            _progressClient = progressClient;
            _parallelConfig = parallelConfig;
        }

        private Dictionary<string, string> GetCreateTableSqls(bool temp = false)
        {
            var dic = new Dictionary<string, string>();
            foreach (var (key, value) in _files)
            {
                var match = Regex.Match(key, @"\(([^\)]+)\)", RegexOptions.IgnoreCase);
                var tableName = match.Groups[1].Value.ToUpper();
                var sb = new StringBuilder($@"CREATE TABLE ""{(temp ? "temp_" : "")}{tableName}"" (");
                var columns = new List<string>();
                var properties = value.Item2.GetProperties();
                foreach (var property in properties)
                {
                    var name = property.Name;

                    if (property.PropertyType == typeof(bool) && name.EndsWith("Specified")) continue;

                    var col = new List<string>(3);
                    col.Add($@"""{name}""");

                    if (property.PropertyType == typeof(int))
                        col.Add("INTEGER");
                    if (property.PropertyType == typeof(long))
                        col.Add("BIGINT");
                    else if (property.PropertyType == typeof(string) && (
                        name.EndsWith("ID") ||
                        name.EndsWith("LEVEL") ||
                        name.EndsWith("CODE") ||
                        name.EndsWith("TYPE") ||
                        name.EndsWith("TYPE1") ||
                        name.EndsWith("TYPE2") ||
                        name.EndsWith("NUM") ||
                        name.EndsWith("NUM1") ||
                        name.EndsWith("NUM2") ||
                        name.EndsWith("NUMBER") ||
                        name.EndsWith("STATUS") ||
                        name.EndsWith("DATE") ||
                        name.EndsWith("SCNAME") ||
                        name.EndsWith("SHORTNAME")))
                        col.Add("VARCHAR(255)");
                    else if (property.PropertyType == typeof(string))
                        col.Add("TEXT");
                    else if (property.PropertyType == typeof(DateTime))
                        col.Add("TIMESTAMP");
                    else if (property.PropertyType == typeof(bool))
                        col.Add("BOOLEAN");
                    else if (property.PropertyType.IsEnum)
                        col.Add("INTEGER");

                    if (properties.Any(x => x.Name == $"{name}Specified"))
                        col.Add("NULL");
                    else if (property.PropertyType != typeof(string))
                        col.Add("NOT NULL");

                    columns.Add(string.Join(" ", col));
                }
                sb.Append(string.Join(",", columns));
                sb.Append(")");
                dic.Add(tableName, sb.ToString());
            }
            return dic;
        }

        public async Task InstallAsync(Stream uploadStream, Dictionary<string, string> options, string session)
        {
            var region = options.ContainsKey("region") ? options["region"] : string.Empty;

            await using var connection = new NpgsqlConnection(GetGarConnectionString());
            await connection.OpenAsync();

            var id = Guid.NewGuid().ToString();
            await _progressClient.Init(id, session);

            DropTables(connection);
            DropTempTables(connection);

            await ExecuteResourceAsync(Assembly.GetExecutingAssembly(), "Loader.Gar.File.CreateSequence.pgsql",
                connection);

            var sqls = GetCreateTableSqls();
            var sqlsTemp = GetCreateTableSqls(temp: true);

            ExecuteNonQueries(new[] { sqls.Values.ToArray() }, connection);
            ExecuteNonQueries(new[] { sqlsTemp.Values.ToArray() }, connection);

            using (var archive = new ZipArchive(uploadStream))
            {
                foreach (var entry in archive.Entries)
                {
                    if (string.IsNullOrEmpty(region) || entry.FullName == $"{region}/{entry.Name}" || entry.FullName == entry.Name)
                    {
                        Type type = null;
                        string tableName = null;
                        TextWriter writer = null;

                        foreach (var (key, value) in _files)
                        {
                            var match = Regex.Match(entry.Name, key, RegexOptions.IgnoreCase);
                            if (match.Success)
                            {
                                type = value.Item2;
                                tableName = match.Groups[1].Value.ToUpper();
                                var properties = value.Item2.GetProperties();

                                var columns = new List<string>();
                                foreach (var property in properties)
                                {
                                    var name = property.Name;

                                    if (property.PropertyType == typeof(bool) && name.EndsWith("Specified")) continue;

                                    columns.Add($@"""{name}""");
                                }
                                writer = await connection.BeginTextImportAsync($@"COPY ""temp_{tableName}"" ({string.Join(",", columns)}) FROM STDIN WITH NULL AS ''");
                                break;
                            }
                        }

                        if (type != null)
                        {
                            var properties = type.GetProperties();

                            await using var stream = entry.Open();
                            XmlReaderSettings settings = new XmlReaderSettings();
                            settings.IgnoreWhitespace = true;
                            using var reader = XmlReader.Create(stream, settings);
                            reader.MoveToContent();
                            reader.Read();
                            var serializer = new XmlSerializer(type, new XmlRootAttribute(reader.Name));

                            var lockObj = new object();
                            var lockObj1 = new object();
                            var doIt = true;

                            Parallel.For(0, _parallelConfig.GetNumberOfThreads(), i =>
                            {
                                for (; ; )
                                {
                                    string xml = null;
                                    lock (lockObj)
                                    {
                                        if (!doIt) break;
                                        xml = reader.ReadOuterXml();
                                        if (string.IsNullOrEmpty(xml))
                                        {
                                            doIt = false;
                                            break;
                                        }
                                    }

                                    var obj = serializer.Deserialize(new MemoryStream(Encoding.UTF8.GetBytes(xml)));
                                    var values = new List<string>();
                                    foreach (var property in properties)
                                    {
                                        var name = property.Name;

                                        if (property.PropertyType == typeof(bool) && name.EndsWith("Specified")) continue;

                                        var specified = properties.FirstOrDefault(x => x.Name == $"{name}Specified");
                                        var value = property.GetValue(obj);

                                        if (specified != null && !(bool)specified.GetValue(obj) || value == null)
                                        {
                                            values.Add(string.Empty);
                                            continue;
                                        }

                                        if (property.PropertyType == typeof(int))
                                            values.Add(value.ToString());
                                        if (property.PropertyType == typeof(long))
                                            values.Add(value.ToString());
                                        else if (property.PropertyType == typeof(string))
                                            values.Add(value.ToString().TextEscape());
                                        else if (property.PropertyType == typeof(DateTime))
                                            values.Add(((DateTime)value).ToString("u", CultureInfo.InvariantCulture));
                                        else if (property.PropertyType == typeof(bool))
                                            values.Add((bool)value ? "true" : "false");
                                        else if (property.PropertyType.IsEnum)
                                            values.Add(((int)value).ToString());
                                    }

                                    lock (lockObj1) writer.WriteLine(string.Join("\t", values));
                                }
                            });

                            writer.Dispose();
                        }
                    }

                    await _progressClient.Progress(100f * uploadStream.Position / uploadStream.Length, id, session);
                }
            }

            var sqls1 = new List<string>();
            foreach (var (key, value) in _files)
            {
                var match = Regex.Match(key, @"\(([^\)]+)\)", RegexOptions.IgnoreCase);
                var tableName = match.Groups[1].Value.ToUpper();
                var properties = value.Item2.GetProperties();

                sqls1.Add($@"CREATE INDEX ON ""temp_{tableName}"" (""{value.Item1}"")");
            }

            ExecuteNonQueries(new[] { sqls1.ToArray() }, connection);

            var sqlsCopy = new List<string>();

            foreach (var (key, value) in _files)
            {
                var match = Regex.Match(key, @"\(([^\)]+)\)", RegexOptions.IgnoreCase);
                var tableName = match.Groups[1].Value.ToUpper();
                var properties = value.Item2.GetProperties();

                var columns = new List<string>();
                foreach (var property in properties)
                {
                    var name = property.Name;

                    if (property.PropertyType == typeof(bool) && name.EndsWith("Specified")) continue;

                    columns.Add($@"""{name}""");
                }
                var sb = new StringBuilder();
                sb.AppendLine($@"INSERT INTO ""{tableName}""({string.Join(",", columns)})");
                sb.AppendLine($@"SELECT DISTINCT ON (""{value.Item1}"") {string.Join(", ", columns)} FROM ""temp_{tableName}""");
                sqlsCopy.Add(sb.ToString());
            }

            ExecuteNonQueries(new[] { sqlsCopy.ToArray() }, connection);

            DropTempTables(connection);

            var sqls2 = new List<string>();

            foreach (var (key, value) in _files)
            {
                var match = Regex.Match(key, @"\(([^\)]+)\)", RegexOptions.IgnoreCase);
                var tableName = match.Groups[1].Value.ToUpper();
                var properties = value.Item2.GetProperties();

                foreach (var property in properties)
                {
                    var name = property.Name;

                    if (property.PropertyType == typeof(bool) && name.EndsWith("Specified")) continue;

                    if (name == value.Item1) 
                        sqls2.Add($@"ALTER TABLE ""{tableName}"" ADD PRIMARY KEY (""{name}"")");
                    else if (name.EndsWith("ID") ||
                        name.EndsWith("LEVEL") ||
                        name.EndsWith("CODE") ||
                        name.EndsWith("TYPE") ||
                        name.EndsWith("TYPE1") ||
                        name.EndsWith("TYPE2") ||
                        name.EndsWith("NUM") ||
                        name.EndsWith("NUM1") ||
                        name.EndsWith("NUM2") ||
                        name.EndsWith("NUMBER") ||
                        name.EndsWith("STATUS") ||
                        name.EndsWith("DATE") ||
                        name.EndsWith("SCNAME") ||
                        name.EndsWith("SHORTNAME") ||
                        property.PropertyType == typeof(bool) ||
                        property.PropertyType == typeof(int) ||
                        property.PropertyType == typeof(long) ||
                        property.PropertyType == typeof(DateTime))
                        sqls2.Add($@"CREATE INDEX ON ""{tableName}"" (""{name}"")");
                }
            }

            ExecuteNonQueries(new[] { sqls2.ToArray() }, connection);

            BuildIndices(sqls.Keys.ToArray(), connection);

            await _progressClient.Finalize(id, session);

            await connection.CloseAsync();
        }

        public async Task UpdateAsync(Stream uploadStream, Dictionary<string, string> options, string session)
        {
            var region = options.ContainsKey("region") ? options["region"] : string.Empty;

            await using var connection = new NpgsqlConnection(GetGarConnectionString());
            await connection.OpenAsync();

            var id = Guid.NewGuid().ToString();
            await _progressClient.Init(id, session);

            DropTempTables(connection);

            var sqlsTemp = GetCreateTableSqls(temp: true);

            ExecuteNonQueries(new[] { sqlsTemp.Values.ToArray() }, connection);

            using (var archive = new ZipArchive(uploadStream))
            {
                foreach (var entry in archive.Entries)
                {
                    if (string.IsNullOrEmpty(region) || entry.FullName == $"{region}/{entry.Name}" || entry.FullName == entry.Name)
                    {
                        Type type = null;
                        string tableName = null;
                        TextWriter writer = null;

                        foreach (var (key, value) in _files)
                        {
                            var match = Regex.Match(entry.Name, key, RegexOptions.IgnoreCase);
                            if (match.Success)
                            {
                                type = value.Item2;
                                tableName = match.Groups[1].Value.ToUpper();
                                var properties = value.Item2.GetProperties();

                                var columns = new List<string>();
                                foreach (var property in properties)
                                {
                                    var name = property.Name;

                                    if (property.PropertyType == typeof(bool) && name.EndsWith("Specified")) continue;

                                    columns.Add($@"""{name}""");
                                }
                                writer = await connection.BeginTextImportAsync($@"COPY ""temp_{tableName}"" ({string.Join(",", columns)}) FROM STDIN WITH NULL AS ''");
                                break;
                            }
                        }

                        if (type != null)
                        {
                            var properties = type.GetProperties();

                            await using var stream = entry.Open();
                            XmlReaderSettings settings = new XmlReaderSettings();
                            settings.IgnoreWhitespace = true;
                            using var reader = XmlReader.Create(stream, settings);
                            reader.MoveToContent();
                            reader.Read();
                            var serializer = new XmlSerializer(type, new XmlRootAttribute(reader.Name));

                            var lockObj = new object();
                            var lockObj1 = new object();
                            var doIt = true;

                            Parallel.For(0, _parallelConfig.GetNumberOfThreads(), i =>
                            {
                                for (; ; )
                                {
                                    string xml = null;
                                    lock (lockObj)
                                    {
                                        if (!doIt) break;
                                        xml = reader.ReadOuterXml();
                                        if (string.IsNullOrEmpty(xml))
                                        {
                                            doIt = false;
                                            break;
                                        }
                                    }

                                    var obj = serializer.Deserialize(new MemoryStream(Encoding.UTF8.GetBytes(xml)));
                                    var values = new List<string>();
                                    foreach (var property in properties)
                                    {
                                        var name = property.Name;

                                        if (property.PropertyType == typeof(bool) && name.EndsWith("Specified")) continue;

                                        var specified = properties.FirstOrDefault(x => x.Name == $"{name}Specified");
                                        var value = property.GetValue(obj);

                                        if (specified != null && !(bool)specified.GetValue(obj) || value == null)
                                        {
                                            values.Add(string.Empty);
                                            continue;
                                        }

                                        if (property.PropertyType == typeof(int))
                                            values.Add(value.ToString());
                                        if (property.PropertyType == typeof(long))
                                            values.Add(value.ToString());
                                        else if (property.PropertyType == typeof(string))
                                            values.Add(value.ToString().TextEscape());
                                        else if (property.PropertyType == typeof(DateTime))
                                            values.Add(((DateTime)value).ToString("u", CultureInfo.InvariantCulture));
                                        else if (property.PropertyType == typeof(bool))
                                            values.Add((bool)value ? "true" : "false");
                                        else if (property.PropertyType.IsEnum)
                                            values.Add(((int)value).ToString());
                                    }

                                    lock (lockObj1) writer.WriteLine(string.Join("\t", values));
                                }
                            });
                        }

                        writer.Dispose();
                    }

                    await _progressClient.Progress(100f * uploadStream.Position / uploadStream.Length, id, session);
                }
            }

            var sqls1 = new List<string>();
            foreach (var (key, value) in _files)
            {
                var match = Regex.Match(key, @"\(([^\)]+)\)", RegexOptions.IgnoreCase);
                var tableName = match.Groups[1].Value.ToUpper();
                var properties = value.Item2.GetProperties();

                sqls1.Add($@"CREATE INDEX ON ""temp_{tableName}"" (""{value.Item1}"")");
            }

            ExecuteNonQueries(new[] { sqls1.ToArray() }, connection);

            var sqlsCopy = new List<string>();

            foreach (var (key, value) in _files)
            {
                var match = Regex.Match(key, @"\(([^\)]+)\)", RegexOptions.IgnoreCase);
                var tableName = match.Groups[1].Value.ToUpper();
                var properties = value.Item2.GetProperties();

                var columns = new List<string>();
                foreach (var property in properties)
                {
                    var name = property.Name;

                    if (property.PropertyType == typeof(bool) && name.EndsWith("Specified")) continue;

                    columns.Add($@"""{name}""");
                }
                var sb = new StringBuilder();
                sb.AppendLine($@"INSERT INTO ""{tableName}""({string.Join(",", columns)})");
                sb.AppendLine($@"SELECT DISTINCT ON (""{value.Item1}"") {string.Join(", ", columns)} FROM ""temp_{tableName}""");
                sb.AppendLine($@"ON CONFLICT (""{value.Item1}"") DO UPDATE SET");
                columns.ForEach(x => { if (x != $@"""{value.Item1}""") sb.AppendLine($@"{x}=EXCLUDED.{x},"); });
                sb.AppendLine($@"record_number=nextval('record_number_seq')");
                sqlsCopy.Add(sb.ToString());
            }

            ExecuteNonQueries(new[] { sqlsCopy.ToArray() }, connection);

            DropTempTables(connection);

            await _progressClient.Finalize(id, session);

            await connection.CloseAsync();
        }

        private void DropTables(NpgsqlConnection conn)
        {
            var sqls = new[]
            {
                new[]
                {
                    @"SELECT CONCAT('DROP TABLE ""', table_name, '""') FROM information_schema.tables WHERE table_schema = 'public'"
                }
            };

            SelectAndExecute(sqls, conn, GetGarConnectionString());
        }
        private void DropTempTables(NpgsqlConnection conn)
        {
            var sqls = new[]
            {
                new[]
                {
                    @"SELECT CONCAT('DROP TABLE ""', table_name, '""') FROM information_schema.tables WHERE table_schema = 'public' AND table_name like 'temp_%'"
                }
            };

            SelectAndExecute(sqls, conn, GetGarConnectionString());
        }

        private void BuildIndices(string[] tableNames, NpgsqlConnection conn)
        {
            if (!tableNames.Any()) return;

            var sqls = new[]
            {
                new[]
                {
                    $@"SELECT CONCAT('ALTER TABLE ""', table_name, '"" ADD COLUMN record_number BIGINT DEFAULT nextval(''record_number_seq'');')
                    FROM information_schema.tables WHERE table_schema = 'public' AND table_name IN ({string.Join(",", tableNames.Select(x => $"'{x}'"))})",
                    $@"SELECT CONCAT('ALTER TABLE ""', table_name, '"" ADD COLUMN record_id BIGINT DEFAULT nextval(''record_id_seq'');')
                    FROM information_schema.tables WHERE table_schema = 'public' AND table_name IN ({string.Join(",", tableNames.Select(x => $"'{x}'"))})",
                }
            };

            SelectAndExecute(sqls, conn, GetGarConnectionString());
        }

    }
}
