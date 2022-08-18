using Newtonsoft.Json;
using Placium.Common;
using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using System.Xml;
using System.Xml.Serialization;

namespace Loader.Gar.File
{
    public class FileGarUploadService : BaseAppService, IUploadService
    {
        private readonly Dictionary<string, Type> _files = new Dictionary<string, Type>()
        {
            { @"^(AS_ADD_OBJ)_\d{8}_[0-9A-F]{8}[-]?([0-9A-F]{4}[-]?){3}[0-9A-F]{12}\.XML$", typeof(AS_ADDR_OBJ_2_251_01_04_01_01.xsd.ADDRESSOBJECTSOBJECT) },
            { @"^(AS_ADD_OBJ_DIVISION)_\d{8}_[0-9A-F]{8}[-]?([0-9A-F]{4}[-]?){3}[0-9A-F]{12}\.XML$", typeof(AS_ADDR_OBJ_DIVISION_2_251_19_04_01_01.xsd.ITEMSITEM) },
            { @"^(AS_ADD_OBJ_TYPES)_\d{8}_[0-9A-F]{8}[-]?([0-9A-F]{4}[-]?){3}[0-9A-F]{12}\.XML$", typeof(AS_ADDR_OBJ_TYPES_2_251_03_04_01_01.xsd.ADDRESSOBJECTTYPESADDRESSOBJECTTYPE) },
            { @"^(AS_ADM_HIERARCHY)_\d{8}_[0-9A-F]{8}[-]?([0-9A-F]{4}[-]?){3}[0-9A-F]{12}\.XML$", typeof(AS_ADM_HIERARCHY_2_251_04_04_01_01.xsd.ITEMSITEM) },
            { @"^(AS_APARTMENT_TYPES)_\d{8}_[0-9A-F]{8}[-]?([0-9A-F]{4}[-]?){3}[0-9A-F]{12}\.XML$", typeof(AS_APARTMENT_TYPES_2_251_07_04_01_01.xsd.APARTMENTTYPESAPARTMENTTYPE) },
            { @"^(AS_APARTMENTS)_\d{8}_[0-9A-F]{8}[-]?([0-9A-F]{4}[-]?){3}[0-9A-F]{12}\.XML$", typeof(AS_APARTMENTS_2_251_05_04_01_01.xsd.APARTMENTSAPARTMENT) },
            { @"^(AS_CARPLACES)_\d{8}_[0-9A-F]{8}[-]?([0-9A-F]{4}[-]?){3}[0-9A-F]{12}\.XML$", typeof(AS_CARPLACES_2_251_06_04_01_01.xsd.CARPLACESCARPLACE) },
            { @"^(AS_CHANGE_HISTORY)_\d{8}_[0-9A-F]{8}[-]?([0-9A-F]{4}[-]?){3}[0-9A-F]{12}\.XML$", typeof(AS_CHANGE_HISTORY_251_21_04_01_01.xsd.ITEMSITEM) },
            { @"^(AS_HOUSE_TYPES)_\d{8}_[0-9A-F]{8}[-]?([0-9A-F]{4}[-]?){3}[0-9A-F]{12}\.XML$", typeof(AS_HOUSE_TYPES_2_251_13_04_01_01.xsd.HOUSETYPESHOUSETYPE) },
            { @"^(AS_HOUSES)_\d{8}_[0-9A-F]{8}[-]?([0-9A-F]{4}[-]?){3}[0-9A-F]{12}\.XML$", typeof(AS_HOUSES_2_251_08_04_01_01.xsd.HOUSESHOUSE) },
            { @"^(AS_MUN_HIERARCHY)_\d{8}_[0-9A-F]{8}[-]?([0-9A-F]{4}[-]?){3}[0-9A-F]{12}\.XML$", typeof(AS_MUN_HIERARCHY_2_251_10_04_01_01.xsd.ITEMSITEM) },
            { @"^(AS_NORMATIVE_DOCS)_\d{8}_[0-9A-F]{8}[-]?([0-9A-F]{4}[-]?){3}[0-9A-F]{12}\.XML$", typeof(AS_NORMATIVE_DOCS_2_251_11_04_01_01.xsd.NORMDOCSNORMDOC) },
            { @"^(AS_NORMATIVE_DOCS_KINDS)_\d{8}_[0-9A-F]{8}[-]?([0-9A-F]{4}[-]?){3}[0-9A-F]{12}\.XML$", typeof(AS_NORMATIVE_DOCS_KINDS_2_251_09_04_01_01.xsd.NDOCKIND) },
            { @"^(AS_NORMATIVE_DOCS_TYPES)_\d{8}_[0-9A-F]{8}[-]?([0-9A-F]{4}[-]?){3}[0-9A-F]{12}\.XML$", typeof(AS_NORMATIVE_DOCS_TYPES_2_251_16_04_01_01.xsd.NDOCTYPE) },
            { @"^(AS_OBJECT_LEVELS)_\d{8}_[0-9A-F]{8}[-]?([0-9A-F]{4}[-]?){3}[0-9A-F]{12}\.XML$", typeof(AS_OBJECT_LEVELS_2_251_12_04_01_01.xsd.OBJECTLEVELSOBJECTLEVEL) },
            { @"^(AS_OPERATION_TYPES)_\d{8}_[0-9A-F]{8}[-]?([0-9A-F]{4}[-]?){3}[0-9A-F]{12}\.XML$", typeof(AS_OPERATION_TYPES_2_251_14_04_01_01.xsd.OPERATIONTYPESOPERATIONTYPE) },
            { @"^(AS_PARAM)_\d{8}_[0-9A-F]{8}[-]?([0-9A-F]{4}[-]?){3}[0-9A-F]{12}\.XML$", typeof(AS_PARAM_2_251_02_04_01_01.xsd.PARAMSPARAM) },
            { @"^(AS_PARAM_TYPES)_\d{8}_[0-9A-F]{8}[-]?([0-9A-F]{4}[-]?){3}[0-9A-F]{12}\.XML$", typeof(AS_PARAM_TYPES_2_251_20_04_01_01.xsd.PARAMTYPESPARAMTYPE) },
            { @"^(AS_REESTR_OBJECTS)_\d{8}_[0-9A-F]{8}[-]?([0-9A-F]{4}[-]?){3}[0-9A-F]{12}\.XML$", typeof(AS_REESTR_OBJECTS_2_251_22_04_01_01.xsd.REESTR_OBJECTSOBJECT) },
            { @"^(AS_ROOM_TYPES)_\d{8}_[0-9A-F]{8}[-]?([0-9A-F]{4}[-]?){3}[0-9A-F]{12}\.XML$", typeof(AS_ROOM_TYPES_2_251_17_04_01_01.xsd.ROOMTYPESROOMTYPE) },
            { @"^(AS_ROOMS)_\d{8}_[0-9A-F]{8}[-]?([0-9A-F]{4}[-]?){3}[0-9A-F]{12}\.XML$", typeof(AS_ROOMS_2_251_15_04_01_01.xsd.ROOMSROOM) },
            { @"^(AS_STEADS)_\d{8}_[0-9A-F]{8}[-]?([0-9A-F]{4}[-]?){3}[0-9A-F]{12}\.XML$", typeof(AS_STEADS_2_251_18_04_01_01.xsd.STEADSSTEAD) },
        };

        public FileGarUploadService(IConnectionsConfig configuration) : base(configuration)
        {
        }

        private List<string> GetCreateTableSqls()
        {
            var list = new List<string>();
            foreach (var (key, value) in _files)
            {
                var match = Regex.Match(key, @"\(([^\)]+)\)", RegexOptions.IgnoreCase);
                var tableName = match.Groups[1].Value.ToUpper();
                var sb = new StringBuilder($"CREATE TABLE {tableName} (");
                var columns = new List<string>();
                foreach(var property in value.GetProperties())
                {
                    var name = property.Name;
                    if (property.PropertyType == typeof(int))
                        columns.Add($"{name} INTEGER");
                    if (property.PropertyType == typeof(long))
                        columns.Add($"{name} BIGINT");
                    else if (property.PropertyType == typeof(string))
                        columns.Add($"{name} VARCHAR(256)");
                    else if (property.PropertyType == typeof(DateTime))
                        columns.Add($"{name} TIMESTAMP");
                    else if (property.PropertyType == typeof(bool))
                        columns.Add($"{name} BOOLEAN");
                    else if (property.PropertyType.IsEnum)
                        columns.Add($"{name} INTEGER");
                }
                sb.Append(string.Join(",", columns));
                sb.Append(")");
                list.Add(sb.ToString()); 
            }
            return list;
        }

        public async Task InstallAsync(Stream uploadStream, Dictionary<string, string> options, string session)
        {
            var sqls = GetCreateTableSqls();
            Console.WriteLine(string.Join(Environment.NewLine, sqls));

            using (var archive = new ZipArchive(uploadStream))
            {
                foreach (var entry in archive.Entries)
                {
                    Type type = null;
                    string tableName = null;

                    foreach (var file in _files)
                    {
                        var match = Regex.Match(entry.Name, file.Key, RegexOptions.IgnoreCase);
                        if (match.Success)
                        {
                            type = file.Value;
                            tableName = match.Groups[1].Value.ToUpper();
                            break;
                        }
                    }
                    if (type != null)
                    {
                        await using var stream = entry.Open();
                        XmlReaderSettings settings = new XmlReaderSettings();
                        settings.IgnoreWhitespace = true;
                        using var reader = XmlReader.Create(stream, settings);
                        reader.MoveToContent();
                        reader.Read();
                        var serializer = new XmlSerializer(type, new XmlRootAttribute(reader.Name));

                        while (true)
                        {
                            var xml = reader.ReadOuterXml();
                            if (string.IsNullOrEmpty(xml)) break;
                            try
                            {
                                var obj = serializer.Deserialize(new MemoryStream(System.Text.Encoding.UTF8.GetBytes(xml)));
                                Console.WriteLine(JsonConvert.SerializeObject(obj));
                            } 
                            catch(Exception ex)
                            {
                                Console.WriteLine(ex.ToString());
                                throw;
                            }
                        }
                    }
                }
            }
        }
         
        public async Task UpdateAsync(Stream uploadStream, Dictionary<string, string> options, string session)
        {
            throw new NotImplementedException();
        }
    }
}
