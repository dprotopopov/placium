using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MySql.Data.MySqlClient;
using Npgsql;
using Placium.Common;
using Placium.Types;

namespace Updater.Addrx.Sphinx
{
    public class SphinxAddrxUpdateService : BaseAppService, IUpdateService
    {
        private readonly NumberFormatInfo _nfi = new NumberFormatInfo { NumberDecimalSeparator = "." };
        private readonly IProgressClient _progressClient;

        public SphinxAddrxUpdateService(IProgressClient progressClient, IConnectionsConfig configuration) : base(
            configuration)
        {
            _progressClient = progressClient;
        }

        public async Task UpdateAsync(string session, bool full)
        {
            await using (var connection = new MySqlConnection(GetSphinxConnectionString()))
            {
                if (full)
                    TryExecuteNonQueries(new[]
                    {
                        "DROP TABLE addrx"
                    }, connection);

                TryExecuteNonQueries(new[]
                {
                    "CREATE TABLE addrx(title text indexed stored,custom_title text indexed stored,custom_level int,priority int,lon float,lat float,building int,data json)"
                    + " phrase_boundary='U+2C'"
                    + " phrase_boundary_step='100'"
                    + " min_infix_len='1'"
                    + " expand_keywords='1'"
                    + " charset_table='0..9,A..Z->a..z,a..z,U+410..U+42F->U+430..U+44F,U+430..U+44F,U+401->U+0435,U+451->U+0435'"
                    + " morphology='stem_ru'"
                }, connection);
            }

            if (full)
            {
                await using var npgsqlConnection = new NpgsqlConnection(GetOsmConnectionString());
                await npgsqlConnection.OpenAsync();

                npgsqlConnection.ReloadTypes();
                npgsqlConnection.TypeMapper.MapEnum<OsmServiceType>("service_type");

                SetLastRecordNumber(npgsqlConnection, OsmServiceType.Addrx, 0);

                await npgsqlConnection.CloseAsync();
            }

            await UpdateAddrxAsync(session, full);

            await using (var connection = new MySqlConnection(GetSphinxConnectionString()))
            {
                TryExecuteNonQueries(new[]
                {
                    "FLUSH RTINDEX addrx"
                }, connection);
            }
        }


        private async Task UpdateAddrxAsync(string session, bool full)
        {
            await using var mySqlConnection = new MySqlConnection(GetSphinxConnectionString());
            await using var npgsqlConnection = new NpgsqlConnection(GetOsmConnectionString());
            var current = 0L;
            var total = 0L;

            var id = Guid.NewGuid().ToString();
            await _progressClient.Init(id, session);

            await npgsqlConnection.OpenAsync();

            npgsqlConnection.ReloadTypes();
            npgsqlConnection.TypeMapper.MapEnum<OsmServiceType>("service_type");

            var last_record_number = GetLastRecordNumber(npgsqlConnection, OsmServiceType.Addrx, full);
            var next_last_record_number = GetNextLastRecordNumber(npgsqlConnection);

            var sql1 =
                "SELECT COUNT(*) FROM addrx join placex on addrx.id=placex.id WHERE addrx.record_number>@last_record_number";

            var sql =
                "SELECT addrx.id,addrx.tags,ST_X(ST_Centroid(placex.location))::real,ST_Y(ST_Centroid(placex.location))::real FROM addrx join placex on addrx.id=placex.id WHERE addrx.record_number>@last_record_number";

            await using (var command = new NpgsqlCommand(string.Join(";", sql1, sql), npgsqlConnection))
            {
                command.Parameters.AddWithValue("last_record_number", last_record_number);

                await command.PrepareAsync();

                await using var reader = command.ExecuteReader();
                if (reader.Read())
                    total = reader.GetInt64(0);

                var take = 1000;

                reader.NextResult();

                for (;;)
                {
                    var docs = ReadDocs(reader, take);

                    if (docs.Any())
                    {
                        var sb = new StringBuilder(
                            "REPLACE INTO addrx(id,title,custom_title,custom_level,priority,lon,lat,building,data) VALUES ");
                        sb.Append(string.Join(",",
                            docs.Select(x =>
                                $"({x.id},'{x.text.TextEscape()}','{x.custom_text.TextEscape()}',{x.custom_level},{x.priority},{x.lon.ToString(_nfi)},{x.lat.ToString(_nfi)},{x.building},'{{{string.Join(",", x.data.Select(t => $"\"{t.Key.TextEscape(2)}\":\"{t.Value.TextEscape(2)}\""))}}}')")));

                        ExecuteNonQueryWithRepeatOnError(sb.ToString(), mySqlConnection);
                    }

                    current += docs.Count;

                    await _progressClient.Progress(100f * current / total, id, session);

                    if (docs.Count < take) break;
                }
            }

            SetLastRecordNumber(npgsqlConnection, OsmServiceType.Addrx, next_last_record_number);

            await npgsqlConnection.CloseAsync();
            mySqlConnection.TryClose();

            await _progressClient.Finalize(id, session);
        }

        public List<Doc> ReadDocs(NpgsqlDataReader reader, int take)
        {
            var keys = new[]
            {
                new KeyValuePair<string, string>("addr:postcode", "{0}"),
                new KeyValuePair<string, string>("addr:region", "{0}"),
                new KeyValuePair<string, string>("addr:peninsula", "{0}"),
                new KeyValuePair<string, string>("addr:district", "{0}"),
                new KeyValuePair<string, string>("addr:city", "{0}"),
                new KeyValuePair<string, string>("addr:town", "{0}"),
                new KeyValuePair<string, string>("addr:village", "{0}"),
                new KeyValuePair<string, string>("addr:municipality", "{0}"),
                new KeyValuePair<string, string>("addr:subdistrict", "{0}"),
                new KeyValuePair<string, string>("addr:landuse", "{0}"),
                new KeyValuePair<string, string>("addr:suburb", "{0}"),
                new KeyValuePair<string, string>("addr:hamlet", "{0}"),
                new KeyValuePair<string, string>("addr:allotments", "{0}"),
                new KeyValuePair<string, string>("addr:isolated_dwelling", "{0}"),
                new KeyValuePair<string, string>("addr:neighbourhood", "{0}"),
                new KeyValuePair<string, string>("addr:locality", "{0}"),
                new KeyValuePair<string, string>("addr:place", "{0}"),
                new KeyValuePair<string, string>("addr:quarter", "{0}"),
                new KeyValuePair<string, string>("addr:island", "{0}"),
                new KeyValuePair<string, string>("addr:islet", "{0}"),
                new KeyValuePair<string, string>("addr:highway:yes", "{0}"),
                new KeyValuePair<string, string>("addr:highway:motorway", "{0}"),
                new KeyValuePair<string, string>("addr:highway:service", "{0}"),
                new KeyValuePair<string, string>("addr:highway:track", "{0}"),
                new KeyValuePair<string, string>("addr:highway:trunk", "{0}"),
                new KeyValuePair<string, string>("addr:highway:primary", "{0}"),
                new KeyValuePair<string, string>("addr:highway:secondary", "{0}"),
                new KeyValuePair<string, string>("addr:highway:tertiary", "{0}"),
                new KeyValuePair<string, string>("addr:highway:unclassified", "{0}"),
                new KeyValuePair<string, string>("addr:highway:residential", "{0}"),
                new KeyValuePair<string, string>("addr:highway:footway", "{0}"),
                new KeyValuePair<string, string>("addr:highway:cycleway", "{0}"),
                new KeyValuePair<string, string>("addr:highway:path", "{0}"),
                new KeyValuePair<string, string>("addr:highway:road", "{0}"),
                new KeyValuePair<string, string>("addr:highway:living_street", "{0}"),
                new KeyValuePair<string, string>("addr:highway:pedestrian", "{0}"),
                new KeyValuePair<string, string>("addr:highway:construction", "{0}"),
                new KeyValuePair<string, string>("addr:highway:proposed", "{0}"),
                new KeyValuePair<string, string>("addr:highway:raceway", "{0}"),
                new KeyValuePair<string, string>("addr:highway:elevator", "{0}"),
                new KeyValuePair<string, string>("addr:highway:corridor", "{0}"),
                new KeyValuePair<string, string>("addr:highway:services", "{0}"),
                new KeyValuePair<string, string>("addr:highway:steps", "{0}"),
                new KeyValuePair<string, string>("addr:highway:motorway_link", "{0}"),
                new KeyValuePair<string, string>("addr:highway:service_link", "{0}"),
                new KeyValuePair<string, string>("addr:highway:track_link", "{0}"),
                new KeyValuePair<string, string>("addr:highway:trunk_link", "{0}"),
                new KeyValuePair<string, string>("addr:highway:primary_link", "{0}"),
                new KeyValuePair<string, string>("addr:highway:secondary_link", "{0}"),
                new KeyValuePair<string, string>("addr:highway:tertiary_link", "{0}"),
                new KeyValuePair<string, string>("addr:railway:rail", "ж/д {0}"),
                new KeyValuePair<string, string>("addr:railway:tram", "т/п {0}"),
                new KeyValuePair<string, string>("addr:railway:subway", "метро {0}"),
                new KeyValuePair<string, string>("addr:railway:monorail", "монорельс {0}"),
                new KeyValuePair<string, string>("addr:railway:funicular", "фуникулёр {0}"),
                new KeyValuePair<string, string>("addr:railway:razed", "{0}"),
                new KeyValuePair<string, string>("addr:railway:construction", "{0}"),
                new KeyValuePair<string, string>("addr:railway:preserved", "{0}"),
                new KeyValuePair<string, string>("addr:railway:proposed", "{0}"),
                new KeyValuePair<string, string>("addr:railway:yard", "{0}"),
                new KeyValuePair<string, string>("addr:railway:service_station", "{0}"),
                new KeyValuePair<string, string>("addr:bridge", "мост {0}"),
                new KeyValuePair<string, string>("addr:tunnel", "туннель {0}"),
                new KeyValuePair<string, string>("addr:street", "{0}"),
                new KeyValuePair<string, string>("addr:square", "{0}"),
                new KeyValuePair<string, string>("addr:housenumber", "{0}"),
                new KeyValuePair<string, string>("addr:building", "{0}"),
                new KeyValuePair<string, string>("addr:station", "станция {0}"),
                new KeyValuePair<string, string>("addr:platform", "платформа {0}"),
                new KeyValuePair<string, string>("addr:highway:stop", "остановка {0}"),
                new KeyValuePair<string, string>("addr:railway:stop", "остановка {0}"),
                new KeyValuePair<string, string>("addr:highway:bus_stop", "остановка {0}"),
                new KeyValuePair<string, string>("addr:railway:tram_stop", "остановка {0}"),
                new KeyValuePair<string, string>("addr:railway:halt", "остановка по требованию {0}"),
                new KeyValuePair<string, string>("addr:highway:halt", "остановка по требованию {0}"),
                new KeyValuePair<string, string>("addr:railway:subway_entrance", "станция {0}"),
                new KeyValuePair<string, string>("addr:railway:station", "станция {0}"),
                new KeyValuePair<string, string>("addr:highway:station", "станция {0}"),
                new KeyValuePair<string, string>("addr:railway:platform", "платформа {0}"),
                new KeyValuePair<string, string>("addr:highway:platform", "платформа {0}"),
                new KeyValuePair<string, string>("addr:man_made", "{0}"),
                new KeyValuePair<string, string>("addr:natural", "{0}"),
                new KeyValuePair<string, string>("addr:shop", "{0}")
            };


            var result = new List<Doc>(take);
            for (var i = 0; i < take && reader.Read(); i++)
            {
                var dictionary = reader.GetValue(1) as Dictionary<string, string> ?? new Dictionary<string, string>();

                var priority = keys.Length;
                for (; priority > 0 && !dictionary.ContainsKey(keys[priority - 1].Key); priority--) ;

                var list = new List<string>(priority);

                foreach (var (key, format) in keys)
                    if (dictionary.TryGetValue(key, out var s) && !string.IsNullOrEmpty(s))
                    {
                        dictionary.TryAdd(key.Replace(":", "_"), s);
                        var item = string.Format(format, s);
                        if (!list.Contains(item, StringComparer.InvariantCultureIgnoreCase))
                            list.Add(item);
                    }

                var list1 = new List<string>();
                if (dictionary.ContainsKey("building") && dictionary.TryGetValue("addr:street", out var street))
                    list1.Add(street);
                if (dictionary.ContainsKey("building") && dictionary.TryGetValue("addr:square", out var square))
                    list1.Add(square);
                if (dictionary.ContainsKey("building") &&
                    dictionary.TryGetValue("addr:housenumber", out var housenumber)) list1.Add(housenumber);
                if (dictionary.TryGetValue("name", out var name)) list1.Add(name);

                var doc = new Doc
                {
                    id = reader.GetInt64(0),
                    text = string.Join(", ", list),
                    custom_text = string.Join(", ", list1),
                    custom_level = dictionary.ContainsKey("highway")
                                   || dictionary.ContainsKey("railway")
                                   || dictionary.ContainsKey("building")
                                   || dictionary.ContainsKey("addr:housenumber")
                                   || dictionary.ContainsKey("addr:building")
                                   || dictionary.ContainsKey("addr:man_made")
                                   || dictionary.ContainsKey("addr:natural")
                                   || dictionary.ContainsKey("addr:shop")
                        ? 1
                        : 0,
                    priority = priority,
                    building = dictionary.ContainsKey("building")
                        ? 1
                        : 0,
                    lon = reader.SafeGetFloat(2) ?? 0,
                    lat = reader.SafeGetFloat(3) ?? 0,
                    data = dictionary
                };

                result.Add(doc);
            }

            return result;
        }
    }
}