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
        private readonly NumberFormatInfo _nfi = new NumberFormatInfo {NumberDecimalSeparator = "."};
        private readonly IProgressClient _progressClient;

        public SphinxAddrxUpdateService(IProgressClient progressClient, IConnectionsConfig configuration) : base(
            configuration)
        {
            _progressClient = progressClient;
        }

        public async Task UpdateAsync(string session, bool full)
        {
            using (var connection = new MySqlConnection(GetSphinxConnectionString()))
            {
                if (full)
                    TryExecuteNonQueries(new[]
                    {
                        "DROP TABLE addrx"
                    }, connection);

                TryExecuteNonQueries(new[]
                {
                    "CREATE TABLE addrx(title text indexed stored,priority int,lon float,lat float,building int)"
                    + " phrase_boundary='U+2C'"
                    + " phrase_boundary_step='100'"
                    + " min_infix_len='1'"
                    + " expand_keywords='1'"
                    + " charset_table='0..9,A..Z->a..z,a..z,U+410..U+42F->U+430..U+44F,U+430..U+44F,U+401->U+0435,U+451->U+0435'"
                    + " morphology='stem_ru'"
                }, connection);
            }

            if (full)
                using (var npgsqlConnection = new NpgsqlConnection(GetOsmConnectionString()))
                {
                    await npgsqlConnection.OpenAsync();

                    npgsqlConnection.ReloadTypes();
                    npgsqlConnection.TypeMapper.MapEnum<OsmServiceType>("service_type");

                    SetLastRecordNumber(npgsqlConnection, OsmServiceType.Addrx, 0);

                    await npgsqlConnection.CloseAsync();
                }

            await UpdateAddrxAsync(session, full);
        }


        private async Task UpdateAddrxAsync(string session, bool full)
        {
            using var mySqlConnection = new MySqlConnection(GetSphinxConnectionString());
            using var npgsqlConnection = new NpgsqlConnection(GetOsmConnectionString());
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

            using (var command = new NpgsqlCommand(string.Join(";", sql1, sql), npgsqlConnection))
            {
                command.Parameters.AddWithValue("last_record_number", last_record_number);

                command.Prepare();

                using var reader = command.ExecuteReader();
                if (reader.Read())
                    total = reader.GetInt64(0);

                var take = 1000;

                reader.NextResult();

                while (true)
                {
                    var docs = ReadDocs(reader, take);


                    if (docs.Any())
                    {
                        var sb = new StringBuilder(
                            "REPLACE INTO addrx(id,title,priority,lon,lat,building) VALUES ");
                        sb.Append(string.Join(",",
                            docs.Select(x =>
                                $"({x.id},'{x.text.TextEscape()}',{x.priority},{x.lon.ToString(_nfi)},{x.lat.ToString(_nfi)},{x.building})")));

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
                "addr:postcode",
                "addr:region",
                "addr:district",
                "addr:city",
                "addr:town",
                "addr:village",
                "addr:subdistrict",
                "addr:suburb",
                "addr:hamlet",
                "addr:allotments",
                "addr:isolated_dwelling",
                "addr:neighbourhood",
                "addr:locality",
                "addr:place",
                "addr:quarter",
                "addr:island",
                "addr:islet",
                "addr:street",
                "addr:housenumber"
            };

            var result = new List<Doc>(take);
            for (var i = 0; i < take && reader.Read(); i++)
            {
                var dictionary = reader.GetValue(1) as Dictionary<string, string> ?? new Dictionary<string, string>();

                var priority = keys.Length;
                for (; priority > 0 && !dictionary.ContainsKey(keys[priority - 1]); priority--) ;

                var list = new List<string>(priority);

                var skipCity = dictionary.ContainsKey("addr:region") && dictionary.ContainsKey("addr:city") &&
                               dictionary["addr:region"] == dictionary["addr:city"];

                var skipTown = dictionary.ContainsKey("addr:city") && dictionary.ContainsKey("addr:town") &&
                               dictionary["addr:city"] == dictionary["addr:town"];

                var skipVillage = dictionary.ContainsKey("addr:city") && dictionary.ContainsKey("addr:village") &&
                                  dictionary["addr:city"] == dictionary["addr:village"];

                for (var k = 0; k < priority; k++)
                {
                    var key = keys[k];
                    if (dictionary.ContainsKey(key) && (key != "addr:city" || !skipCity) &&
                        (key != "addr:town" || !skipTown) &&
                        (key != "addr:village" || !skipVillage))
                        list.Add(dictionary[key]);
                }

                var doc = new Doc
                {
                    id = reader.GetInt64(0),
                    text = string.Join(", ", list),
                    priority = priority,
                    building = dictionary.ContainsKey("addr:housenumber") ? 1 : 0,
                    lon = reader.SafeGetFloat(2) ?? 0,
                    lat = reader.SafeGetFloat(3) ?? 0
                };

                result.Add(doc);
            }

            return result;
        }
    }
}