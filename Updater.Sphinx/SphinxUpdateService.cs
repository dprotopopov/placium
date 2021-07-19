using System;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using MySql.Data.MySqlClient;
using Npgsql;
using Placium.Common;
using Placium.Types;

namespace Updater.Sphinx
{
    public class SphinxUpdateService : BaseService, IUpdateService
    {
        private readonly NumberFormatInfo _nfi = new NumberFormatInfo {NumberDecimalSeparator = "."};
        private readonly ProgressHub _progressHub;

        public SphinxUpdateService(ProgressHub progressHub, IConfiguration configuration) : base(configuration)
        {
            _progressHub = progressHub;
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
                    "CREATE TABLE addrx(title text,priority int,lon float,lat float,building int)"
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
            using (var mySqlConnection = new MySqlConnection(GetSphinxConnectionString()))
            using (var npgsqlConnection = new NpgsqlConnection(GetOsmConnectionString()))
            {
                var current = 0L;
                var total = 0L;

                var id = Guid.NewGuid().ToString();
                await _progressHub.InitAsync(id, session);

                await npgsqlConnection.OpenAsync();

                npgsqlConnection.ReloadTypes();
                npgsqlConnection.TypeMapper.MapEnum<OsmServiceType>("service_type");

                var last_record_number = GetLastRecordNumber(npgsqlConnection, OsmServiceType.Addrx, full);
                var next_last_record_number = GetNextLastRecordNumber(npgsqlConnection);

                var sql1 =
                    "SELECT COUNT(*) FROM addrx join placex on addrx.id=placex.id WHERE addrx.record_number>@last_record_number";

                var sql =
                    "SELECT addrx.id,addrx.tags,ST_X(ST_Centroid(placex.location)),ST_Y(ST_Centroid(placex.location)) FROM addrx join placex on addrx.id=placex.id WHERE addrx.record_number>@last_record_number";

                using (var command = new NpgsqlCommand(string.Join(";", sql1, sql), npgsqlConnection))
                {
                    command.Parameters.AddWithValue("last_record_number", last_record_number);

                    command.Prepare();

                    using (var reader = command.ExecuteReader())
                    {
                        if (reader.Read())
                            total = reader.GetInt64(0);

                        var take = 1000;

                        reader.NextResult();

                        while (true)
                        {
                            var docs = reader.ReadDocs3(take);


                            if (docs.Any())
                            {
                                var sb = new StringBuilder(
                                    "REPLACE INTO addrx(id,title,priority,lon,lat,building) VALUES ");
                                sb.Append(string.Join(",",
                                    docs.Select(x =>
                                        $"({x.id},'{x.text.TextEscape()}',{x.priority},{x.lon.ToString(_nfi)},{x.lat.ToString(_nfi)},{(x.building ? 1 : 0)})")));

                                ExecuteNonQueryWithRepeatOnError(sb.ToString(), mySqlConnection);
                            }

                            current += docs.Count;

                            await _progressHub.ProgressAsync(100f * current / total, id, session);

                            if (docs.Count < take) break;
                        }
                    }
                }

                SetLastRecordNumber(npgsqlConnection, OsmServiceType.Addrx, next_last_record_number);

                await npgsqlConnection.CloseAsync();
                mySqlConnection.TryClose();

                await _progressHub.ProgressAsync(100f, id, session);
            }
        }
    }
}