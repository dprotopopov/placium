using System;
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
                    "CREATE TABLE addrx(title text,priority int)"
                }, connection);

                await UpdateAddrxAsync(connection, session, full);
            }
        }


        private async Task UpdateAddrxAsync(MySqlConnection connection, string session, bool full)
        {
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
                    "SELECT COUNT(*) FROM addrx WHERE record_number>@last_record_number";

                var sql =
                    "SELECT id,tags FROM addrx WHERE record_number>@last_record_number";

                using (var command = new NpgsqlCommand(string.Join(";", sql1, sql), npgsqlConnection))
                {
                    command.Parameters.AddWithValue("last_record_number", last_record_number);

                    command.Prepare();

                    using (var reader = command.ExecuteReader())
                    {
                        if (reader.Read())
                            total = reader.GetInt64(0);

                        var take = 10000;

                        reader.NextResult();

                        while (true)
                        {
                            var docs = reader.ReadDocs3(take);

                            if (docs.Any())
                            {
                                var sb = new StringBuilder("REPLACE INTO addrx(id,title,priority) VALUES ");
                                sb.Append(string.Join(",",
                                    docs.Select(x => $"({x.id},'{x.text.TextEscape()}',{x.priority})")));

                                ExecuteNonQueryWithRepeatOnError(sb.ToString(), connection);

                                current += docs.Count;

                                await _progressHub.ProgressAsync(100f * current / total, id, session);
                            }

                            if (docs.Count < take) break;
                        }
                    }
                }

                SetLastRecordNumber(npgsqlConnection, OsmServiceType.Addrx, next_last_record_number);

                await npgsqlConnection.CloseAsync();

                await _progressHub.ProgressAsync(100f, id, session);
            }
        }
    }
}