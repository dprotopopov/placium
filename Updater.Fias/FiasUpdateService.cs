using System;
using System.Collections.Generic;
using System.Reflection;
using System.Threading.Tasks;
using Npgsql;
using Placium.Common;
using Placium.Types;

namespace Updater.Fias
{
    public class FiasUpdateService : BaseAppService, IUpdateService
    {
        private readonly IProgressClient _progressClient;

        public FiasUpdateService(IProgressClient progressClient, IConnectionsConfig configuration) : base(configuration)
        {
            _progressClient = progressClient;
        }

        public async Task UpdateAsync(string session, bool full)
        {
            using (var npgsqlConnection = new NpgsqlConnection(GetFiasConnectionString()))
            {
                await npgsqlConnection.OpenAsync();
                if (full)
                    await ExecuteResourceAsync(Assembly.GetExecutingAssembly(), "Updater.Fias.CreateAddrxTables.sql",
                        npgsqlConnection);

                ExecuteNonQueries(new[]
                {
                    new[]
                    {
                        "DROP INDEX IF EXISTS addrx_title_idx"
                    }
                }, npgsqlConnection);
                await npgsqlConnection.CloseAsync();
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
            using (var npgsqlConnection2 = new NpgsqlConnection(GetFiasConnectionString()))
            using (var npgsqlConnection = new NpgsqlConnection(GetOsmConnectionString()))
            {
                var current = 0L;
                var total = 0L;

                var id = Guid.NewGuid().ToString();
                await _progressClient.Init(id, session);

                await npgsqlConnection2.OpenAsync();
                await npgsqlConnection.OpenAsync();

                npgsqlConnection.ReloadTypes();
                npgsqlConnection.TypeMapper.MapEnum<OsmServiceType>("service_type");

                var last_record_number = GetLastRecordNumber(npgsqlConnection, OsmServiceType.Addrx, full);
                var next_last_record_number = GetNextLastRecordNumber(npgsqlConnection);

                await ExecuteResourceAsync(Assembly.GetExecutingAssembly(), "Updater.Fias.CreateAddrxTempTables.sql",
                    npgsqlConnection2);

                using (var writer = npgsqlConnection2.BeginTextImport(
                    "COPY temp_addrx (id,title,priority,lon,lat,housenumber,building) FROM STDIN WITH NULL AS ''"))
                {
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
                                var docs = ReadDocs3(reader, take);

                                foreach (var doc in docs)
                                {
                                    var values = new[]
                                    {
                                        doc.id.ToString(),
                                        doc.text.ValueAsText(),
                                        doc.priority.ToString(),
                                        doc.lon.ValueAsText(),
                                        doc.lat.ValueAsText(),
                                        doc.housenumber.ValueAsText(),
                                        doc.building.ToString()
                                    };

                                    writer.WriteLine(string.Join("\t", values));
                                }

                                current += docs.Count;

                                await _progressClient.Progress(100f * current / total, id, session);

                                if (docs.Count < take) break;
                            }
                        }
                    }
                }

                await ExecuteResourceAsync(Assembly.GetExecutingAssembly(),
                    "Updater.Fias.InsertAddrxFromTempTables.sql",
                    npgsqlConnection2);


                SetLastRecordNumber(npgsqlConnection, OsmServiceType.Addrx, next_last_record_number);

                await npgsqlConnection.CloseAsync();
                await npgsqlConnection2.CloseAsync();

                await _progressClient.Finalize(id, session);
            }
        }

        public List<Doc3> ReadDocs3(NpgsqlDataReader reader, int take)
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
                "addr:street"
            };

            var result = new List<Doc3>(take);
            for (var i = 0; i < take && reader.Read(); i++)
            {
                var dictionary = (Dictionary<string, string>) reader.GetValue(1);

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

                var doc = new Doc3
                {
                    id = reader.GetInt64(0),
                    text = string.Join(", ", list),
                    priority = priority,
                    housenumber = dictionary.ContainsKey("addr:housenumber")
                        ? dictionary["addr:housenumber"]
                        : string.Empty,
                    building = dictionary.ContainsKey("addr:housenumber") ? 1 : 0,
                    lon = reader.SafeGetDouble(2) ?? 0,
                    lat = reader.SafeGetDouble(3) ?? 0
                };

                result.Add(doc);
            }

            return result;
        }
    }
}