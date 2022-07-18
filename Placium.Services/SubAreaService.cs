using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Npgsql;
using Placium.Common;
using Placium.Types;

namespace Placium.Services
{
    public class SubAreaService : BaseApiService
    {
        private readonly ILogger<SubAreaService> _logger;

        public SubAreaService(IConfiguration configuration, ILogger<SubAreaService> logger) : base(configuration)
        {
            _logger = logger;
        }

        public async Task<string> GetSubArea(List<string> osmIds, int level)
        {
            await using var connection = new NpgsqlConnection(GetOsmConnectionString());
            await connection.OpenAsync();

            connection.ReloadTypes();
            connection.TypeMapper.MapComposite<OsmRelationMember>("relation_member");

            var types = osmIds.Select(x =>
                x.StartsWith("R", StringComparison.InvariantCultureIgnoreCase) ? "relation" :
                x.StartsWith("W", StringComparison.InvariantCultureIgnoreCase) ? "way" :
                x.StartsWith("N", StringComparison.InvariantCultureIgnoreCase) ? "node" :
                throw new ArgumentException()).ToArray();
            var ids = osmIds.Select(x => long.Parse(x[1..])).ToArray();

            for (var i = 0; i < level; i++)
            {
                var result = new List<OsmRelationMember>(osmIds.Count);

                await using (var command =
                             new NpgsqlCommand(
                                 @"SELECT id,members
                             FROM (SELECT r.id,r.members FROM relation r
                             JOIN (SELECT unnest(@types::osm_type[]) AS type, unnest(@ids) AS id) t
                             ON t.type='relation' AND r.id=t.id) q",
                                 connection))
                {
                    command.Parameters.AddWithValue("types", types);
                    command.Parameters.AddWithValue("ids", ids);

                    await command.PrepareAsync();

                    await using var reader = command.ExecuteReader();

                    while (reader.Read())
                    {
                        var members = (OsmRelationMember[])reader.GetValue(1);
                        result.AddRange(members.Where(x => x.Role == "subarea"));
                    }

                    ids = result.Select(x => x.Id).ToArray();
                    types = result.Select(x => x.Type.ToOsmType()).ToArray();
                }
            }

            await connection.CloseAsync();

            return string.Join(",", types.Zip(ids, (s, l) => s[..1].ToUpper() + l));
        }
    }
}