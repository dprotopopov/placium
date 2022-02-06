using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Npgsql;
using OsmSharp;
using Placium.Common;
using Placium.Types;

namespace Placium.Services;

public class OsmService : BaseApiService
{
    private readonly string _selectNodeById = @"SELECT 
        id,
        version,
        latitude,
        longitude,
        change_set_id,
        time_stamp,
        user_id,
        user_name,
        visible,
        tags
        FROM node
        WHERE id=@p";

    private readonly string _selectRelationById = @"SELECT 
        id,
        version,
        change_set_id,
        time_stamp,
        user_id,
        user_name,
        visible,
        tags,
        members
        FROM relation
        WHERE id=@p";

    private readonly string _selectWayById = @"SELECT 
        id,
        version,
        change_set_id,
        time_stamp,
        user_id,
        user_name,
        visible,
        tags,
        nodes
        FROM way
        WHERE id=@p";

    public OsmService(IConfiguration configuration) : base(configuration)
    {
    }

    public async Task<OsmGeo> GetByIdAsync(long osm_id, OsmType type)
    {
        await using var connection = new NpgsqlConnection(GetOsmConnectionString());
        await connection.OpenAsync();

        connection.ReloadTypes();
        connection.TypeMapper.MapComposite<OsmRelationMember>("relation_member");
        connection.TypeMapper.MapEnum<OsmType>("osm_type");
        connection.TypeMapper.MapEnum<OsmServiceType>("service_type");

        switch (type)
        {
            case OsmType.Node:
                await using (var command = new NpgsqlCommand(_selectNodeById, connection))
                {
                    command.Parameters.AddWithValue("p", osm_id);

                    await command.PrepareAsync();

                    await using var reader = command.ExecuteReader();
                    if (reader.Read())
                        return new Node().Fill(reader);
                }

                break;
            case OsmType.Way:
                await using (var command = new NpgsqlCommand(_selectWayById, connection))
                {
                    command.Parameters.AddWithValue("p", osm_id);

                    await command.PrepareAsync();

                    await using var reader = command.ExecuteReader();
                    if (reader.Read())
                        return new Way().Fill(reader);
                }

                break;
            case OsmType.Relation:
                await using (var command = new NpgsqlCommand(_selectRelationById, connection))
                {
                    command.Parameters.AddWithValue("p", osm_id);

                    await command.PrepareAsync();

                    await using var reader = command.ExecuteReader();
                    if (reader.Read())
                        return new Relation().Fill(reader);
                }

                break;
        }

        return null;
    }
}