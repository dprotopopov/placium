using System;
using Itinero.LocalGeo;
using Npgsql;
using OsmSharp;
using OsmSharp.Streams;
using Placium.Common;
using Placium.Types;

namespace Placium.IO.Osm.PostgreSQL
{
    public class PostgresSQLDataSource : OsmStreamSource
    {
        private readonly NpgsqlCommand _command;
        private readonly NpgsqlConnection _connection;
        private readonly string _connectionString;
        private readonly Box _box;

        private readonly string _selectNode = @"SELECT 
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
        WHERE @lat1<=latitude
        AND latitude<=@lat2
        AND @lon1<=longitude
        AND longitude<=@lon2";

        private readonly string _selectRelation = @"SELECT 
        r.id,
        r.version,
        r.change_set_id,
        r.time_stamp,
        r.user_id,
        r.user_name,
        r.visible,
        r.tags,
        r.members
        FROM relation r JOIN (SELECT DISTINCT q.id 
        FROM (SELECT DISTINCT r2.id 
        FROM relation r2,unnest(r2.members) m2,way w2,node n2 
        WHERE m2.id=w2.id AND m2.type=2 AND n2.id=ANY(w2.nodes)
        AND @lat1<=n2.latitude
        AND n2.latitude<=@lat2
        AND @lon1<=n2.longitude
        AND n2.longitude<=@lon2
        UNION ALL SELECT DISTINCT r1.id 
        FROM relation r1,unnest(r1.members) m1,node n1 
        WHERE m1.id=n1.id AND m1.type=1
        AND @lat1<=n1.latitude
        AND n1.latitude<=@lat2
        AND @lon1<=n1.longitude
        AND n1.longitude<=@lon2) q) q1 ON r.id=q1.id";

        private readonly string _selectWay = @"SELECT
        w.id,
        w.version,
        w.change_set_id,
        w.time_stamp,
        w.user_id,
        w.user_name,
        w.visible,
        w.tags,
        w.nodes
        FROM way w JOIN (SELECT DISTINCT
        w1.id FROM way w1 JOIN node n ON n.id=ANY(w1.nodes)
        WHERE @lat1<=n.latitude
        AND n.latitude<=@lat2
        AND @lon1<=n.longitude
        AND n.longitude<=@lon2) AS q ON w.id=q.id";

        private int _nextResult;

        private NpgsqlDataReader _reader;

        public PostgresSQLDataSource(string connectionString, Box box)
        {
            _box = box;
            _connectionString = connectionString;
            _connection = new NpgsqlConnection(_connectionString);
            _connection.Open();
            _connection.ReloadTypes();
            _connection.TypeMapper.MapComposite<OsmRelationMember>("relation_member");
            _connection.TypeMapper.MapEnum<OsmType>("osm_type");

            _command = new NpgsqlCommand(string.Join(";", _selectNode, _selectWay, _selectRelation), _connection);
            _command.Parameters.AddWithValue("@lat1", _box.MinLat);
            _command.Parameters.AddWithValue("@lat2", _box.MaxLat);
            _command.Parameters.AddWithValue("@lon1", _box.MinLon);
            _command.Parameters.AddWithValue("@lon2", _box.MaxLon);
            _command.Prepare();

            _reader = _command.ExecuteReader();
            _nextResult = 0;
        }

        /// <summary>
        ///     Returns true if this stream can be reset.
        /// </summary>
        public override bool CanReset => true;

        public override bool MoveNext(bool ignoreNodes, bool ignoreWays, bool ignoreRelations)
        {
            for (;;)
            {
                if (ignoreNodes && _nextResult == 0)
                {
                    while (_reader.Read())
                    {
                    }

                    _reader.NextResult();
                    _nextResult++;
                }

                if (ignoreWays && _nextResult == 1)
                {
                    while (_reader.Read())
                    {
                    }

                    _reader.NextResult();
                    _nextResult++;
                }

                if (ignoreRelations && _nextResult == 2)
                {
                    while (_reader.Read())
                    {
                    }

                    _reader.NextResult();
                    _nextResult++;
                }

                if (_nextResult == 3) return false;
                if (_reader.Read()) return true;
                _reader.NextResult();
                _nextResult++;
            }
        }

        public override OsmGeo Current()
        {
            return _nextResult switch
                {
                0 => new Node().Fill(_reader),
                1 => new Way().Fill(_reader),
                2 => new Relation().Fill(_reader),
                _ => throw new NotImplementedException()
                };
        }

        public override void Reset()
        {
            _reader.Dispose();
            _reader = _command.ExecuteReader();
            _nextResult = 0;
        }
    }
}