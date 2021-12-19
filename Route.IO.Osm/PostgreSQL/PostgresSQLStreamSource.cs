using System;
using Npgsql;
using OsmSharp;
using OsmSharp.Streams;
using Placium.Common;
using Placium.Types;

namespace Route.IO.Osm.PostgreSQL
{
    public class PostgresSQLStreamSource : OsmStreamSource
    {
        private readonly NpgsqlCommand _command;
        private readonly NpgsqlConnection _connection;
        public string ConnectionString { get; }

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
        FROM node";

        private readonly string _selectRelation = @"SELECT 
        id,
        version,
        change_set_id,
        time_stamp,
        user_id,
        user_name,
        visible,
        tags,
        members
        FROM relation";

        private readonly string _selectWay = @"SELECT
        id,
        version,
        change_set_id,
        time_stamp,
        user_id,
        user_name,
        visible,
        tags,
        nodes
        FROM way";

        private int _nextResult;

        private NpgsqlDataReader _reader;

        public PostgresSQLStreamSource(string connectionString)
        {
            ConnectionString = connectionString;
            _connection = new NpgsqlConnection(ConnectionString);
            _connection.Open();
            _connection.ReloadTypes();
            _connection.TypeMapper.MapComposite<OsmRelationMember>("relation_member");
            _connection.TypeMapper.MapEnum<OsmType>("osm_type");

            _command = new NpgsqlCommand(string.Join(";", _selectNode, _selectWay, _selectRelation), _connection);
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