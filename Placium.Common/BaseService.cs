using System.Collections.Generic;
using System.IO;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Npgsql;

namespace Placium.Common
{
    public class BaseService
    {
        private readonly IConfiguration _configuration;

        public BaseService(IConfiguration configuration)
        {
            _configuration = configuration;
        }

        protected string GetSphinxConnectionString()
        {
            return _configuration.GetConnectionString("SphinxConnection");
        }

        protected string GetFiasConnectionString()
        {
            return _configuration.GetConnectionString("FiasConnection");
        }

        protected string GetOsmConnectionString()
        {
            return _configuration.GetConnectionString("OsmConnection");
        }

        protected void SelectAndExecute(string[][] sqls, NpgsqlConnection conn, string connectionString)
        {
            foreach (var sql in sqls)
            {
                var cmds = new List<string>();

                cmds.Fill(string.Join("\nUNION ALL\n", sql), conn);

                Parallel.ForEach(cmds, new ParallelOptions
                {
                    MaxDegreeOfParallelism = 4
                }, cmd =>
                {
                    using (var connection = new NpgsqlConnection(connectionString))
                    {
                        connection.Open();
                        using (var command = new NpgsqlCommand(cmd, connection))
                        {
                            command.Prepare();

                            command.ExecuteNonQuery();
                        }

                        connection.Close();
                    }
                });
            }
        }

        protected async Task ExecuteResourceAsync(Assembly assembly, string resource, NpgsqlConnection connection)
        {
            using (var stream = assembly.GetManifestResourceStream(resource))
            using (var sr = new StreamReader(stream, Encoding.UTF8))
            using (var command = new NpgsqlCommand(await sr.ReadToEndAsync(), connection))
            {
                command.Prepare();

                command.ExecuteNonQuery();
            }
        }

        protected bool TableIsExists(string tableName, NpgsqlConnection conn)
        {
            using (var command = new NpgsqlCommand(
                $"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema='public' AND table_name='{tableName}');"
                , conn))
            {
                command.Prepare();

                return (bool) command.ExecuteScalar();
            }
        }

        protected long GetNextLastRecordNumber(NpgsqlConnection connection)
        {
            using (var command = new NpgsqlCommand(
                "SELECT last_value FROM record_number_seq"
                , connection))
            {
                command.Prepare();

                return (long)command.ExecuteScalar();
            }
        }

    }
}