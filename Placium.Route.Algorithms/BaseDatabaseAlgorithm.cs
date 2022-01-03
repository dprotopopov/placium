using System;
using System.IO;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Npgsql;

namespace Placium.Route.Algorithms
{
    public abstract class BaseDatabaseAlgorithm : IDatabaseAlgorithm
    {
        public BaseDatabaseAlgorithm(Guid guid, string connectionString, string profile)
        {
            Guid = guid;
            ConnectionString = connectionString;
            Profile = profile;
        }

        public Guid Guid { get; }
        public string ConnectionString { get; }
        public string Profile { get; }

        protected async Task ExecuteResourceAsync(Assembly assembly, string resource, NpgsqlConnection connection)
        {
            using var stream = assembly.GetManifestResourceStream(resource);
            using var sr = new StreamReader(stream, Encoding.UTF8);
            using var command = new NpgsqlCommand(await sr.ReadToEndAsync(), connection);
            command.Prepare();
            command.ExecuteNonQuery();
        }
    }
}