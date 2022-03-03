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
        protected BaseDatabaseAlgorithm(Guid guid, string connectionString, string profile)
        {
            Guid = guid;
            ConnectionString = connectionString;
            Profile = profile;
        }

        public string Profile { get; }

        public Guid Guid { get; }
        public string ConnectionString { get; }

        protected async Task ExecuteResourceAsync(Assembly assembly, string resource, NpgsqlConnection connection)
        {
            await using var stream = assembly.GetManifestResourceStream(resource);
            using var sr = new StreamReader(stream, Encoding.UTF8);
            await using var command = new NpgsqlCommand(await sr.ReadToEndAsync(), connection);
            await command.PrepareAsync();
            command.ExecuteNonQuery();
        }
    }
}