using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using MySql.Data.MySqlClient;
using Placium.Common;

namespace Placium.Seeker
{
    public class GarAddressService : BaseApiService
    {
        private readonly ILogger _logger;

        public GarAddressService(IConfiguration configuration, ILogger<GarAddressService> logger) : base(
            configuration)
        {
            _logger = logger;
        }


        public async Task<IEnumerable<AddressEntry>> GetByNameAsync(string searchString, int limit = 20)
        {
            var result = new List<AddressEntry>();

            if (string.IsNullOrEmpty(searchString)) return result;


            var list = searchString.Split(",").ToList();

            var match = list.ToMatch();

            await using var mySqlConnection = new MySqlConnection(GetSphinxConnectionString());
            var skip = 0;
            var take = 20;
            while (limit > 0)
            {
                mySqlConnection.TryOpen();

                await using var command =
                    new MySqlCommand(
                        @"SELECT title,objectid,objectguid FROM garx WHERE MATCH(@match) ORDER BY priority ASC LIMIT @skip,@take",
                        mySqlConnection);
                command.Parameters.AddWithValue("skip", skip);
                command.Parameters.AddWithValue("take", take);
                command.Parameters.AddWithValue("match", match);

                await using var reader = command.ExecuteReader();
                var count = 0;
                while (limit > 0 && reader.Read())
                {
                    count++;
                    limit--;
                    var addressString = reader.GetString(0);
                    var objectid = reader.GetInt64(1);
                    var objectguid = reader.GetString(2);
                    result.Add(new AddressEntry
                    {
                        ObjectId = objectid,
                        ObjectGuid = objectguid,
                        AddressString = addressString
                    });
                }

                if (count < take) break;
            }

            return result;
        }
    }
}