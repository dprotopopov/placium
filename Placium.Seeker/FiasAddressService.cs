using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using MySql.Data.MySqlClient;
using Placium.Common;

namespace Placium.Seeker
{
    public class FiasAddressService : BaseApiService
    {
        private readonly ILogger _logger;

        public FiasAddressService(IConfiguration configuration, ILogger<FiasAddressService> logger) : base(
            configuration)
        {
            _logger = logger;
        }


        public async Task<IEnumerable<AddressEntry>> GetByNameAsync(string searchString, int limit = 20)
        {
            try
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
                            @"SELECT title,guid FROM addrobx WHERE MATCH(@match) ORDER BY priority ASC LIMIT @skip,@take",
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
                        var guid = reader.GetString(1);
                        result.Add(new AddressEntry
                        {
                            AddressString = addressString
                        });
                    }

                    if (count < take) break;
                }

                return result;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex.Message);
                throw;
            }
        }
    }
}