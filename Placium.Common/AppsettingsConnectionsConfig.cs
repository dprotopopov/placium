using Microsoft.Extensions.Configuration;

namespace Placium.Common
{
    public class AppsettingsConnectionsConfig : IConnectionsConfig
    {
        private readonly IConfiguration _configuration;

        public AppsettingsConnectionsConfig(IConfiguration configuration)
        {
            _configuration = configuration;
        }

        public string GetConnectionString(string name)
        {
            return _configuration.GetConnectionString(name);
        }
    }
}