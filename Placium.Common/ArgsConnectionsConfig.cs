using Microsoft.Extensions.Configuration;

namespace Placium.Common;

public class ArgsConnectionsConfig : IConnectionsConfig
{
    private readonly IConfiguration _configuration;

    public ArgsConnectionsConfig(IConfiguration configuration)
    {
        _configuration = configuration;
    }

    public string GetConnectionString(string name)
    {
        return _configuration[name];
    }
}