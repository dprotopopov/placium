using Microsoft.Extensions.Configuration;

namespace Placium.Common;

public class BaseApiService
{
    private readonly IConfiguration _configuration;

    public BaseApiService(IConfiguration configuration)
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

    protected string GetRouteConnectionString()
    {
        return _configuration.GetConnectionString("RouteConnection");
    }
}