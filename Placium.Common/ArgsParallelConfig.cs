using Microsoft.Extensions.Configuration;

namespace Placium.Common;

public class ArgsParallelConfig : IParallelConfig
{
    private readonly IConfiguration _configuration;

    public ArgsParallelConfig(IConfiguration configuration)
    {
        _configuration = configuration;
    }

    public int GetNumberOfThreads()
    {
        var value = _configuration["threads"];
        if (!int.TryParse(value, out var threads)) threads = 12;
        return threads;
    }
}