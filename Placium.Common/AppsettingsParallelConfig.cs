using Microsoft.Extensions.Configuration;
using System;
using System.Diagnostics;

namespace Placium.Common
{
    public class AppsettingsParallelConfig : IParallelConfig
    {
        private readonly IConfiguration _configuration;

        public AppsettingsParallelConfig(IConfiguration configuration)
        {
            _configuration = configuration;
        }

        public int GetNumberOfThreads()
        {
            var value = _configuration.GetSection($"{nameof(ParallelConfig)}:{nameof(ParallelConfig.NumberOfThreads)}")
                .Value;
            Console.WriteLine($"{nameof(ParallelConfig)}:{nameof(ParallelConfig.NumberOfThreads)} = {value}");
            if (!int.TryParse(value, out var threads)) threads = 12;
            return threads;
        }
    }
}