using Microsoft.Extensions.Configuration;
using System.Diagnostics;
using System.IO;

namespace Placium.Common
{
    public class AppsettingsSphinxConfig : ISphinxConfig
    {
        private readonly IConfiguration _configuration;

        public AppsettingsSphinxConfig(IConfiguration configuration)
        {
            _configuration = configuration;
        }

        public string GetWordformsPath(string fileName)
        {
            var folder = _configuration.GetSection($"{nameof(SphinxConfig)}:{nameof(SphinxConfig.WordformsFolder)}")
                .Value;
            Debug.WriteLine($"{nameof(SphinxConfig)}:{nameof(SphinxConfig.WordformsFolder)} = '{folder}'");
            return Path.Combine(folder, fileName);
        }

        public string SphinxHttp()
        {
            var http = _configuration.GetSection($"{nameof(SphinxConfig)}:{nameof(SphinxConfig.SphinxHttp)}")
                .Value;
            Debug.WriteLine($"{nameof(SphinxConfig)}:{ nameof(SphinxConfig.SphinxHttp)} = '{http}'");
            return http;
        }
    }
}