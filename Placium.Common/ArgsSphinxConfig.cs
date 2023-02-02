using Microsoft.Extensions.Configuration;
using System.IO;

namespace Placium.Common
{
    public class ArgsSphinxConfig : ISphinxConfig
    {
        private readonly IConfiguration _configuration;

        public ArgsSphinxConfig(IConfiguration configuration)
        {
            _configuration = configuration;
        }

        public string GetWordformsPath(string fileName)
        {
            var folder = _configuration["wordformsfolder"];
            return Path.Combine(folder, fileName);
        }

        public string SphinxHttp()
        {
            var http = _configuration["sphinxhttp"];
            return http;
        }
    }

}