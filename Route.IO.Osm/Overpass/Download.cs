using System.IO;
using System.Net.Http;
using Route.Logging;

namespace Route.IO.Osm.Overpass
{
    /// <summary>
    /// Downloads data from overpass.
    /// </summary>
    public static class OverpassDownload
    {
        /// <summary>
        /// Downloads a file if it doesn't exist yet.
        /// </summary>
        public static Stream ToStream(string query)
        {
            Logger.Log("Download", TraceEventType.Information, 
                "Downloading from overpass...");

            var client = new HttpClient();
            var content = new StringContent("data=" + query);
            var response = client.PostAsync(@"http://overpass-api.de/api/interpreter", content);
            return response.GetAwaiter().GetResult().Content.ReadAsStreamAsync().GetAwaiter().GetResult();
        }
    }
}