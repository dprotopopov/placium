namespace Placium.Common
{

    public class SphinxConfig
    {
        /// <summary>
        /// You can connect to Manticore Search over HTTP/HTTPS.
        /// By default Manticore listens for HTTP, HTTPS and binary requests on ports 9308 and 9312.
        /// In section "searchd" of your configuration file the HTTP port can be defined with directive listen like this:
        /// searchd {
        /// ...
        /// listen = 127.0.0.1:9308
        /// listen = 127.0.0.1:9312:http
        /// ...
        /// }
        /// Both lines are valid and equal by meaning(except for the port number), they both define listeners that will serve all api/http/https protocols.There are no special requirements and any HTTP client can be used to connect to Manticore.
        /// All HTTP endpoints respond with application/json content type. 
        /// </summary>
        public string SphinxHttp { get; set; }

        /// <summary>
        /// The dictionaries are used to normalize incoming words both during indexing and searching.
        /// CREATE TABLE products(title text, price float) wordforms = '/var/lib/manticore/wordforms.txt' wordforms = '/var/lib/manticore/alternateforms.txt /var/lib/manticore/dict*.txt'
        /// In RT mode only absolute paths are allowed.
        /// </summary>
        public string WordformsFolder { get; set; }
    }
}