using System.Collections.Generic;

namespace Updater.Addrx.Sphinx
{
    public class Doc
    {
        public long id { get; set; }
        public string text { get; set; }
        public int priority { get; set; }
        public float lon { get; set; }
        public float lat { get; set; }
        public int building { get; set; }
        public Dictionary<string, string> data { get; set; }
    }
}