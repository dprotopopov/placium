using System.Collections;

namespace MySql.QueryTools.WebApp.Models
{
    public class QueryViewModel
    {
        public string Query { get; set; }
        public ArrayList Headers { get; set; }
        public ArrayList Items { get; set; }
        public string Error { get; set; }
    }
}