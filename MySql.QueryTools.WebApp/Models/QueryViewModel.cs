using System.Collections;

namespace MySql.QueryTools.WebApp.Models
{
    public class QueryViewModel
    {
        public string q { get; set; }
        public ArrayList Headers { get; set; }
        public ArrayList Items { get; set; }
        public string Error { get; set; }
    }
}