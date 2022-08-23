using System;

namespace Placium.Models
{
    public class Element
    {
        public long? objectid { get; set; }
        public Guid guid { get; set; }
        public string title { get; set; }

        public override string ToString()
        {
            return title;
        }
    }
}