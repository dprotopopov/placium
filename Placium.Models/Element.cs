using System;

namespace Placium.Models
{
    public abstract class Element
    {
        public Guid guid { get; set; }
        public string title { get; set; }

        public override string ToString()
        {
            return title; 
        }
    }
}
