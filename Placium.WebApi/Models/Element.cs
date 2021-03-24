using System;

namespace Placium.WebApi.Models
{
    public abstract class Element
    {
        public Guid guid { get; set; }
        public string title { get; set; }
    }
}