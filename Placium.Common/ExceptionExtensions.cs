using System;
using System.Text;

namespace Placium.Common
{
    public static class ExceptionExtensions
    {
        public static string FullMessage(this Exception ex)
        {
            var sb = new StringBuilder();
            for (; ex != null; ex = ex.InnerException) sb.AppendLine(ex.Message);
            return sb.ToString();
        }
    }
}