using Microsoft.AspNetCore.Mvc.Filters;
using Microsoft.Extensions.Logging;

namespace Placium.WebApi.Filters
{
    public class ExceptionFilter : IExceptionFilter
    {
        private readonly ILogger<ExceptionFilter> _logger;

        public ExceptionFilter(ILogger<ExceptionFilter> logger)
        {
            _logger = logger;
        }

        public void OnException(ExceptionContext context)
        {
            var actionName = context.ActionDescriptor.DisplayName;
            var exceptionStack = context.Exception.StackTrace;
            var exceptionMessage = context.Exception.Message;
            _logger.LogError($"{exceptionMessage} in {actionName}\n{exceptionStack}");
        }
    }
}