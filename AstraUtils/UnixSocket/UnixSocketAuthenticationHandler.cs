using System;
using System.Globalization;
using System.Security.Claims;
using System.Text.Encodings.Web;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Authentication;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace AstraUtils.UnixSocket;

public class UnixSocketAuthenticationHandler : AuthenticationHandler<UnixSocketAuthenticationOptions>
{
    private readonly ILogger<UnixSocketAuthenticationHandler> _logger;

    public UnixSocketAuthenticationHandler(
        IOptionsMonitor<UnixSocketAuthenticationOptions> options,
        ILoggerFactory logger,
        UrlEncoder encoder,
        ISystemClock clock) : base(options, logger, encoder, clock)
    {
        _logger = logger.CreateLogger<UnixSocketAuthenticationHandler>();
    }


    protected override Task<AuthenticateResult> HandleAuthenticateAsync()
    {
        try
        {
            //Context.Features.Get<IConnectionEndPointFeature>();
            var feature = Context.Features.Get<IUnixSocketCredentialFeature>();

            if (feature == null || !feature.IsAuthenticated)
                return Task.FromResult(AuthenticateResult.Fail("not supporting"));


            return HandleAuthenticateAsync(feature);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "In Task<AuthenticateResult> HandleAuthenticateAsync()");
            throw ex;
        }
    }

    protected virtual Task<AuthenticateResult> HandleAuthenticateAsync(IUnixSocketCredentialFeature feature)
    {
        try
        {
            var identity = new ClaimsIdentity(Scheme.Name);

            identity.AddClaim(new Claim(ClaimTypes.NameIdentifier,
                feature.UserId.ToString(CultureInfo.InvariantCulture)));
            identity.AddClaim(new Claim(nameof(feature.ProcessId),
                feature.ProcessId.ToString(CultureInfo.InvariantCulture)));
            identity.AddClaim(
                new Claim(nameof(feature.GroupId), feature.GroupId.ToString(CultureInfo.InvariantCulture)));

            var p = new ClaimsPrincipal(identity);

            return Task.FromResult(AuthenticateResult.Success(new AuthenticationTicket(p, Scheme.Name)));
        }
        catch (Exception ex)
        {
            _logger.LogError(ex,
                "In Task<AuthenticateResult> HandleAuthenticateAsync(IUnixSocketCredentialFeature feature)");
            throw ex;
        }
    }

    protected override Task HandleChallengeAsync(AuthenticationProperties properties)
    {
        Response.StatusCode = 403;
        return Task.CompletedTask;
    }
}