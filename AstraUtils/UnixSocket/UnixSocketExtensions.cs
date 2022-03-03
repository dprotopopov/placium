using System;
using System.Net.Sockets;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Connections;
using Microsoft.AspNetCore.Server.Kestrel.Core;

namespace AstraUtils.UnixSocket
{
    public static class UnixSocketExtensions
    {
        public static AuthenticationBuilder AddUnixDomainSocket(this AuthenticationBuilder builder,
            string authenticationScheme,
            Action<UnixSocketAuthenticationOptions> configureOptions = null)
        {
            builder.AddScheme<
                UnixSocketAuthenticationOptions,
                UnixSocketAuthenticationHandler>(authenticationScheme, configureOptions);

            return builder;
        }

        public static ListenOptions AddUnixSocketCredentialFeature(this ListenOptions options)
        {
            if (options.SocketPath != null)
                options.Use(next =>
                {
                    var accessor = new FieldAccessor<ConnectionContext, Socket>("_socket");

                    return ctx =>
                    {
                        ctx.Features.Set<IUnixSocketCredentialFeature>(
                            new UnixSocketCredentialFeature(accessor.GetValue(ctx)));

                        return next(ctx);
                    };
                });

            return options;
        }
    }
}