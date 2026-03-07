using FluxQueue.BrokerHost.Configuration;
using FluxQueue.BrokerHost.Services;
using FluxQueue.Core;
using FluxQueue.Transport.Abstractions;
using Microsoft.Extensions.Options;

namespace FluxQueue.BrokerHost;

public static class DependencyInjection
{
    public static IServiceCollection ConfigureServices(this WebApplicationBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(builder);

        builder.Services.AddFluxQueue(builder.Configuration);

        return builder.Services;
    }
}
