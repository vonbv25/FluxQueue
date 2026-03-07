using FluxQueue.BrokerHost.Configuration;
using FluxQueue.BrokerHost.Services;
using FluxQueue.Core;
using FluxQueue.Transport.Abstractions;
using FluxQueue.Transport.Amqp;
using Microsoft.Extensions.Options;

namespace FluxQueue.BrokerHost;

public static class DependencyInjection
{
    public static IServiceCollection ConfigureServices(this WebApplicationBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(builder);

        builder.Services.AddFluxQueue(builder.Configuration);
        builder.Services.AddFluxQueueAmqp(builder.Configuration);
        return builder.Services;
    }
}
