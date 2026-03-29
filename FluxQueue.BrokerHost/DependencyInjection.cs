using FluxQueue.BrokerHost.Configuration;
using FluxQueue.BrokerHost.Telemetry;
using FluxQueue.Transport.Amqp;

namespace FluxQueue.BrokerHost;

public static class DependencyInjection
{
    public static IServiceCollection ConfigureServices(this WebApplicationBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(builder);

        builder.AddFluxQueueTelemetry();
        builder.Services.AddFluxQueue(builder.Configuration);
        builder.Services.AddFluxQueueAmqp(builder.Configuration);
        return builder.Services;
    }
}
