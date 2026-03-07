using FluxQueue.Transport.Abstractions;
using Microsoft.Extensions.DependencyInjection;

namespace FluxQueue.Transport.Amqp;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddFluxQueueAmqp(this IServiceCollection services, Action<AmqpTransportOptions>? configure = null)
    {
        if (configure is not null)
            services.Configure(configure);
        else
            services.AddOptions<AmqpTransportOptions>();
        services.AddHostedService<AmqpHostedService>();
        services.AddSingleton<IFluxQueueAmqpEndpointFactory, FluxQueueAmqpEndpointFactory>();
        return services;
    }
}

