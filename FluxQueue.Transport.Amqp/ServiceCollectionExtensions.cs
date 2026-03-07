using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace FluxQueue.Transport.Amqp;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddFluxQueueAmqp(
        this IServiceCollection services,
        IConfiguration configuration)
    {
        services
            .AddOptions<AmqpTransportOptions>()
            .Bind(configuration.GetSection($"{AmqpTransportOptions.SectionName}"))
            .ValidateDataAnnotations()
            .ValidateOnStart();

        services.AddSingleton(sp =>
            sp.GetRequiredService<IOptions<AmqpTransportOptions>>().Value);

        services.AddSingleton<IFluxQueueAmqpEndpointFactory, FluxQueueAmqpEndpointFactory>();
        services.AddHostedService<AmqpHostedService>();

        return services;
    }
}

