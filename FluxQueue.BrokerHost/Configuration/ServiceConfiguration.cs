using FluxQueue.BrokerHost.Services;
using FluxQueue.Core;
using FluxQueue.Transport.Abstractions;
using Microsoft.Extensions.Options;

namespace FluxQueue.BrokerHost.Configuration;

public static class FluxQueueServiceCollectionExtensions
{
    public static IServiceCollection AddFluxQueue(
        this IServiceCollection services,
        IConfiguration configuration)
    {
        services.BindValidatedOptions<FluxQueueOptions>(
            configuration,
            FluxQueueOptions.SectionName);

        services.BindValidatedOptions<QueueReconcilerOptions>(
            configuration,
            $"{FluxQueueOptions.SectionName}:Reconciler");

        services.BindValidatedOptions<QueueSweeperOptions>(
            configuration,
            $"{FluxQueueOptions.SectionName}:Sweeper");

        services.AddSingleton<QueueEngine>(sp =>
        {
            var options = sp.GetRequiredService<IOptions<FluxQueueOptions>>().Value;

            if (string.IsNullOrWhiteSpace(options.DbPath))
            {
                throw new InvalidOperationException(
                    $"{FluxQueueOptions.SectionName}:{nameof(FluxQueueOptions.DbPath)} must be configured.");
            }

            Directory.CreateDirectory(options.DbPath);
            return new QueueEngine(options.DbPath);
        });

        services.AddSingleton<IQueuePolicyProvider, QueuePolicyProvider>();
        services.AddSingleton<IQueueRequestValidator, QueueRequestValidator>();
        services.AddSingleton<QueueEngineOperations>();
        services.AddSingleton<IQueueOperations, TelemetryQueueOperations>();

        services.AddHostedService<QueueReconcilerHostedService>();
        services.AddHostedService<QueueSweeper>();

        return services;
    }

    private static IServiceCollection BindValidatedOptions<TOptions>(
        this IServiceCollection services,
        IConfiguration configuration,
        string sectionName)
        where TOptions : class
    {
        services
            .AddOptions<TOptions>()
            .Bind(configuration.GetSection(sectionName))
            .ValidateDataAnnotations()
            .ValidateOnStart();

        return services;
    }
}
