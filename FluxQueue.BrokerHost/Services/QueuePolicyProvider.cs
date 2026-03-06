using FluxQueue.BrokerHost.Configuration;
using Microsoft.Extensions.Options;

namespace FluxQueue.BrokerHost.Services;

public interface IQueuePolicyProvider
{
    FluxQueueDefaultsOptions GetDefaults();
}

public sealed class QueuePolicyProvider : IQueuePolicyProvider
{
    private readonly IOptions<FluxQueueOptions> _options;

    public QueuePolicyProvider(IOptions<FluxQueueOptions> options)
    {
        _options = options;
    }

    public FluxQueueDefaultsOptions GetDefaults() => _options.Value.Defaults;
}