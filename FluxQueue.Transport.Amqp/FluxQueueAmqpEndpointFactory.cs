using Amqp.Listener;
using FluxQueue.Transport.Abstractions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace FluxQueue.Transport.Amqp;

public interface IFluxQueueAmqpEndpointFactory
{
    TargetLinkEndpoint CreateProducerEndpoint(string queue, ListenerLink link);
    SourceLinkEndpoint CreateConsumerEndpoint(string queue, ListenerLink link);
}

public sealed class FluxQueueAmqpEndpointFactory : IFluxQueueAmqpEndpointFactory
{
    private readonly IQueueOperations _ops;
    private readonly AmqpTransportOptions _opt;
    private readonly ILoggerFactory _loggerFactory;

    public FluxQueueAmqpEndpointFactory(
        IQueueOperations ops,
        IOptions<AmqpTransportOptions> opt,
        ILoggerFactory loggerFactory)
    {
        _ops = ops ?? throw new ArgumentNullException(nameof(ops));
        _opt = opt.Value ?? throw new ArgumentNullException(nameof(opt.Value));
        _loggerFactory = loggerFactory ?? throw new ArgumentNullException(nameof(loggerFactory));
    }

    public TargetLinkEndpoint CreateProducerEndpoint(string queue, ListenerLink link)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(queue);
        ArgumentNullException.ThrowIfNull(link);

        var ingress = new QueueIngressProcessor(
            _ops,
            queue,
            _opt.IngressInitialCredit > 0 ? _opt.IngressInitialCredit : 200,
            _loggerFactory.CreateLogger<QueueIngressProcessor>());

        return new TargetLinkEndpoint(ingress, link);
    }

    public SourceLinkEndpoint CreateConsumerEndpoint(string queue, ListenerLink link)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(queue);
        ArgumentNullException.ThrowIfNull(link);

        var egress = new QueueEgressSource(
            _ops,
            queue,
            _opt,
            _loggerFactory.CreateLogger<QueueEgressSource>());

        return new SourceLinkEndpoint(egress, link);
    }
}