using Amqp.Framing;
using Amqp.Listener;
using Microsoft.Extensions.Logging;

namespace FluxQueue.Transport.Amqp;

internal sealed class FluxQueueLinkProcessor : ILinkProcessor
{
    private readonly IFluxQueueAmqpEndpointFactory _endpointFactory;
    private readonly AmqpTransportOptions _opt;
    private readonly ILogger<FluxQueueLinkProcessor> _log;

    public FluxQueueLinkProcessor(
        IFluxQueueAmqpEndpointFactory endpointFactory,
        AmqpTransportOptions opt,
        ILogger<FluxQueueLinkProcessor> log)
    {
        _endpointFactory = endpointFactory ?? throw new ArgumentNullException(nameof(endpointFactory));
        _opt = opt ?? throw new ArgumentNullException(nameof(opt));
        _log = log ?? throw new ArgumentNullException(nameof(log));
    }

    public void Process(AttachContext context)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(context.Attach);
        ArgumentNullException.ThrowIfNull(context.Link);

        try
        {
            var attach = context.Attach;
            var peerIsSender = !attach.Role;

            if (peerIsSender)
            {
                ProcessProducerLink(context, attach);
                return;
            }

            ProcessConsumerLink(context, attach);
        }
        catch (Exception ex)
        {
            _log.LogError(
                ex,
                "Failed to process AMQP link attach. LinkName={LinkName}, Role={Role}",
                context.Attach?.LinkName,
                context.Attach?.Role);

            CompleteWithError(
                context,
                condition: "amqp:internal-error",
                description: "Failed to process link attach.");
        }
    }

    private void ProcessProducerLink(AttachContext context, Attach attach)
    {
        var target = attach.Target as Target;
        if (target is null)
        {
            _log.LogWarning(
                "Rejected AMQP producer link because target terminus was missing or invalid. LinkName={LinkName}",
                attach.LinkName);

            CompleteWithError(
                context,
                condition: "amqp:invalid-field",
                description: "Target terminus is required for producer link.");
            return;
        }

        if (target.Dynamic == true)
        {
            _log.LogWarning(
                "Rejected AMQP producer link because dynamic target is not supported. LinkName={LinkName}",
                attach.LinkName);

            CompleteWithError(
                context,
                condition: "amqp:not-implemented",
                description: "Dynamic target is not supported.");
            return;
        }

        if (!TryResolveQueue(
                context,
                attachName: attach.LinkName,
                address: target.Address?.ToString(),
                fieldName: "Target.Address",
                direction: "producer",
                out var queue))
        {
            return;
        }

        var credit = NormalizeInitialCredit(_opt.IngressInitialCredit, fallback: 200);

        _log.LogInformation(
            "Accepted AMQP producer link. LinkName={LinkName}, Queue={Queue}, InitialCredit={InitialCredit}",
            attach.LinkName,
            queue,
            credit);

        var endpoint = _endpointFactory.CreateProducerEndpoint(queue, context.Link);
        context.Complete(endpoint, initialCredit: credit);
    }

    private void ProcessConsumerLink(AttachContext context, Attach attach)
    {
        var source = attach.Source as Source;
        if (source is null)
        {
            _log.LogWarning(
                "Rejected AMQP consumer link because source terminus was missing or invalid. LinkName={LinkName}",
                attach.LinkName);

            CompleteWithError(
                context,
                condition: "amqp:invalid-field",
                description: "Source terminus is required for consumer link.");
            return;
        }

        if (source.Dynamic == true)
        {
            _log.LogWarning(
                "Rejected AMQP consumer link because dynamic source is not supported. LinkName={LinkName}",
                attach.LinkName);

            CompleteWithError(
                context,
                condition: "amqp:not-implemented",
                description: "Dynamic source is not supported.");
            return;
        }

        if (!TryResolveQueue(
                context,
                attachName: attach.LinkName,
                address: source.Address?.ToString(),
                fieldName: "Source.Address",
                direction: "consumer",
                out var queue))
        {
            return;
        }

        var credit = NormalizeInitialCredit(_opt.EgressInitialCredit, fallback: 50);

        _log.LogInformation(
            "Accepted AMQP consumer link. LinkName={LinkName}, Queue={Queue}, InitialCredit={InitialCredit}",
            attach.LinkName,
            queue,
            credit);

        var endpoint = _endpointFactory.CreateConsumerEndpoint(queue, context.Link);
        context.Complete(endpoint, initialCredit: credit);
    }

    private bool TryResolveQueue(
        AttachContext context,
        string? attachName,
        string? address,
        string fieldName,
        string direction,
        out string queue)
    {
        queue = string.Empty;

        if (string.IsNullOrWhiteSpace(address))
        {
            _log.LogWarning(
                "Rejected AMQP {Direction} link because {FieldName} was empty. LinkName={LinkName}",
                direction,
                fieldName,
                attachName);

            CompleteWithError(
                context,
                condition: "amqp:invalid-field",
                description: $"{fieldName} (queue) is required for {direction} link.");
            return false;
        }

        try
        {
            queue = AmqpAddressMapper.ToQueueName(address);
            return true;
        }
        catch (Exception ex)
        {
            _log.LogWarning(
                ex,
                "Rejected AMQP {Direction} link because queue address could not be mapped. LinkName={LinkName}, Address={Address}",
                direction,
                attachName,
                address);

            CompleteWithError(
                context,
                condition: "amqp:invalid-field",
                description: ex.Message);
            return false;
        }
    }

    private static int NormalizeInitialCredit(int configuredValue, int fallback)
        => configuredValue > 0 ? configuredValue : fallback;

    private static void CompleteWithError(
        AttachContext context,
        string condition,
        string description)
    {
        context.Complete(new Error
        {
            Condition = condition,
            Description = description
        });
    }
}