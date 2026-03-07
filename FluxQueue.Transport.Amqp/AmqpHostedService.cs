using Amqp.Listener;
using FluxQueue.Transport.Abstractions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace FluxQueue.Transport.Amqp;

public sealed class AmqpHostedService(
    IOptions<AmqpTransportOptions> opt,
    ILogger<AmqpHostedService> log,
    IFluxQueueAmqpEndpointFactory fluxQueueAmqpEndpointFactory,
    ILoggerFactory loggerFactory) : BackgroundService
{
    private readonly AmqpTransportOptions _opt = opt.Value;
    private readonly IFluxQueueAmqpEndpointFactory _fluxQueueAmqpEndpointFactory = fluxQueueAmqpEndpointFactory;
    private ContainerHost? _host;

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var uri = $"amqp://{_opt.DefaultAddress}:{_opt.Port}";

        _host = new ContainerHost(new[] { uri });
        _host.RegisterLinkProcessor(new FluxQueueLinkProcessor(_fluxQueueAmqpEndpointFactory, _opt, loggerFactory.CreateLogger<FluxQueueLinkProcessor>()));

        _host.Open();

        log.LogInformation("AMQP transport listening on {Uri}", uri);

        // Keep running until stopped
        stoppingToken.Register(() =>
        {
            try
            {
                log.LogInformation("AMQP transport stopping...");
                _host?.Close();
            }
            catch { /* ignore */ }
        });

        return Task.Delay(Timeout.Infinite, stoppingToken);
    }
}