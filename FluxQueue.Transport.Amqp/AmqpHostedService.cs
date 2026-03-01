using Amqp.Listener;
using FluxQueue.Transport.Abstractions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace FluxQueue.Transport.Amqp;

public sealed class AmqpHostedService : BackgroundService
{
    private readonly IQueueOperations _ops;
    private readonly AmqpTransportOptions _opt;
    private readonly ILogger<AmqpHostedService> _log;

    private ContainerHost? _host;

    public AmqpHostedService(
        IQueueOperations ops,
        IOptions<AmqpTransportOptions> opt,
        ILogger<AmqpHostedService> log)
    {
        _ops = ops;
        _opt = opt.Value;
        _log = log;
    }

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var uri = $"amqp://{_opt.DefaultAddress}:{_opt.Port}";

        _host = new ContainerHost(new[] { uri });
        _host.RegisterLinkProcessor(new FluxQueueLinkProcessor(_ops, _opt));

        _host.Open();

        _log.LogInformation("AMQP transport listening on {Uri}", uri);

        // Keep running until stopped
        stoppingToken.Register(() =>
        {
            try
            {
                _log.LogInformation("AMQP transport stopping...");
                _host?.Close();
            }
            catch { /* ignore */ }
        });

        return Task.Delay(Timeout.Infinite, stoppingToken);
    }
}