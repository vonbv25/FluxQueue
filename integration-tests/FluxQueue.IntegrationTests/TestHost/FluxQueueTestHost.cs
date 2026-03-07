using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Sockets;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.DependencyInjection;
using FluxQueue.Core;
using FluxQueue.Transport.Abstractions;
using FluxQueue.Transport.Amqp;
using FluxQueue.BrokerHost.Services;
using FluxQueue.BrokerHost.Configuration;
using Microsoft.Extensions.Configuration;

namespace FluxQueue.IntegrationTests.TestHost;

public sealed class FluxQueueTestHost : IAsyncDisposable
{
    private readonly IHost _host;

    public int AmqpPort { get; }
    public string DbPath { get; }
    public string AmqpHost => "127.0.0.1";

    public FluxQueueTestHost()
    {
        AmqpPort = GetFreeTcpPort();
        DbPath = Path.Combine(Path.GetTempPath(), "fluxqueue-tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(DbPath);

        var configData = new Dictionary<string, string?>
        {
            [$"{FluxQueueOptions.SectionName}:DbPath"] = DbPath,
            [$"{FluxQueueOptions.SectionName}:Reconciler:Enabled"] = "false",
            [$"{FluxQueueOptions.SectionName}:Sweeper:Enabled"] = "false",

            [$"{AmqpTransportOptions.SectionName}:Port"] = AmqpPort.ToString(),
            [$"{AmqpTransportOptions.SectionName}:DefaultVisibilityTimeoutSeconds"] = "2",
            [$"{AmqpTransportOptions.SectionName}:DefaultWaitSeconds"] = "1",
            [$"{AmqpTransportOptions.SectionName}:EgressInitialCredit"] = "50",
            [$"{AmqpTransportOptions.SectionName}:IngressInitialCredit"] = "200"
        };

        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(configData)
            .Build();

        _host = Host.CreateDefaultBuilder()
            .ConfigureLogging(l =>
            {
                l.ClearProviders();
                l.AddConsole();
                l.SetMinimumLevel(LogLevel.Warning);
            })
            .ConfigureServices(services =>
            {
                services.AddSingleton<IConfiguration>(configuration);

                services.AddFluxQueue(configuration);
                services.AddFluxQueueAmqp(configuration);
            })
            .Build();
    }

    public async Task StartAsync()
    {
        await _host.StartAsync();

        // allow listener socket bind/startup to settle in tests
        await Task.Delay(150);
    }

    public Task StopAsync() => _host.StopAsync();

    public async Task<int> SweepQueueAsync(string queue, int maxToProcess = 1000, CancellationToken ct = default)
    {
        if (string.IsNullOrWhiteSpace(queue))
            throw new ArgumentNullException(nameof(queue));

        var engine = _host.Services.GetRequiredService<QueueEngine>();
        return await engine.SweepExpiredAsync(queue, maxToProcess, ct);
    }

    public async ValueTask DisposeAsync()
    {
        try { await _host.StopAsync(); } catch { }
        _host.Dispose();

        try { Directory.Delete(DbPath, recursive: true); } catch { }
    }

    private static int GetFreeTcpPort()
    {
        var l = new TcpListener(IPAddress.Loopback, 0);
        l.Start();
        var port = ((IPEndPoint)l.LocalEndpoint).Port;
        l.Stop();
        return port;
    }
}
