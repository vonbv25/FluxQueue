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
using FluxQueue.Core.Adapters;
using FluxQueue.Transport.Abstractions;
using FluxQueue.Transport.Amqp;

namespace FluxQueue.IntegrationTests.TestHost;

public sealed class FluxQueueTestHost : IAsyncDisposable
{
    private readonly IHost _host;

    public int AmqpPort { get; }
    public string DbPath { get; }

    public FluxQueueTestHost()
    {
        AmqpPort = GetFreeTcpPort();
        DbPath = Path.Combine(Path.GetTempPath(), "fluxqueue-tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(DbPath);

        _host = Host.CreateDefaultBuilder()
            .ConfigureLogging(l =>
            {
                l.ClearProviders();
                l.AddConsole();
                l.SetMinimumLevel(LogLevel.Warning);
            })
            .ConfigureServices(services =>
            {
                services.AddSingleton(_ => new QueueEngine(DbPath));

                // Your adapter (QueueEngine -> IQueueOperations)
                services.AddSingleton<IQueueOperations, QueueEngineOperations>();

                // AMQP transport hosted service
                services.AddFluxQueueAmqp(o =>
                {
                    o.Port = AmqpPort;
                    o.DefaultVisibilityTimeoutSeconds = 2; // small for tests
                    o.DefaultWaitSeconds = 1;
                    o.MaxBatch = 50;
                });
            })
            .Build();
    }

    public async Task<int> SweepQueueAsync(string queue, int maxToProcess = 1000, CancellationToken ct = default)
    {
        if (string.IsNullOrWhiteSpace(queue))
            throw new ArgumentNullException(nameof(queue));

        // Resolve the same singleton QueueEngine used by AMQP transport
        var engine = _host.Services.GetRequiredService<QueueEngine>();

        // Run sweep once (your tests can call this deterministically)
        return await engine.SweepExpiredAsync(queue, maxToProcess, ct);
    }

    public async Task StartAsync() => await _host.StartAsync();

    public async Task StopAsync() => await _host.StopAsync();

    public async ValueTask DisposeAsync()
    {
        try { await _host.StopAsync(); } catch { /* ignore */ }
        _host.Dispose();

        try { Directory.Delete(DbPath, recursive: true); } catch { /* ignore */ }
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
