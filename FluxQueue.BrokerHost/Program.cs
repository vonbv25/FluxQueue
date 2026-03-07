using FluxQueue.BrokerHost;
using FluxQueue.BrokerHost.Configuration;
using FluxQueue.BrokerHost.Http;
using FluxQueue.BrokerHost.Services;
using FluxQueue.Core;
using FluxQueue.Transport.Abstractions;
using FluxQueue.Transport.Amqp;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.Options;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddGrpc();

builder.ConfigureServices();
//
// --------------------
// Background workers
// --------------------
//

builder.Services.AddHostedService<QueueReconcilerHostedService>();
builder.Services.AddHostedService<QueueSweeper>();

//
// --------------------
// Health checks
// --------------------
//

builder.Services.AddHealthChecks();

//
// --------------------
// Kestrel configuration
// --------------------
//

builder.WebHost.ConfigureKestrel((context, options) =>
{
    var kestrelSection = context.Configuration.GetSection("Kestrel");

    if (kestrelSection.Exists())
    {
        options.Configure(kestrelSection);
    }
    else
    {
        options.ListenAnyIP(8080, listen =>
        {
            listen.Protocols = HttpProtocols.Http1;
        });

        options.ListenAnyIP(8081, listen =>
        {
            listen.Protocols = HttpProtocols.Http2;
        });
    }
});

var app = builder.Build();

var lifetime = app.Services.GetRequiredService<IHostApplicationLifetime>();

lifetime.ApplicationStopping.Register(() =>
{
    var logger = app.Services
        .GetRequiredService<ILoggerFactory>()
        .CreateLogger("FluxQueue.Shutdown");

    logger.LogInformation("FluxQueue shutting down gracefully...");

    try
    {
        var engine = app.Services.GetRequiredService<QueueEngine>();
        engine.Dispose();
    }
    catch (Exception ex)
    {
        logger.LogError(ex, "Error during QueueEngine shutdown");
    }
});

//
// --------------------
// Health endpoints
// --------------------
//

app.MapGet("/health/live", () => Results.Ok(new
{
    status = "ok",
    service = "fluxqueue-broker"
}));

app.MapGet("/health/ready", (IOptions<FluxQueueOptions> options) =>
{
    return Results.Ok(new
    {
        status = "ready",
        dbPath = options.Value.DbPath
    });
});

app.MapHealthChecks("/health");

//
// --------------------
// HTTP API
// --------------------
//

app.MapQueueHttpEndpoints();

//
// --------------------
// gRPC API
// --------------------
//

app.MapGrpcService<QueueBrokerGrpcService>();

//
// --------------------
// Root endpoint
// --------------------
//

app.MapGet("/", (IOptions<FluxQueueOptions> options) =>
{
    return Results.Ok(new
    {
        service = "FluxQueue Broker",
        protocols = new[] { "HTTP", "gRPC", "AMQP" },
        httpPort = 8080,
        grpcPort = 8081,
        amqpPort = options.Value.Amqp.Port,
        dbPath = options.Value.DbPath
    });
});

app.Run();