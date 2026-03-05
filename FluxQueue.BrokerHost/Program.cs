using FluxQueue.BrokerHost.Services;
using FluxQueue.Core;
using FluxQueue.Core.Adapters;
using FluxQueue.Transport.Abstractions;
using FluxQueue.Transport.Amqp;
using Microsoft.AspNetCore.Server.Kestrel.Core;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddGrpc();

// RocksDB path should be config-driven
var dbPath = builder.Configuration["FluxQueue:DbPath"] ?? "./data/rocksdb";
builder.Services.AddSingleton(_ =>
{
    Directory.CreateDirectory(dbPath);
    return new QueueEngine(dbPath);
});

// Background sweep: MVP needs it
builder.Services.Configure<QueueReconcilerOptions>(builder.Configuration.GetSection("FluxQueue:Reconciler"));
builder.Services.AddHostedService<QueueReconcilerHostedService>();
builder.Services.Configure<QueueSweeperOptions>(builder.Configuration.GetSection("FluxQueue:Sweeper"));
builder.Services.AddHostedService<QueueSweeper>();
builder.Services.AddHealthChecks();
builder.Services.AddSingleton<IQueueOperations, QueueEngineOperations>();
builder.Services.AddFluxQueueAmqp(o =>
{
    o.Port = 5672;
    o.DefaultVisibilityTimeoutSeconds = 30; // match your typical default
    o.DefaultWaitSeconds = 1;               // enable long poll
    o.MaxBatch = 50;
});
builder.WebHost.ConfigureKestrel((context, options) =>
{
    options.Configure(context.Configuration.GetSection("Kestrel"));
});

var app = builder.Build();
// ----- HTTP -----
app.MapQueueHttpEndpoints();
// ----- gRPC -----
app.MapGrpcService<QueueBrokerGrpcService>();

app.MapGet("/", () => "FluxQueue Broker running (HTTP + gRPC).");

app.Run();