using FluxQueue.BrokerHost.Services;
using FluxQueue.Core;
using Microsoft.AspNetCore.Server.Kestrel.Core;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddGrpc();

// RocksDB path should be config-driven
var dbPath = builder.Configuration["FluxQueue:DbPath"] ?? "./data/rocksdb";
builder.Services.AddSingleton(_ => new QueueEngine(dbPath));

// Background sweep: MVP needs it
builder.Services.AddHostedService<QueueSweeper>();

builder.WebHost.ConfigureKestrel(o =>
{
    o.ListenAnyIP(5000, listen => listen.Protocols = HttpProtocols.Http1AndHttp2);
});

var app = builder.Build();

// ----- HTTP -----
app.MapPost("/queues/{queue}/messages", async (string queue, SendDto dto, QueueEngine engine) =>
{
    var payload = dto.PayloadBase64 is null ? Array.Empty<byte>() : Convert.FromBase64String(dto.PayloadBase64);
    var id = await engine.SendAsync(queue, payload, dto.DelaySeconds, dto.MaxReceiveCount);
    return Results.Ok(new { messageId = id });
});

app.MapPost("/queues/{queue}/messages:receive", async (string queue, ReceiveDto dto, QueueEngine engine) =>
{
    var msgs = await engine.ReceiveAsync(queue, dto.MaxMessages, dto.VisibilityTimeoutSeconds, dto.WaitSeconds);
    return Results.Ok(msgs.Select(m => new
    {
        messageId = m.MessageId,
        payloadBase64 = Convert.ToBase64String(m.Payload),
        receiptHandle = m.ReceiptHandle,
        receiveCount = m.ReceiveCount
    }));
});

app.MapDelete("/queues/{queue}/receipts/{receiptHandle}", async (string queue, string receiptHandle, QueueEngine engine) =>
{
    var ok = await engine.AckAsync(queue, receiptHandle);
    return ok ? Results.NoContent() : Results.NotFound();
});

// ----- gRPC -----
app.MapGrpcService<QueueBrokerGrpcService>();

app.MapGet("/", () => "FluxQueue Broker running (HTTP + gRPC).");
app.Run();

record SendDto(string? PayloadBase64, int DelaySeconds = 0, int MaxReceiveCount = 5);
record ReceiveDto(int MaxMessages = 1, int VisibilityTimeoutSeconds = 30, int WaitSeconds = 0);