using FluxQueue.Transport.Abstractions;
using FluxQueue.Transport.Abstractions.Models; // this should match option csharp_namespace in your proto

namespace FluxQueue.BrokerHost.Services;

public static class QueueHttpEndpoints
{
    public static IEndpointRouteBuilder MapQueueHttpEndpoints(this IEndpointRouteBuilder app)
    {
        // ----- HTTP -----
        app.MapPost("/queues/{queue}/messages", async (
            string queue,
            SendDto dto,
            IQueueOperations ops,
            CancellationToken ct) =>
        {
            var payload = dto.PayloadBase64 is null
                ? Array.Empty<byte>()
                : Convert.FromBase64String(dto.PayloadBase64);

            var id = await ops.SendAsync(new SendRequest(
                Queue: queue,
                Payload: payload,
                DelaySeconds: dto.DelaySeconds,
                MaxReceiveCount: dto.MaxReceiveCount
            ), ct);

            return Results.Ok(new { messageId = id });
        });

        app.MapPost("/queues/{queue}/messages:receive", async (
            string queue,
            ReceiveDto dto,
            IQueueOperations ops,
            CancellationToken ct) =>
        {
            var msgs = await ops.ReceiveAsync(new ReceiveRequest(
                Queue: queue,
                MaxMessages: dto.MaxMessages,
                VisibilityTimeoutSeconds: dto.VisibilityTimeoutSeconds,
                WaitSeconds: dto.WaitSeconds
            ), ct);

            return Results.Ok(msgs.Select(m => new
            {
                messageId = m.MessageId,
                payloadBase64 = Convert.ToBase64String(m.Payload),
                receiptHandle = m.ReceiptHandle,
                receiveCount = m.ReceiveCount
            }));
        });

        app.MapDelete("/queues/{queue}/receipts/{receiptHandle}", async (
            string queue,
            string receiptHandle,
            IQueueOperations ops,
            CancellationToken ct) =>
        {
            var ok = await ops.AckAsync(queue, receiptHandle, ct);
            return ok ? Results.NoContent() : Results.NotFound();
        });

        app.MapHealthChecks("/health");

        return app;
    }
}