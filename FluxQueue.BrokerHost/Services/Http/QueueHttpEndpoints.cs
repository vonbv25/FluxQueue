using FluxQueue.Transport.Abstractions;
using FluxQueue.Transport.Abstractions.Models;
using FluxQueue.Transport.Http;

namespace FluxQueue.BrokerHost.Services.Http;

public static class QueueHttpEndpoints
{
    public static IEndpointRouteBuilder MapQueueHttpEndpoints(this IEndpointRouteBuilder app)
    {
        var group = app
            .MapGroup("/api/v1/queues")
            .WithTags("FluxQueue")
            .AddEndpointFilter<QueueHttpExceptionFilter>();

        MapSend(group);
        MapReceive(group);
        MapAck(group);
        MapReject(group);

        return app;
    }

    private static void MapSend(RouteGroupBuilder group)
    {
        group.MapPost("/{queue}/messages", async (
                string queue,
                SendDto dto,
                IQueueOperations ops,
                CancellationToken ct) =>
        {
            byte[] payload;
            try
            {
                payload = dto.PayloadBase64 is null
                    ? Array.Empty<byte>()
                    : Convert.FromBase64String(dto.PayloadBase64);
            }
            catch (FormatException)
            {
                return Results.BadRequest(
                    new ErrorResponseDto("payloadBase64 must be a valid Base64 string."));
            }

            var id = await ops.SendAsync(new SendRequest(
                Queue: queue,
                Payload: payload,
                DelaySeconds: dto.DelaySeconds,
                MaxReceiveCount: dto.MaxReceiveCount
            ), ct);

            return Results.Ok(new SendResponseDto(id));
        })
            .WithName("FluxQueue_SendMessage")
            .WithSummary("Send a message to a queue")
            .WithDescription("""
                Sends a message to the specified queue.

                The payload must be Base64-encoded in `payloadBase64`.
                Optional delay and maxReceiveCount may be provided.
                """)
            .Produces<SendResponseDto>(StatusCodes.Status200OK)
            .Produces<ErrorResponseDto>(StatusCodes.Status400BadRequest)
            .ProducesProblem(StatusCodes.Status500InternalServerError);
    }

    private static void MapReceive(RouteGroupBuilder group)
    {
        group.MapPost("/{queue}/messages:receive", async (
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

            var response = new ReceiveResponseDto(
                msgs.Select(ToReceivedMessageDto).ToArray());

            return Results.Ok(response);
        })
            .WithName("FluxQueue_ReceiveMessages")
            .WithSummary("Receive messages from a queue")
            .WithDescription("""
                Receives up to `maxMessages` from the specified queue.

                Supports long polling via `waitSeconds`.
                Returned messages include a `receiptHandle` required for ack or reject.
                """)
            .Produces<ReceiveResponseDto>(StatusCodes.Status200OK)
            .Produces<ErrorResponseDto>(StatusCodes.Status400BadRequest)
            .ProducesProblem(StatusCodes.Status500InternalServerError);
    }

    private static void MapAck(RouteGroupBuilder group)
    {
        group.MapPost("/{queue}/receipts/{receiptHandle}:ack", async (
                string queue,
                string receiptHandle,
                IQueueOperations ops,
                CancellationToken ct) =>
        {
            var ok = await ops.AckAsync(queue, receiptHandle, ct);

            return ok
                ? Results.Ok(new ReceiptActionResponseDto(
                    Success: true,
                    Action: "ack",
                    Queue: queue,
                    ReceiptHandle: receiptHandle))
                : Results.NotFound(new ErrorResponseDto("Receipt handle was not found or is no longer valid."));
        })
            .WithName("FluxQueue_AckMessage")
            .WithSummary("Acknowledge a received message")
            .WithDescription("""
                Acknowledges successful processing of a received message.

                The receipt handle must come from a previous receive operation.
                Acknowledged messages are removed permanently from the queue.
                """)
            .Produces<ReceiptActionResponseDto>(StatusCodes.Status200OK)
            .Produces<ErrorResponseDto>(StatusCodes.Status404NotFound)
            .Produces<ErrorResponseDto>(StatusCodes.Status400BadRequest)
            .ProducesProblem(StatusCodes.Status500InternalServerError);

        // Optional backward-compatible route
        group.MapDelete("/{queue}/receipts/{receiptHandle}", async (
                string queue,
                string receiptHandle,
                IQueueOperations ops,
                CancellationToken ct) =>
        {
            var ok = await ops.AckAsync(queue, receiptHandle, ct);
            return ok ? Results.NoContent() : Results.NotFound();
        })
            .WithName("FluxQueue_AckMessage_LegacyDelete")
            .WithSummary("Acknowledge a received message (legacy route)")
            .ExcludeFromDescription();
    }

    private static void MapReject(RouteGroupBuilder group)
    {
        group.MapPost("/{queue}/receipts/{receiptHandle}:reject", async (
                string queue,
                string receiptHandle,
                IQueueOperations ops,
                CancellationToken ct) =>
        {
            var ok = await ops.RejectAsync(queue, receiptHandle, ct);

            return ok
                ? Results.Ok(new ReceiptActionResponseDto(
                    Success: true,
                    Action: "reject",
                    Queue: queue,
                    ReceiptHandle: receiptHandle))
                : Results.NotFound(new ErrorResponseDto("Receipt handle was not found or is no longer valid."));
        })
            .WithName("FluxQueue_RejectMessage")
            .WithSummary("Reject a received message")
            .WithDescription("""
                Rejects a received message and moves it to the dead-letter queue (DLQ).

                The receipt handle must come from a previous receive operation.
                Rejected messages are not returned to the source queue.
                """)
            .Produces<ReceiptActionResponseDto>(StatusCodes.Status200OK)
            .Produces<ErrorResponseDto>(StatusCodes.Status404NotFound)
            .Produces<ErrorResponseDto>(StatusCodes.Status400BadRequest)
            .ProducesProblem(StatusCodes.Status500InternalServerError);
    }

    private static ReceivedMessageDto ToReceivedMessageDto(TransportMessage m) =>
        new(
            MessageId: m.MessageId,
            PayloadBase64: Convert.ToBase64String(m.Payload),
            ReceiptHandle: m.ReceiptHandle,
            ReceiveCount: m.ReceiveCount);
}