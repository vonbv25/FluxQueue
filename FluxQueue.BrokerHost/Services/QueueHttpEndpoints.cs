using FluxQueue.BrokerHost.Services;
using FluxQueue.Transport.Abstractions;
using FluxQueue.Transport.Abstractions.Models;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;

namespace FluxQueue.BrokerHost.Http;

public sealed record SendDto(
    string? PayloadBase64,
    int? DelaySeconds = null,
    int? MaxReceiveCount = null);

public sealed record ReceiveDto(
    int? MaxMessages = null,
    int? VisibilityTimeoutSeconds = null,
    int? WaitSeconds = null);
public static class QueueHttpEndpoints
{
    public static IEndpointRouteBuilder MapQueueHttpEndpoints(this IEndpointRouteBuilder app)
    {
        app.MapPost("/queues/{queue}/messages", async (
            string queue,
            SendDto dto,
            IQueueOperations ops,
            IQueuePolicyProvider policyProvider,
            CancellationToken ct) =>
        {
            if (!IsValidQueueName(queue))
            {
                return Results.BadRequest(new
                {
                    error = "Invalid queue name. Allowed characters are letters, digits, '-', '_' and '.'."
                });
            }

            var defaults = policyProvider.GetDefaults();

            byte[] payload;
            try
            {
                payload = dto.PayloadBase64 is null
                    ? Array.Empty<byte>()
                    : Convert.FromBase64String(dto.PayloadBase64);
            }
            catch (FormatException)
            {
                return Results.BadRequest(new
                {
                    error = "payloadBase64 must be a valid Base64 string."
                });
            }

            if (payload.Length > defaults.MaxMessageSizeBytes)
            {
                return Results.BadRequest(new
                {
                    error = $"Payload exceeds max allowed size of {defaults.MaxMessageSizeBytes} bytes."
                });
            }

            var delaySeconds = dto.DelaySeconds ?? 0;
            if (delaySeconds < 0)
            {
                return Results.BadRequest(new
                {
                    error = "delaySeconds must be greater than or equal to 0."
                });
            }

            var maxReceiveCount = dto.MaxReceiveCount ?? defaults.MaxReceiveCount;
            if (maxReceiveCount <= 0)
            {
                return Results.BadRequest(new
                {
                    error = "maxReceiveCount must be greater than 0."
                });
            }

            var id = await ops.SendAsync(new SendRequest(
                Queue: queue,
                Payload: payload,
                DelaySeconds: delaySeconds,
                MaxReceiveCount: maxReceiveCount
            ), ct);

            return Results.Ok(new { messageId = id });
        });

        app.MapPost("/queues/{queue}/messages:receive", async (
            string queue,
            ReceiveDto dto,
            IQueueOperations ops,
            IQueuePolicyProvider policyProvider,
            CancellationToken ct) =>
        {
            if (!IsValidQueueName(queue))
            {
                return Results.BadRequest(new
                {
                    error = "Invalid queue name. Allowed characters are letters, digits, '-', '_' and '.'."
                });
            }

            var defaults = policyProvider.GetDefaults();

            var maxMessages = dto.MaxMessages ?? 1;
            if (maxMessages <= 0)
            {
                return Results.BadRequest(new
                {
                    error = "maxMessages must be greater than 0."
                });
            }

            if (maxMessages > defaults.MaxBatchReceiveMessages)
            {
                return Results.BadRequest(new
                {
                    error = $"maxMessages exceeds allowed limit of {defaults.MaxBatchReceiveMessages}."
                });
            }

            var visibilityTimeoutSeconds =
                dto.VisibilityTimeoutSeconds ?? defaults.DefaultVisibilityTimeoutSeconds;

            if (visibilityTimeoutSeconds <= 0)
            {
                return Results.BadRequest(new
                {
                    error = "visibilityTimeoutSeconds must be greater than 0."
                });
            }

            if (visibilityTimeoutSeconds > defaults.MaxVisibilityTimeoutSeconds)
            {
                return Results.BadRequest(new
                {
                    error = $"visibilityTimeoutSeconds exceeds allowed limit of {defaults.MaxVisibilityTimeoutSeconds}."
                });
            }

            var waitSeconds = dto.WaitSeconds ?? defaults.DefaultWaitSeconds;
            if (waitSeconds < 0)
            {
                return Results.BadRequest(new
                {
                    error = "waitSeconds must be greater than or equal to 0."
                });
            }

            if (waitSeconds > defaults.MaxWaitSeconds)
            {
                return Results.BadRequest(new
                {
                    error = $"waitSeconds exceeds allowed limit of {defaults.MaxWaitSeconds}."
                });
            }

            var msgs = await ops.ReceiveAsync(new ReceiveRequest(
                Queue: queue,
                MaxMessages: maxMessages,
                VisibilityTimeoutSeconds: visibilityTimeoutSeconds,
                WaitSeconds: waitSeconds
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
            if (!IsValidQueueName(queue))
            {
                return Results.BadRequest(new
                {
                    error = "Invalid queue name. Allowed characters are letters, digits, '-', '_' and '.'."
                });
            }

            if (string.IsNullOrWhiteSpace(receiptHandle))
            {
                return Results.BadRequest(new
                {
                    error = "receiptHandle is required."
                });
            }

            var ok = await ops.AckAsync(queue, receiptHandle, ct);
            return ok ? Results.NoContent() : Results.NotFound();
        });
        return app;
    }

    private static bool IsValidQueueName(string queue) =>
        !string.IsNullOrWhiteSpace(queue) &&
        queue.Length <= 200 &&
        queue.All(c => char.IsLetterOrDigit(c) || c is '-' or '_' or '.');
}