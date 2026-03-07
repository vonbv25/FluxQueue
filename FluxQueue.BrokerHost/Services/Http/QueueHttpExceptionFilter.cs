namespace FluxQueue.Transport.Http;

internal sealed class QueueHttpExceptionFilter : IEndpointFilter
{
    public async ValueTask<object?> InvokeAsync(
        EndpointFilterInvocationContext context,
        EndpointFilterDelegate next)
    {
        try
        {
            return await next(context);
        }
        catch (ArgumentException ex)
        {
            return Results.BadRequest(new ErrorResponseDto(ex.Message));
        }
        catch (FormatException ex)
        {
            return Results.BadRequest(new ErrorResponseDto(ex.Message));
        }
        catch (OperationCanceledException)
        {
            return Results.StatusCode(StatusCodes.Status499ClientClosedRequest);
        }
        catch (Exception ex)
        {
            return Results.Problem(
                title: "An unexpected error occurred.",
                detail: ex.Message,
                statusCode: StatusCodes.Status500InternalServerError);
        }
    }
}