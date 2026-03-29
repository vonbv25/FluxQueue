using FluxQueue.BrokerHost.Configuration;
using FluxQueue.Transport.Abstractions;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;

namespace FluxQueue.BrokerHost.Telemetry;

public static class OpenTelemetryConfiguration
{
    public static WebApplicationBuilder AddFluxQueueTelemetry(this WebApplicationBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(builder);

        builder.Services
            .AddOptions<OpenTelemetryOptions>()
            .Bind(builder.Configuration.GetSection(OpenTelemetryOptions.SectionName));

        var options = builder.Configuration
            .GetSection(OpenTelemetryOptions.SectionName)
            .Get<OpenTelemetryOptions>() ?? new OpenTelemetryOptions();

        var serviceVersion = string.IsNullOrWhiteSpace(options.ServiceVersion)
            ? typeof(OpenTelemetryConfiguration).Assembly.GetName().Version?.ToString()
            : options.ServiceVersion;

        builder.Services
            .AddOpenTelemetry()
            .ConfigureResource(resource =>
            {
                resource
                    .AddService(
                        serviceName: options.ServiceName,
                        serviceVersion: serviceVersion,
                        serviceInstanceId: Environment.MachineName)
                    .AddAttributes(
                    [
                        new KeyValuePair<string, object>(
                            "deployment.environment",
                            builder.Environment.EnvironmentName ?? "Uknown")
                    ]);
            })
            .WithTracing(tracing =>
            {
                tracing
                    .AddSource(FluxQueueTelemetry.ActivitySourceName)
                    .AddAspNetCoreInstrumentation(options =>
                    {
                        options.RecordException = true;
                    });

                ConfigureTraceExporters(tracing, options);
            })
            .WithMetrics(metrics =>
            {
                metrics
                    .AddMeter(FluxQueueTelemetry.MeterName)
                    .AddAspNetCoreInstrumentation()
                    .AddRuntimeInstrumentation();

                ConfigureMetricExporters(metrics, options);
            });

        return builder;
    }

    private static void ConfigureTraceExporters(
        TracerProviderBuilder tracing,
        OpenTelemetryOptions options)
    {
        if (options.Otlp.Enabled &&
            Uri.TryCreate(options.Otlp.Endpoint, UriKind.Absolute, out var endpoint))
        {
            tracing.AddOtlpExporter(exporter =>
            {
                exporter.Endpoint = endpoint;
                exporter.Headers = options.Otlp.Headers;
            });
        }
    }

    private static void ConfigureMetricExporters(
        MeterProviderBuilder metrics,
        OpenTelemetryOptions options)
    {
        if (options.Otlp.Enabled &&
            Uri.TryCreate(options.Otlp.Endpoint, UriKind.Absolute, out var endpoint))
        {
            metrics.AddOtlpExporter(exporter =>
            {
                exporter.Endpoint = endpoint;
                exporter.Headers = options.Otlp.Headers;
            });
        }
    }
}