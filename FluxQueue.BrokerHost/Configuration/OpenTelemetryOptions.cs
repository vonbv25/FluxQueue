namespace FluxQueue.BrokerHost.Configuration;

public sealed class OpenTelemetryOptions
{
    public const string SectionName = "OpenTelemetry";

    public string ServiceName { get; init; } = "FluxQueue.BrokerHost";
    public string? ServiceVersion { get; init; }
    public OtlpOptions Otlp { get; init; } = new();
    public ConsoleExporterOptions Console { get; set; } = new();

    public sealed class OtlpOptions
    {
        public bool Enabled { get; init; }
        public string? Endpoint { get; init; }
        public string? Headers { get; init; }
    }

    public sealed class ConsoleExporterOptions
    {
        public bool Enabled { get; set; }
        public bool MetricsEnabled { get; set; }
        public bool TracingEnabled { get; set; }
        public int MetricsExportIntervalMilliseconds { get; set; } = 5000;
    }

    public OpenTelemetryOptions ResolveFromEnvironment(IConfiguration configuration)
    {
        var otlpEndpoint = configuration["OTEL_EXPORTER_OTLP_ENDPOINT"];
        var otlpHeaders = configuration["OTEL_EXPORTER_OTLP_HEADERS"];
        var serviceName = configuration["OTEL_SERVICE_NAME"];

        var effectiveEndpoint = string.IsNullOrWhiteSpace(Otlp.Endpoint)
            ? otlpEndpoint
            : Otlp.Endpoint;

        var effectiveHeaders = string.IsNullOrWhiteSpace(Otlp.Headers)
            ? otlpHeaders
            : Otlp.Headers;

        return new OpenTelemetryOptions
        {
            ServiceName = string.IsNullOrWhiteSpace(serviceName) ? ServiceName : serviceName,
            ServiceVersion = ServiceVersion,
            Otlp = new OtlpOptions
            {
                Enabled = Otlp.Enabled || !string.IsNullOrWhiteSpace(effectiveEndpoint),
                Endpoint = effectiveEndpoint,
                Headers = effectiveHeaders
            }
        };
    }
}
