namespace FluxQueue.Transport.Amqp;

using System.ComponentModel.DataAnnotations;

public sealed class AmqpTransportOptions
{
    public const string SectionName = "FluxQueue:Amqp";

    /// <summary>
    /// Enables the AMQP transport listener.
    /// </summary>
    public bool Enabled { get; set; } = true;

    /// <summary>
    /// Network interface to bind the AMQP listener to.
    /// </summary>
    [Required]
    public string BindAddress { get; set; } = "127.0.0.1";

    /// <summary>
    /// TCP port used by the AMQP listener.
    /// </summary>
    [Range(1, 65535)]
    public int Port { get; set; } = 5673;

    //
    // -------------------------
    // Queue receive behavior
    // -------------------------
    //

    /// <summary>
    /// Maximum number of messages returned in a single receive request.
    /// </summary>
    [Range(1, 1000)]
    public int MaxBatch { get; set; } = 50;

    /// <summary>
    /// Default visibility timeout used when receiving messages.
    /// </summary>
    [Range(1, 86400)]
    public int DefaultVisibilityTimeoutSeconds { get; set; } = 30;

    /// <summary>
    /// Default long-poll wait duration.
    /// </summary>
    [Range(0, 60)]
    public int DefaultWaitSeconds { get; set; } = 5;

    //
    // -------------------------
    // AMQP flow control
    // -------------------------
    //

    /// <summary>
    /// Initial credit for producer links (ingress).
    /// Controls how many messages a client can send before needing more credit.
    /// </summary>
    [Range(1, 10000)]
    public int IngressInitialCredit { get; set; } = 200;

    /// <summary>
    /// Initial credit for consumer links (egress).
    /// Controls how many messages the broker may push to a consumer.
    /// </summary>
    [Range(1, 10000)]
    public int EgressInitialCredit { get; set; } = 50;

    //
    // -------------------------
    // Operational tuning
    // -------------------------
    //

    /// <summary>
    /// Maximum concurrent AMQP connections.
    /// </summary>
    [Range(1, 10000)]
    public int MaxConnections { get; set; } = 1000;

    /// <summary>
    /// Maximum concurrent AMQP sessions per connection.
    /// </summary>
    [Range(1, 1000)]
    public int MaxSessionsPerConnection { get; set; } = 50;

    /// <summary>
    /// Maximum concurrent links per session.
    /// </summary>
    [Range(1, 1000)]
    public int MaxLinksPerSession { get; set; } = 100;

    //
    // -------------------------
    // Future TLS support
    // -------------------------
    //

    /// <summary>
    /// Enables TLS for AMQP connections.
    /// </summary>
    public bool EnableTls { get; set; } = false;

    /// <summary>
    /// Path to TLS certificate.
    /// </summary>
    public string? TlsCertificatePath { get; set; }

    /// <summary>
    /// TLS certificate password if required.
    /// </summary>
    public string? TlsCertificatePassword { get; set; }
}