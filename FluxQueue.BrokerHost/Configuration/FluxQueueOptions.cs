using FluxQueue.BrokerHost.Services;
using FluxQueue.Transport.Amqp;
using System.ComponentModel.DataAnnotations;

namespace FluxQueue.BrokerHost.Configuration;

public sealed class FluxQueueOptions
{
    public const string SectionName = "FluxQueue";

    [Required]
    public string DbPath { get; set; } = "/data/rocksdb";

    [Required]
    public QueueReconcilerOptions Reconciler { get; set; } = new();

    [Required]
    public QueueSweeperOptions Sweeper { get; set; } = new();

    [Required]
    public AmqpTransportOptions Amqp { get; set; } = new();

    [Required]
    public FluxQueueDefaultsOptions Defaults { get; set; } = new();
}