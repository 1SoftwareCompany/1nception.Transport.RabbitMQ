﻿using System;
using System.ComponentModel.DataAnnotations;
using System.Diagnostics.CodeAnalysis;
using Microsoft.Extensions.Configuration;

namespace One.Inception.Transport.RabbitMQ;

public class RabbitMqConsumerOptions : IEquatable<RabbitMqConsumerOptions>
{
    [Range(1, int.MaxValue, ErrorMessage = "The configuration `Inception:Transport:RabbitMq:Consumer:WorkersCount` allows values from 1 to 2147483647.")]
    public int WorkersCount { get; set; } = 10;

    public int RpcTimeout { get; set; } = 10; // In seconds

    public int RpcWorkersCount { get; set; } = 10;

    /// <summary>
    /// Drasticly changes the infrastructure behavior. This will create a separate queue per node and a message will be delivered to every node.
    /// </summary>
    public bool FanoutMode { get; set; } = false;

    public override string ToString()
    {
        return $"WorkersCount: {WorkersCount}, RpcTimeout: {10} sec.";
    }

    public bool Equals([AllowNull] RabbitMqConsumerOptions other)
    {
        if (other is null)
            return false;

        return WorkersCount == other.WorkersCount && FanoutMode == other.FanoutMode && RpcTimeout == other.RpcTimeout;
    }

    public override bool Equals(object obj)
    {
        var other = (obj as RabbitMqConsumerOptions);
        return Equals(other);
    }

    public override int GetHashCode()
    {
        return HashCode.Combine(WorkersCount, FanoutMode, RpcTimeout);
    }

    public static bool operator ==(RabbitMqConsumerOptions left, RabbitMqConsumerOptions right)
    {
        if (left is null && right is null)
            return true;

        if (left is null || right is null)
            return false;

        return left.Equals(right);
    }

    public static bool operator !=(RabbitMqConsumerOptions left, RabbitMqConsumerOptions right)
    {
        return !(left == right);
    }
}

public class RabbitMqConsumerOptionsProvider : InceptionOptionsProviderBase<RabbitMqConsumerOptions>
{
    public const string SectionKey = "Inception:transport:rabbitmq:consumer";

    public RabbitMqConsumerOptionsProvider(IConfiguration configuration) : base(configuration) { }

    public override void Configure(RabbitMqConsumerOptions options)
    {
        configuration.GetSection(SectionKey).Bind(options);
    }
}
