using System;

namespace One.Inception.Transport.RabbitMQ;

internal static class MessageExtentions
{
    private const long TtlTreasholdMilliseconds = 30_000; // https://learn.microsoft.com/en-us/azure/virtual-machines/linux/time-sync

    /// <summary>
    /// Gets the <see cref="InceptionMessage"/> TTL calculate => Utc.Now - MessageHeaders[publish_timestamp].
    /// </summary>
    /// <param name="message">The <see cref="InceptionMessage"/>.</param>
    /// <returns>Returns the TTL in milliseconds.</returns>
    private static long GetPublishDelayMilliseconds(this InceptionMessage message)
    {
        if (message.Headers.TryGetValue(MessageHeader.PublishTimestamp, out string publishAt))
        {
            return (long)(DateTime.FromFileTimeUtc(long.Parse(publishAt)) - DateTime.UtcNow).TotalMilliseconds;
        }

        return 0;
    }

    /// <summary>
    /// Gets the <see cref="InceptionMessage"/> TTL from the MessageHeaders[ttl].
    /// </summary>
    /// <param name="message">The <see cref="InceptionMessage"/>.</param>
    /// <returns>Returns the TTL in milliseconds.</returns>
    private static string GetTtlMillisecondsFromHeader(this InceptionMessage message)
    {
        string ttl = string.Empty;
        message.Headers.TryGetValue(MessageHeader.TTL, out ttl);

        return ttl;
    }

    /// <summary>
    /// Gets the <see cref="InceptionMessage"/> TTL. Because there are 2 ways to set a message delay, <see cref="GetPublishDelayMilliseconds(InceptionMessage)"/> is with higher priority than
    /// <see cref="GetTtlMillisecondsFromHeader(InceptionMessage)"/>
    /// </summary>
    /// <param name="message">The <see cref="InceptionMessage"/>.</param>
    /// <returns>Returns the TTL in milliseconds.</returns>
    internal static string GetTtlMilliseconds(this InceptionMessage message)
    {
        long ttl = GetPublishDelayMilliseconds(message);
        if (ttl < TtlTreasholdMilliseconds)
        {
            string ttlFromHeader = GetTtlMillisecondsFromHeader(message);
            if (string.IsNullOrEmpty(ttlFromHeader))
                return string.Empty;

            ttl = long.Parse(ttlFromHeader);
            if (ttl < TtlTreasholdMilliseconds)
                return string.Empty;
        }

        if (ttl < TtlTreasholdMilliseconds)
            return string.Empty;

        return ttl.ToString();
    }
}
