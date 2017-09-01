using System;
using System.IO;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;

namespace Elders.Cronus.Pipeline.Transport.RabbitMQ
{
    public class RabbitMqSafeChannel
    {
        IModel channel;

        readonly RetryPolicy retryPolicy = RetryableOperation.RetryPolicyFactory.CreateInfiniteLinearRetryPolicy(new TimeSpan(500000));

        readonly RabbitMqSession session;

        public RabbitMqSafeChannel(RabbitMqSession session)
        {
            this.session = session;
            channel = session.OpenChannel();
        }

        public IModel Channel
        {
            get
            {
                try
                {
                    Reconnect();
                    return channel;
                }
                catch (EndOfStreamException)
                {
                    RetryableOperation.TryExecute<bool>(Reconnect, retryPolicy);
                    return channel;
                }
                catch (AlreadyClosedException)
                {
                    RetryableOperation.TryExecute<bool>(Reconnect, retryPolicy);
                    return channel;
                }
                catch (OperationInterruptedException)
                {
                    RetryableOperation.TryExecute<bool>(Reconnect, retryPolicy);
                    return channel;
                }
            }
        }
        public bool Reconnect()
        {
            if (channel != null && channel.IsOpen)
                return true;
            channel = session.OpenChannel();
            return true;
        }

        public void Close()
        {
            if (channel != null)
            {
                lock (channel)
                {
                    channel?.Close();
                    channel?.Dispose();
                    channel = null;
                }
            }
        }
    }
}
