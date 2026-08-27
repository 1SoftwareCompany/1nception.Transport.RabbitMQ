using System;

namespace One.Inception.Transport.RabbitMQ.DedicatedQueues;

[AttributeUsage(AttributeTargets.Class, AllowMultiple = false, Inherited = true)]
public class DedicatedQueueAttribute : Attribute
{

}
