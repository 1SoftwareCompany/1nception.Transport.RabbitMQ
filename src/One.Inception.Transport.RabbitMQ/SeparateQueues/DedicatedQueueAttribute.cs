using System;

namespace One.Inception.Transport.RabbitMQ.SeparateQueues;

[AttributeUsage(AttributeTargets.Class, AllowMultiple = false, Inherited = true)]
public class DedicatedQueueAttribute : Attribute
{

}
