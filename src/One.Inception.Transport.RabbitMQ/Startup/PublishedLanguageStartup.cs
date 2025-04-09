﻿namespace One.Inception.Transport.RabbitMQ.Startup;

[InceptionStartup(Bootstraps.ExternalResource)]
public class PublishedLanguageStartup : IInceptionStartup
{
    private readonly RabbitMqInfrastructure infrastructure;

    public PublishedLanguageStartup(RabbitMqInfrastructure infrastructure)
    {
        this.infrastructure = infrastructure;
    }

    public void Bootstrap()
    {
        infrastructure.Initialize();
    }
}
