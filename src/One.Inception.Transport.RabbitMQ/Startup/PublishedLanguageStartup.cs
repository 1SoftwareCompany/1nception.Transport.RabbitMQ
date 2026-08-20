using System.Collections.Generic;
using System.Threading.Tasks;

namespace One.Inception.Transport.RabbitMQ.Startup;

[InceptionStartup(Bootstraps.ExternalResource)]
public class PublishedLanguageStartup : IInceptionStartup
{
    private readonly RabbitMqInfrastructure infrastructure;

    public PublishedLanguageStartup(RabbitMqInfrastructure infrastructure)
    {
        this.infrastructure = infrastructure;
    }

    public async Task BootstrapAsync()
    {
        await infrastructure.InitializeAsync();
    }

    public Task BootstrapAsync(IEnumerable<string> tenants)
    {
        return Task.CompletedTask;
    }
}
