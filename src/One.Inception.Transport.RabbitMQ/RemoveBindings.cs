using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json.Serialization;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using One.Inception.Transport.RabbitMQ.Management;

namespace One.Inception.Transport.RabbitMQ;
public class RemoveBindings
{
    private readonly RabbitMqOptions options;
    private readonly ILogger<RemoveBindings> logger;

    public RemoveBindings(IOptionsMonitor<RabbitMqOptions> provider, ILogger<RemoveBindings> logger)
    {
        this.options = provider.CurrentValue;
        this.logger = logger;
    }

    public async Task RemovePublicBindingsForTenant(string tenant)
    {
        try
        {
            RabbitMqManagementClient rmqClient = new RabbitMqManagementClient(this.options);

            string theTenantSuffix = $"@{tenant}";

            IEnumerable<Exchange> allExchanges = await rmqClient.GetExchangesForVHost(this.options.VHost).ConfigureAwait(false);
            foreach (var exchange in allExchanges)
            {
                var allBindings = await rmqClient.GetAllBindingsForExchange(this.options.VHost, exchange.Name).ConfigureAwait(false);

                var bindingsThatWillBeDeleted = allBindings.Where(x => x.Arguments.Keys.Any(k => k.EndsWith(theTenantSuffix, StringComparison.OrdinalIgnoreCase)));
                foreach (var ripBinding in bindingsThatWillBeDeleted)
                {
                    await rmqClient.DeleteBindingAsync(ripBinding).ConfigureAwait(false);
                }

                logger.LogInformation($"Deleted {bindingsThatWillBeDeleted?.Count()} bindings for exchange with name {exchange?.Name}");
            }

            logger.LogInformation("Finished deleting tenant bindings for tenant {tenant}", tenant);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Error while trying to remove bindings for tenant, {tenant}", tenant);
        }
    }
}

public sealed record Exchange
{
    [JsonPropertyName("name")]
    public string Name { get; set; }
}
