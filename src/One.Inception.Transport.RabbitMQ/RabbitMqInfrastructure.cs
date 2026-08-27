using One.Inception.Transport.RabbitMQ.Management;
using One.Inception.Transport.RabbitMQ.Management.Model;
using One.Inception.Transport.RabbitMQ.Startup;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace One.Inception.Transport.RabbitMQ;

public class RabbitMqInfrastructure
{
    static readonly ILogger logger = InceptionLogger.CreateLogger(typeof(PublishedLanguageStartup));

    private readonly RabbitMqOptions options;
    private readonly PublicRabbitMqOptionsCollection publicRmqOptions;
    private readonly IRabbitMqNamer rabbitMqNamer;

    public RabbitMqInfrastructure(IOptionsMonitor<RabbitMqOptions> options, IOptionsMonitor<PublicRabbitMqOptionsCollection> publicOptions, IRabbitMqNamer rabbitMqNamer)
    {
        this.options = options.CurrentValue;
        this.publicRmqOptions = publicOptions.CurrentValue;
        this.rabbitMqNamer = rabbitMqNamer;
    }

    public async Task InitializeAsync()
    {
        try
        {
            RabbitMqManagementClient priv = new RabbitMqManagementClient(options);
            await CreateVHostAsync(priv, options).ConfigureAwait(false);

            foreach (var opt in publicRmqOptions.PublicClustersOptions)
            {
                RabbitMqManagementClient pub = new RabbitMqManagementClient(opt);
                await CreateVHostAsync(pub, opt).ConfigureAwait(false);
            }

            if (ChecksIfHavePublishedLanguageConfigurations() == false)
                logger.LogWarning("Missing configurations for public rabbitMq.");

            foreach (PublicRabbitMqOptions publicSettings in publicRmqOptions.PublicClustersOptions)
                await CreatePublishedLanguageConnection(priv, publicSettings).ConfigureAwait(false);
        }
        catch (Exception ex) when (True(() => logger.LogError(ex, ex.Message))) { }
    }

    private bool ChecksIfHavePublishedLanguageConfigurations()
    {
        // We are sure that if missing configurations for public rabbitMq VHost by default equals "/"
        return publicRmqOptions.PublicClustersOptions.Any();
    }

    private async Task CreateVHostAsync(RabbitMqManagementClient client, IRabbitMqOptions options)
    {
        var vhosts = await client.GetVHostsAsync().ConfigureAwait(false);
        if (!vhosts.Any(vh => vh.Name == options.VHost))
        {
            var vhost = await client.CreateVirtualHostAsync(options.VHost).ConfigureAwait(false);
            var rabbitMqUsers = await client.GetUsersAsync().ConfigureAwait(false);
            var rabbitMqUser = rabbitMqUsers.SingleOrDefault(x => x.Name == options.Username);
            var permissionInfo = new PermissionInfo(rabbitMqUser, vhost);
            await client.CreatePermissionAsync(permissionInfo).ConfigureAwait(false);
        }
    }

    private async Task CreatePublishedLanguageConnection(RabbitMqManagementClient downstreamClient, PublicRabbitMqOptions publicSettings)
    {
        var upstreams = publicSettings.GetUpstreamUris();
        if (upstreams.Any() == false)
            return;

        IEnumerable<string> publicExchangeNames = rabbitMqNamer.Get_FederationUpstream_ExchangeNames(typeof(IPublicEvent));
        IEnumerable<string> signalExchangeNames = rabbitMqNamer.Get_FederationUpstream_ExchangeNames(typeof(ISignal));
        IEnumerable<string> exchanges = publicExchangeNames.Concat(signalExchangeNames);

        foreach (var exchange in exchanges)
        {
            foreach (var upstream in upstreams)
            {
                FederatedExchange federatedExchange = new FederatedExchange()
                {
                    Name = publicSettings.VHost + $"--{exchange.ToLower()}",
                    Value = new FederatedExchange.ValueParameters()
                    {
                        Uri = upstream,
                        Exchange = exchange,
                        MaxHops = publicSettings.FederatedExchange.MaxHops
                    }
                };
                await downstreamClient.CreateFederatedExchangeAsync(federatedExchange, options.VHost).ConfigureAwait(false);
            }
        }

        foreach (var exchange in exchanges)
        {
            Policy policy = new Policy()
            {
                VHost = options.VHost,
                Name = publicSettings.VHost + $"--{exchange.ToLower()}",
                Pattern = $"^{exchange}$",
                Priority = 1,
                Definition = new Policy.DefinitionDto()
                {
                    FederationUpstream = publicSettings.VHost + $"--{exchange.ToLower()}"
                }
            };
            await downstreamClient.CreatePolicyAsync(policy, options.VHost).ConfigureAwait(false);
        }
    }
}
