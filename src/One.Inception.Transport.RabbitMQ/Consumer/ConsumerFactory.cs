using System;
using System.Linq;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Collections.Concurrent;
using One.Inception.MessageProcessing;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using One.Inception.Transport.RabbitMQ.Startup;
using System.Threading;

namespace One.Inception.Transport.RabbitMQ;

public class ConsumerFactory<T>
{
    private readonly ILogger<ConsumerFactory<T>> logger;
    private readonly TypeContainer<ISaga> allSagas;
    private readonly TypeContainer<ITrigger> allTriggers;
    private readonly ConsumerPerQueueChannelResolver channelResolver;
    private readonly ISerializer serializer;
    private readonly RabbitMqConsumerOptions consumerOptions;
    private readonly BoundedContext boundedContext;
    private readonly ISubscriberCollection<T> subscriberCollection;
    private readonly SchedulePoker<T> schedulePoker;
    private readonly ConcurrentBag<AsyncConsumerBase> consumers = new ConcurrentBag<AsyncConsumerBase>();
    private readonly RabbitMqOptions options;
    private string queueName;

    public ConsumerFactory(TypeContainer<ISaga> allSagas, TypeContainer<ITrigger> allTriggers, IOptionsMonitor<RabbitMqOptions> optionsMonitor, ConsumerPerQueueChannelResolver channelResolver, IOptionsMonitor<RabbitMqConsumerOptions> consumerOptions, IOptionsMonitor<BoundedContext> boundedContext, ISerializer serializer, ISubscriberCollection<T> subscriberCollection, SchedulePoker<T> schedulePoker, ILogger<ConsumerFactory<T>> logger)
    {
        this.logger = logger;
        this.boundedContext = boundedContext.CurrentValue;
        this.allSagas = allSagas;
        this.allTriggers = allTriggers;
        this.channelResolver = channelResolver;
        this.serializer = serializer;
        this.consumerOptions = consumerOptions.CurrentValue;
        this.subscriberCollection = subscriberCollection;
        this.schedulePoker = schedulePoker;
        this.options = optionsMonitor.CurrentValue;

        queueName = GetQueueName(this.boundedContext.Name, this.consumerOptions.FanoutMode);
    }

    public async Task CreateAndStartConsumersAsync(CancellationToken cancellationToken)
    {
        bool isTrigger = typeof(T).IsAssignableFrom(typeof(ITrigger));

        if (isTrigger)
            await CreateAndStartTriggerConsumersAsync().ConfigureAwait(false);
        else
            await CreateAndStartNormalConsumersAsync().ConfigureAwait(false);

        await CreateAndStartSchedulePokerAsync(cancellationToken).ConfigureAwait(false);
    }

    private async Task CreateAndStartTriggerConsumersAsync()
    {
        IRabbitMqOptions scopedOptions = options.GetOptionsFor(boundedContext.Name);

        for (int i = 0; i < consumerOptions.WorkersCount; i++)
        {
            string consumerChannelKey = $"{boundedContext.Name}_{typeof(T).Name}_{i}";
            IChannel channel = await channelResolver.ResolveAsync(consumerChannelKey, scopedOptions, options.VHost).ConfigureAwait(false);

            AsyncConsumerBase<T> asyncListener = new AsyncConsumer<T>(queueName, channel, subscriberCollection, serializer, logger);
            consumers.Add(asyncListener);

            await asyncListener.StartAsync();
        }
    }

    private async Task CreateAndStartNormalConsumersAsync()
    {
        for (int i = 0; i < consumerOptions.WorkersCount; i++)
        {
            string consumerChannelKey = $"{boundedContext.Name}_{typeof(T).Name}_{i}";
            IChannel channel = await channelResolver.ResolveAsync(consumerChannelKey, options, options.VHost).ConfigureAwait(false);

            AsyncConsumerBase<T> asyncListener = new AsyncConsumer<T>(queueName, channel, subscriberCollection, serializer, logger);
            consumers.Add(asyncListener);

            await asyncListener.StartAsync();
        }

    }

    private async Task CreateAndStartSchedulePokerAsync(CancellationToken cancellationToken)
    {
        bool isSaga = typeof(ISaga).IsAssignableFrom(typeof(T));
        if (isSaga)
        {
            bool isSystemSaga = typeof(ISystemSaga).IsAssignableFrom(typeof(T));
            bool hasRegisteredSagas = allSagas.Items.Where(saga => typeof(ISystemSaga).IsAssignableFrom(saga) == isSystemSaga).Any();
            if (hasRegisteredSagas)
            {
                //This should not be awaited here to avoid deadlocks
                schedulePoker.PokeAsync(cancellationToken).ConfigureAwait(false);
            }
        }

        Type theType = typeof(T);
        bool isANormalTrigger = typeof(ITrigger).IsAssignableFrom(theType) && typeof(ISystemHandler).IsAssignableFrom(theType) == false;
        if (isANormalTrigger) // as of 18.08.25 there is no record for scheduled system signals to exists, so I am skipping it entirely, because there is no such queue ISystemTrigger.Scheduled when we try to start consumers and it throws exception.
        {
            var allNormalTriggers = allTriggers.Items.Where(justTrigger => typeof(ISystemHandler).IsAssignableFrom(justTrigger) == false);
            if (allNormalTriggers.Any())
            {
                //This should not be awaited here to avoid deadlocks
                schedulePoker.PokeAsync(cancellationToken).ConfigureAwait(false);
            }
        }
    }

    public async Task StopAsync()
    {
        IEnumerable<Task> stopTasks = consumers.Select(consumer => consumer.StopAsync());

        await Task.WhenAll(stopTasks).ConfigureAwait(false);

        subscriberCollection.UnsubscribeAll();
        consumers.Clear();
    }

    private string GetQueueName(string boundedContext, bool useFanoutMode = false)
    {
        if (useFanoutMode)
        {
            return $"{boundedContext}.{typeof(T).Name}.{Environment.MachineName}";
        }
        else
        {
            string systemMarker = typeof(ISystemHandler).IsAssignableFrom(typeof(T)) ? "inception." : string.Empty;
            return $"{boundedContext}.{systemMarker}{typeof(T).Name}";
        }
    }
}
