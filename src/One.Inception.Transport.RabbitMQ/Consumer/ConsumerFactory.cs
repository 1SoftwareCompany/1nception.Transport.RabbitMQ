using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using One.Inception.MessageProcessing;
using One.Inception.Transport.RabbitMQ.DedicatedQueues;
using One.Inception.Transport.RabbitMQ.Startup;
using RabbitMQ.Client;

namespace One.Inception.Transport.RabbitMQ;

public class ConsumerFactory<T>
{
    private readonly ILogger<ConsumerFactory<T>> logger;
    private readonly TypeContainer<IProcessManager> allProcessManagers;
    private readonly TypeContainer<ITrigger> allTriggers;
    private readonly ConsumerPerQueueChannelResolver channelResolver;
    private readonly ISerializer serializer;
    private readonly RabbitMqConsumerOptions consumerOptions;
    private readonly BoundedContext boundedContext;
    private readonly ISubscriberCollection<T> subscriberCollection;
    private readonly SchedulePoker<T> schedulePoker;
    private readonly ConcurrentBag<AsyncConsumerBase> consumers = new ConcurrentBag<AsyncConsumerBase>();
    private readonly RabbitMqOptions options;
    private readonly BoundedContextRabbitMqNamer bcRabbitMqNamer;
    private string queueName;

    public ConsumerFactory(TypeContainer<IProcessManager> allProcessManagers, TypeContainer<ITrigger> allTriggers, IOptionsMonitor<RabbitMqOptions> optionsMonitor, ConsumerPerQueueChannelResolver channelResolver, IOptionsMonitor<RabbitMqConsumerOptions> consumerOptions, IOptionsMonitor<BoundedContext> boundedContext, ISerializer serializer, ISubscriberCollection<T> subscriberCollection, SchedulePoker<T> schedulePoker, BoundedContextRabbitMqNamer bcRabbitMqNamer, ILogger<ConsumerFactory<T>> logger)
    {
        this.logger = logger;
        this.boundedContext = boundedContext.CurrentValue;
        this.allProcessManagers = allProcessManagers;
        this.allTriggers = allTriggers;
        this.channelResolver = channelResolver;
        this.serializer = serializer;
        this.consumerOptions = consumerOptions.CurrentValue;
        this.subscriberCollection = subscriberCollection;
        this.schedulePoker = schedulePoker;
        this.options = optionsMonitor.CurrentValue;
        this.bcRabbitMqNamer = bcRabbitMqNamer;

        queueName = bcRabbitMqNamer.Get_QueueName(typeof(T), this.consumerOptions.FanoutMode);
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

        IEnumerable<ISubscriber> subscribersWithDedicatedQueues = subscriberCollection.Subscribers.SubscribersWithDedicatedQueuesOnly();
        if (subscribersWithDedicatedQueues.Any())
        {
            foreach (var subscriber in subscribersWithDedicatedQueues)
            {
                string queueName = bcRabbitMqNamer.Get_QueueName(subscriber.HandlerType, consumerOptions.FanoutMode);

                for (int i = 0; i < consumerOptions.WorkersCount; i++)
                {
                    string consumerChannelKey = $"{boundedContext.Name}_{subscriber.HandlerType.Name}_{i}";
                    IChannel channel = await channelResolver.ResolveAsync(consumerChannelKey, scopedOptions, options.VHost).ConfigureAwait(false);

                    AsyncConsumerForSingleSubscriber asyncListener = new AsyncConsumerForSingleSubscriber(queueName, channel, subscriber, serializer, logger);

                    consumers.Add(asyncListener);

                    await asyncListener.StartAsync();
                }
            }
        }

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
        IEnumerable<ISubscriber> subscribersWithDedicatedQueues = subscriberCollection.Subscribers.SubscribersWithDedicatedQueuesOnly();
        if (subscribersWithDedicatedQueues.Any())
        {
            foreach (ISubscriber subscriber in subscribersWithDedicatedQueues)
            {
                string queueName = bcRabbitMqNamer.Get_QueueName(subscriber.HandlerType, consumerOptions.FanoutMode);
                for (int i = 0; i < consumerOptions.WorkersCount; i++)
                {
                    string consumerChannelKey = $"{boundedContext.Name}_{subscriber.HandlerType.Name}_{i}";
                    IChannel channel = await channelResolver.ResolveAsync(consumerChannelKey, options, options.VHost).ConfigureAwait(false);

                    AsyncConsumerForSingleSubscriber asyncListener = new AsyncConsumerForSingleSubscriber(queueName, channel, subscriber, serializer, logger);
                    consumers.Add(asyncListener);

                    await asyncListener.StartAsync();
                }
            }
        }

        var theRestOfTheSubscribers = subscriberCollection.Subscribers.Except(subscribersWithDedicatedQueues);
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
        bool isProcessManager = typeof(IProcessManager).IsAssignableFrom(typeof(T));
        if (isProcessManager)
        {
            bool isSystemProcessManager = typeof(ISystemProcessManager).IsAssignableFrom(typeof(T));
            IEnumerable<Type> registeredSagas = allProcessManagers.Items.Where(processManager => typeof(ISystemProcessManager).IsAssignableFrom(processManager) == isSystemProcessManager);

            bool hasRegisteredProcessManager = registeredSagas.Any();
            if (hasRegisteredProcessManager)
            {
                IEnumerable<ISubscriber> subscribersWithDedicatedQueues = subscriberCollection.Subscribers.SubscribersWithDedicatedQueuesOnly();
                foreach (var subscriber in subscribersWithDedicatedQueues)
                {
                    string seperateQueueName = $"{bcRabbitMqNamer.Get_QueueName(subscriber.HandlerType, consumerOptions.FanoutMode)}.Scheduled";
                    schedulePoker.PokeAsync(seperateQueueName, cancellationToken).ConfigureAwait(false);
                }

                bool thereAreAnyRemainig = registeredSagas.Except(subscribersWithDedicatedQueues.Select(x => x.HandlerType)).Any();
                if (thereAreAnyRemainig)
                {
                    string queueNameScheduled = $"{queueName}.Scheduled";

                    //This should not be awaited here to avoid deadlocks
                    schedulePoker.PokeAsync(queueNameScheduled, cancellationToken).ConfigureAwait(false);
                }
            }
        }

        Type theType = typeof(T);
        bool isANormalTrigger = typeof(ITrigger).IsAssignableFrom(theType) && typeof(ISystemHandler).IsAssignableFrom(theType) == false;
        if (isANormalTrigger) // as of 18.08.25 there is no record for scheduled system signals to exists, so I am skipping it entirely, because there is no such queue ISystemTrigger.Scheduled when we try to start consumers and it throws exception.
        {
            var allNormalTriggers = allTriggers.Items.Where(justTrigger => typeof(ISystemHandler).IsAssignableFrom(justTrigger) == false);
            if (allNormalTriggers.Any())
            {
                IEnumerable<ISubscriber> subscribersWithDedicatedQueues = subscriberCollection.Subscribers.SubscribersWithDedicatedQueuesOnly();
                foreach (var subscriber in subscribersWithDedicatedQueues)
                {
                    string queueName = $"{bcRabbitMqNamer.Get_QueueName(subscriber.HandlerType, consumerOptions.FanoutMode)}.Scheduled";
                    schedulePoker.PokeAsync(queueName, cancellationToken).ConfigureAwait(false);
                }

                string queueNameRegular = $"{queueName}.Scheduled";
                //This should not be awaited here to avoid deadlocks
                schedulePoker.PokeAsync(queueNameRegular, cancellationToken).ConfigureAwait(false);
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
}
