using System;
using System.Linq;
using System.Collections.Generic;
using Microsoft.Extensions.Options;
using One.Inception.Transport.RabbitMQ.RpcAPI;
using Microsoft.Extensions.Logging;
using System.Threading.Tasks;

namespace One.Inception.Transport.RabbitMQ.Startup;

[InceptionStartup(Bootstraps.Runtime)]
public class RpcApiStartup : IInceptionStartup
{
    private InceptionHostOptions hostOptions;
    private readonly IRequestResponseFactory requestFactory;
    private readonly ILogger<RpcApiStartup> logger;

    public RpcApiStartup(IOptionsMonitor<InceptionHostOptions> InceptionHostOptions, IRequestResponseFactory requestFactory, ILogger<RpcApiStartup> logger)
    {
        this.hostOptions = InceptionHostOptions.CurrentValue;
        this.requestFactory = requestFactory;
        this.logger = logger;

        InceptionHostOptions.OnChange(options =>
        {
            if (logger.IsEnabled(LogLevel.Debug))
                logger.LogDebug("Host options re-loaded with {@options}", options);

            this.hostOptions = options;
        });
    }

    public Task BootstrapAsync()
    {
        if (hostOptions.RpcApiEnabled)
        {
            ILookup<Type, Type> handlers = GetHandlers();
            requestFactory.RegisterHandlers(handlers);

            return Task.CompletedTask;
        }

        logger.LogInformation("Rpc API feature disabled.");
        return Task.CompletedTask;
    }

    private static ILookup<Type, Type> GetHandlers()
    {
        ILookup<Type, Type> handlers = new DefaulAssemblyScanner()
             .Scan()
              .Where(t => t.IsAbstract == false)
                .Select(t => new
                {
                    HandlerType = t,
                    RequestTypes = GetHandledRequestTypes(t)
                })
                .Where(x => x.RequestTypes.Any())
                .SelectMany(p => p.RequestTypes.Select(r => new { p.HandlerType, RequestType = r }))
              .ToLookup(pair => pair.HandlerType, pair => pair.RequestType);

        return handlers;
    }

    private static IEnumerable<Type> GetHandledRequestTypes(Type type)
    {
        IEnumerable<Type> handlerInterfaces = type.GetInterfaces()
             .Where(i =>
                 i.IsGenericType &&
                 i.GetGenericTypeDefinition() == typeof(IRequestHandler<,>));

        return handlerInterfaces.Select(handlerInterface => handlerInterface is null ? null : handlerInterface.GetGenericArguments()[0]);
    }
}
