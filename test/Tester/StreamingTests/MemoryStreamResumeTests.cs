using Microsoft.Extensions.Configuration;
using Orleans.Providers;
using Orleans.TestingHost;

namespace Tester.StreamingTests
{
    [TestCategory("SlowBVT"), TestCategory("Streaming"), TestCategory("StreamingResume")]
    public class MemoryStreamResumeTests : StreamingResumeTests
    {
        protected override void ConfigureTestCluster(TestClusterBuilder builder)
        {
            builder.AddSiloBuilderConfigurator<MySiloBuilderConfigurator>();
            builder.AddClientBuilderConfigurator<MyClientBuilderConfigurator>();
        }

        #region Configuration stuff
        private class MySiloBuilderConfigurator : ISiloConfigurator
        {
            public void Configure(ISiloBuilder hostBuilder)
            {
                hostBuilder
                    .AddMemoryGrainStorageAsDefault()
                    .AddMemoryGrainStorage("PubSubStore")
                    .AddMemoryStreams<DefaultMemoryMessageBodySerializer>(StreamProviderName, b =>
                    {
                        b.ConfigurePullingAgent(ob => ob.Configure(options =>
                        {
                            options.StreamInactivityPeriod = StreamInactivityPeriod;
                            options.MaxEventDeliveryTime = TimeSpan.FromSeconds(2);
                        }));
                        b.ConfigureCacheEviction(op => op.Configure(opt =>
                        {
                            opt.DataMinTimeInCache = TimeSpan.FromSeconds(3);
                            opt.DataMaxAgeInCache = TimeSpan.FromSeconds(5);
                            opt.MetadataMinTimeInCache = TimeSpan.FromSeconds(7);
                        }));
                    });
            }
        }

        private class MyClientBuilderConfigurator : IClientBuilderConfigurator
        {
            public void Configure(IConfiguration configuration, IClientBuilder clientBuilder)
            {
                clientBuilder
                    .AddMemoryStreams<DefaultMemoryMessageBodySerializer>(StreamProviderName);
            }
        }

        #endregion
    }
}
