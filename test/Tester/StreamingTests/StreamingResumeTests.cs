using Orleans.Streams;
using TestExtensions;
using UnitTests.GrainInterfaces;
using Xunit;

namespace Tester.StreamingTests
{
    public abstract class StreamingResumeTests : TestClusterPerTest
    {
        protected static readonly TimeSpan StreamInactivityPeriod = TimeSpan.FromSeconds(5);

        protected const string StreamProviderName = "StreamingCacheMissTests";

        [SkippableFact]
        public virtual async Task ResumeAfterInactivity()
        {
            var streamProvider = this.Client.GetStreamProvider(StreamProviderName);

            // Tested stream and corresponding grain
            var key = Guid.NewGuid();
            var stream = streamProvider.GetStream<byte[]>(nameof(IImplicitSubscriptionCounterGrain), key);
            var grain = this.Client.GetGrain<IImplicitSubscriptionCounterGrain>(key);

            // Data that will be sent to the grains
            var interestingData = new byte[1] { 1 };
            await stream.OnNextAsync(interestingData);

            await Task.Delay(1_000);

            // Wait for the stream to become inactive
            await Task.Delay(StreamInactivityPeriod.Multiply(3));

            var interestingData2 = new byte[1] { 2 };
            await stream.OnNextAsync(interestingData2);

            await Task.Delay(2_000);

            var grainInts = await grain.GetInts();
            Assert.Contains(1, grainInts);
            Assert.Contains(2, grainInts);
            Assert.Equal(0, await grain.GetErrorCounter());
            Assert.Equal(2, await grain.GetEventCounter());
        }

        [SkippableFact]
        public virtual async Task ResumeAfterInactivityAndCacheMiss()
        {
            var streamProvider = this.Client.GetStreamProvider(StreamProviderName);

            // Tested stream and corresponding grain
            var key = Guid.NewGuid();
            var stream = streamProvider.GetStream<byte[]>(nameof(IImplicitSubscriptionCounterGrain), key);
            var grain = this.Client.GetGrain<IImplicitSubscriptionCounterGrain>(key);

            // Data that will be sent to the grains
            var interestingData = new byte[1] { 1 };

            await stream.OnNextAsync(interestingData);

            await Task.Delay(1_000);

            // Wait for the stream to become inactive
            await Task.Delay(StreamInactivityPeriod.Multiply(3));

            var interestingData2 = new byte[1] { 2 };
            await stream.OnNextAsync(interestingData2);

            await Task.Delay(2_000);

            var grainInts = await grain.GetInts();
            Assert.Contains(1, grainInts);
            Assert.Contains(2, grainInts);
            Assert.Equal(0, await grain.GetErrorCounter());
            Assert.Equal(2, await grain.GetEventCounter());
        }

        [SkippableFact]
        public virtual async Task ResumeAfterDeactivation()
        {
            var streamProvider = this.Client.GetStreamProvider(StreamProviderName);

            // Tested stream and corresponding grain
            var key = Guid.NewGuid();
            var stream = streamProvider.GetStream<byte[]>(nameof(IImplicitSubscriptionCounterGrain), key);
            var grain = this.Client.GetGrain<IImplicitSubscriptionCounterGrain>(key);

            // Data that will be sent to the grains
            var interestingData = new byte[1] { 1 };

            await stream.OnNextAsync(interestingData);

            await Task.Delay(1_000);

            // Wait for the stream to become inactive
            await Task.Delay(StreamInactivityPeriod.Multiply(3));
            await grain.Deactivate();

            var interestingData2 = new byte[1] { 2 };
            await stream.OnNextAsync(interestingData2);

            await Task.Delay(2_000);

            var grainInts = await grain.GetInts();
            Assert.Contains(1, grainInts);
            Assert.Contains(2, grainInts);
            Assert.Equal(0, await grain.GetErrorCounter());
            Assert.Equal(2, await grain.GetEventCounter());
        }

        [SkippableFact]
        public virtual async Task ResumeAfterDeactivationActiveStream()
        {
            var streamProvider = this.Client.GetStreamProvider(StreamProviderName);

            // Tested stream and corresponding grain
            var key = Guid.NewGuid();
            var stream = streamProvider.GetStream<byte[]>(nameof(IImplicitSubscriptionCounterGrain), key);
            var otherStream = streamProvider.GetStream<byte[]>(nameof(IImplicitSubscriptionCounterGrain), Guid.NewGuid());
            var grain = this.Client.GetGrain<IImplicitSubscriptionCounterGrain>(key);
            await grain.DeactivateOnEvent(true);

            // Data that will be sent to the grains
            var interestingData = new byte[1] { 1 };

            await stream.OnNextAsync(interestingData);
            // Push other data
            await otherStream.OnNextAsync(interestingData);
            await otherStream.OnNextAsync(interestingData);
            await otherStream.OnNextAsync(interestingData);

            var interestingData2 = new byte[1] { 2 };
            await stream.OnNextAsync(interestingData2);

            await Task.Delay(1_000);

            // Wait for the stream to become inactive
            await Task.Delay(StreamInactivityPeriod.Multiply(3));
            await grain.Deactivate();

            var interestingData3 = new byte[1] { 3 };
            await stream.OnNextAsync(interestingData3);

            await Task.Delay(2_000);

            var grainInts = await grain.GetInts();
            Assert.Contains(1, grainInts);
            Assert.Contains(2, grainInts);
            Assert.Contains(3, grainInts);
            Assert.Equal(0, await grain.GetErrorCounter());
            Assert.Equal(3, await grain.GetEventCounter());
        }
    }
}