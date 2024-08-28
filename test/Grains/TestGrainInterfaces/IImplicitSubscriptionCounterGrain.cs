namespace UnitTests.GrainInterfaces
{
    public interface IImplicitSubscriptionCounterGrain : IGrainWithGuidKey
    {
        Task<int> GetEventCounter();

        Task<int> GetErrorCounter();

        Task<List<int>> GetInts();

        Task Deactivate();

        Task DeactivateOnEvent(bool deactivate);
    }
}