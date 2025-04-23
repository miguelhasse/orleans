namespace BatchProcessing.Abstractions.Grains;
using Orleans;

[Alias("BatchProcessing.Abstractions.Grains.IBatchProcessManagerGrain")]
public interface IBatchProcessManagerGrain : IGrainWithIntegerKey
{
    [Alias("GetBatchProcesses")]
    public Task<IEnumerable<BatchProcessRecord>> GetBatchProcesses();

    [Alias("GetBatchProcess")]
    public Task<BatchProcessRecord?> GetBatchProcess(Guid engineId);
}