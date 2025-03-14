using Orleans;
using Orleans.Concurrency;

namespace BatchProcessing.Abstractions.Grains;

[Alias("BatchProcessing.Abstractions.Grains.IEngineGrain")]
public interface IEngineGrain : IGrainWithGuidKey
{
    [OneWay]
    [Alias("RunAnalysis")]
    Task RunAnalysis(int recordsToSimulate);

    [Alias("GetStatus")]
    Task<EngineStatusRecord> GetStatus();
}