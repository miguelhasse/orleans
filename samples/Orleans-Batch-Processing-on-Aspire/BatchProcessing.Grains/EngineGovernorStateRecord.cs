using BatchProcessing.Abstractions.Grains;
using BatchProcessing.Shared;

namespace BatchProcessing.Grains;

[GenerateSerializer]
public class EngineGovernorStateRecord(EngineStatusRecord engineStatus, DateTime createdOn, DateTime lastUpdated)
{
    [Id(0)]
    public EngineStatusRecord EngineStatus { get; private set; } = engineStatus;

    [Id(1)]
    public DateTime LastUpdated { get; private set; } = lastUpdated;

    [Id(2)]
    public DateTime CreatedOn { get; private set; } = createdOn;

    public void SetLastUpdated(DateTime lastUpdated) => LastUpdated = lastUpdated;

    public void SetState(AnalysisStatus newState)
    {
        EngineStatus = EngineStatus with { Status = newState };
        SetLastUpdated(DateTime.UtcNow);
    }
}
