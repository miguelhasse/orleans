using Orleans.Runtime.MembershipService.SiloMetadata;

namespace Orleans.Runtime.Placement;

internal class RegionBasedPlacementDirector(ISiloMetadataCache siloMetadataCache) : IPlacementDirector
{
    public const string RegionHintKey = "cloud.region";

    public Task<SiloAddress> OnAddActivation(PlacementStrategy strategy, PlacementTarget target, IPlacementContext context)
    {
        var regionScope = target.RequestContextData[RegionHintKey] as string ?? target.GrainIdentity.GetKeyExtension();

        var compatibleSilos = context.GetCompatibleSilos(target)
            .Where(s => siloMetadataCache.GetSiloMetadata(s).Metadata.GetValueOrDefault(RegionHintKey) == regionScope)
            .ToArray();

        // If a valid placement hint was specified, use it.
        if (IPlacementDirector.GetPlacementHint(target.RequestContextData, compatibleSilos) is { } placementHint)
        {
            return Task.FromResult(placementHint);
        }

        return Task.FromResult(compatibleSilos[Random.Shared.Next(compatibleSilos.Length)]);
    }
}
