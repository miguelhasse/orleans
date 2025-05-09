using Orleans.Placement;
using Orleans.Runtime.MembershipService.SiloMetadata;

namespace Orleans.Runtime.Placement;

internal class RegionPlacementFilterDirector(ISiloMetadataCache siloMetadataCache) : IPlacementFilterDirector
{
    public IEnumerable<SiloAddress> Filter(PlacementFilterStrategy filterStrategy, PlacementTarget target, IEnumerable<SiloAddress> silos)
    {
        var regionScope = target.RequestContextData?.TryGetValue(RegionDelegatingPlacement.RegionHintKey, out var hint) == true
            && hint is string regionHint ? regionHint : target.GrainIdentity.GetKeyExtension();

        return string.IsNullOrEmpty(regionScope) ? [] : FilterRegionCompatibleSilos(silos, regionScope);
    }

    private IEnumerable<SiloAddress> FilterRegionCompatibleSilos(IEnumerable<SiloAddress> addresses, string regionScope) => addresses.Where(s =>
            siloMetadataCache.GetSiloMetadata(s).Metadata.GetValueOrDefault(RegionDelegatingPlacement.RegionHintKey) == regionScope);
}
