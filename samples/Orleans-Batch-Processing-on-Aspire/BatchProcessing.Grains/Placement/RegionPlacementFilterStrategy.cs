namespace Orleans.Placement;

internal class RegionPlacementFilterStrategy(int order) : PlacementFilterStrategy(order)
{
    public RegionPlacementFilterStrategy() : this(0) { }
}
