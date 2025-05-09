namespace Orleans.Placement;

[AttributeUsage(AttributeTargets.Class, AllowMultiple = false)]
public class RegionPlacementFilterAttribute(int order = 0) : PlacementFilterAttribute(new RegionPlacementFilterStrategy(order));
