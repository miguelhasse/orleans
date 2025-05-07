using Orleans.Metadata;

namespace Orleans.Runtime;

[Serializable]
public class RegionDelegatingPlacement(PlacementStrategy innerStrategy) : PlacementStrategy
{
    public RegionDelegatingPlacement() : this(new RandomPlacement()) { }

    internal PlacementStrategy InnerStrategy { get; private set; } = innerStrategy;

    public override void Initialize(GrainProperties properties)
    {
        base.Initialize(properties);

        if (properties.Properties.TryGetValue(WellKnownGrainTypeProperties.PlacementStrategy, out var placementStrategy))
        {
            InnerStrategy = Activator.CreateInstance(Type.GetType(placementStrategy) ?? typeof(RandomPlacement)) as PlacementStrategy
                ?? throw new InvalidOperationException($"Unable to create instance of placement strategy {placementStrategy}");
        }
    }

    public override void PopulateGrainProperties(IServiceProvider services, Type grainClass, GrainType grainType, Dictionary<string, string> properties)
    {
        properties[WellKnownGrainTypeProperties.PlacementStrategy] = InnerStrategy.GetType().AssemblyQualifiedName!;

        base.PopulateGrainProperties(services, grainClass, grainType, properties);
    }
}
