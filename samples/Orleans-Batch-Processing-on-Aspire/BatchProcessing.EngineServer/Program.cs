using BatchProcessing.Domain;
using BatchProcessing.EngineServer.Services;
using BatchProcessing.Grains;

var builder = WebApplication.CreateBuilder(args);

builder.AddServiceDefaults();

builder.AddDomainInfrastructure();
builder.AddBatchProcessingEngine();

builder.AddKeyedRedisClient("redis");

builder.UseOrleans(siloBuilder =>
{
    siloBuilder.UseSiloMetadataWithRegion(Environment.GetEnvironmentVariable("REGION_NAME")!);
    siloBuilder.UseRedisClustering("redis");

    if (builder.Environment.IsDevelopment())
    {
        siloBuilder.UseDashboard(options => options.HostSelf = false);
    }
});

//builder.Services.AddSingleton<ClusterDiagnosticsService>();

var app = builder.Build();

app.MapDefaultEndpoints();
//app.MapGet("/data.json", ([FromServices] ClusterDiagnosticsService clusterDiagnosticsService) => clusterDiagnosticsService.GetGrainCallFrequencies());

app.Run();
