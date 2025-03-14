using BatchProcessing.Domain;
using BatchProcessing.EngineServer.Services;
using BatchProcessing.Grains;

var builder = WebApplication.CreateBuilder(args);

builder.AddServiceDefaults();

builder.AddDomainInfrastructure();

builder.Services.AddBatchProcessingEngineApplication(builder.Configuration);

builder.AddKeyedRedisClient("redis");

builder.UseOrleans(siloBuilder =>
{
    siloBuilder.UseSiloMetadataWithRegion(Environment.GetEnvironmentVariable("REGION_NAME")!);
    
    if (builder.Environment.IsDevelopment())
    {
        siloBuilder.UseDashboard(options =>
        {
            options.HostSelf = true;
            options.Port = Random.Shared.Next(10_000, 50_000);
        });
    }
});

//builder.Services.AddSingleton<ClusterDiagnosticsService>();

var app = builder.Build();

app.MapDefaultEndpoints();
//app.MapGet("/data.json", ([FromServices] ClusterDiagnosticsService clusterDiagnosticsService) => clusterDiagnosticsService.GetGrainCallFrequencies());

if (builder.Environment.IsDevelopment())
{
    app.Map("/dashboard", x => x.UseOrleansDashboard());
}

app.Run();
