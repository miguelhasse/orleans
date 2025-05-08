using BatchProcessing.Domain;
using BatchProcessing.Grains;
using Orleans.Configuration;
using StackExchange.Redis;

var builder = WebApplication.CreateBuilder(args);

var redisConfig = ConfigurationOptions.Parse(builder.Configuration.GetConnectionString("redis")!);
var dashboard = Environment.GetEnvironmentVariable("ORLEANS_DASHBOARD") == "HostSelf";

builder.AddServiceDefaults();

builder.AddDomainInfrastructure();
builder.AddBatchProcessingEngine();

builder.AddKeyedRedisClient("redis");

builder.UseOrleans(siloBuilder =>
{
    siloBuilder.AddActivityPropagation();
    //siloBuilder.AddActivationRepartitioner();
    siloBuilder.AddDistributedGrainDirectory();

    siloBuilder.UseRedisClustering(options => options.ConfigurationOptions = redisConfig);
    siloBuilder.UseRedisGrainDirectoryAsDefault(options => options.ConfigurationOptions = redisConfig);
    siloBuilder.UseSiloMetadataWithRegion(Environment.GetEnvironmentVariable("REGION_NAME")!);

    if (builder.Environment.IsDevelopment())
    {
        siloBuilder.UseDashboard(options => options.HostSelf = dashboard);
    }
});

//builder.Services.AddSingleton<ClusterDiagnosticsService>();

var app = builder.Build();

app.MapDefaultEndpoints();
//app.MapGet("/data.json", ([FromServices] ClusterDiagnosticsService clusterDiagnosticsService) => clusterDiagnosticsService.GetGrainCallFrequencies());

if (builder.Environment.IsDevelopment() && dashboard)
{
    app.Map("", x => x.UseOrleansDashboard());
}

app.Run();
