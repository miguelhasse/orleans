using Orleans.Runtime.MembershipService.SiloMetadata;
using StackExchange.Redis;

var builder = WebApplication.CreateBuilder(args);

var redisConfig = ConfigurationOptions.Parse(builder.Configuration.GetConnectionString("redis")!);

builder.AddServiceDefaults();

builder.AddKeyedRedisClient("redis");

builder.UseOrleans(siloBuilder =>
{
    siloBuilder.AddDistributedGrainDirectory();

    siloBuilder.UseRedisClustering(options => options.ConfigurationOptions = redisConfig);
    siloBuilder.UseRedisGrainDirectoryAsDefault(options => options.ConfigurationOptions = redisConfig);
    siloBuilder.UseSiloMetadata();

    if (builder.Environment.IsDevelopment())
    {
        siloBuilder.UseDashboard(options => options.HostSelf = true);
    }
});

var app = builder.Build();

app.MapDefaultEndpoints();
app.Map("/dashboard", x => x.UseOrleansDashboard());

app.Run();
