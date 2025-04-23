using Orleans.Runtime.MembershipService.SiloMetadata;

var builder = WebApplication.CreateBuilder(args);

builder.AddServiceDefaults();

builder.AddKeyedRedisClient("redis");

builder.UseOrleans(orleansBuilder =>
{
    orleansBuilder.UseSiloMetadata();

    if (builder.Environment.IsDevelopment())
    {
        orleansBuilder.UseDashboard(options => options.HostSelf = true);
    }
});

var app = builder.Build();

app.MapDefaultEndpoints();
app.Map("/dashboard", x => x.UseOrleansDashboard());

app.Run();