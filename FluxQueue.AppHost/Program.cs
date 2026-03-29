var builder = DistributedApplication.CreateBuilder(args);

builder.AddProject(
    name: "fluxqueue-broker",
    projectPath: @"..\FluxQueue.BrokerHost\FluxQueue.BrokerHost.csproj");

builder.Build().Run();
