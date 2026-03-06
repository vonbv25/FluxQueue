# Base runtime
FROM mcr.microsoft.com/dotnet/aspnet:9.0-bookworm-slim AS base

WORKDIR /app

ENV ASPNETCORE_URLS=http://0.0.0.0:8080
ENV FluxQueue__DbPath=/data/rocksdb

EXPOSE 8080
EXPOSE 5672

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       libc6 \
       libstdc++6 \
       libgcc-s1 \
       curl \
    && rm -rf /var/lib/apt/lists/* \
    && ln -sf /lib/x86_64-linux-gnu/libdl.so.2 /lib/x86_64-linux-gnu/libdl.so \
    && useradd -m -u 10001 appuser \
    && mkdir -p /data/rocksdb \
    && chown -R appuser:appuser /app /data

USER appuser

# Build stage
FROM mcr.microsoft.com/dotnet/sdk:9.0 AS build
ARG BUILD_CONFIGURATION=Release
WORKDIR /src

COPY ["FluxQueue.BrokerHost/FluxQueue.BrokerHost.csproj", "FluxQueue.BrokerHost/"]
RUN dotnet restore "./FluxQueue.BrokerHost/FluxQueue.BrokerHost.csproj"

COPY . .
WORKDIR "/src/FluxQueue.BrokerHost"
RUN dotnet build "./FluxQueue.BrokerHost.csproj" -c $BUILD_CONFIGURATION -o /app/build

# Publish stage
FROM build AS publish
ARG BUILD_CONFIGURATION=Release
RUN dotnet publish "./FluxQueue.BrokerHost.csproj" -c $BUILD_CONFIGURATION -o /app/publish /p:UseAppHost=false

# Final stage
FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish ./
ENTRYPOINT ["dotnet", "FluxQueue.BrokerHost.dll"]