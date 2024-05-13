# See https://aka.ms/containerfastmode to understand how Visual Studio uses this
# Dockerfile to build your images for faster debugging.

FROM mcr.microsoft.com/dotnet/aspnet:8.0 AS base
WORKDIR /app

FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src
COPY ["EmqxLearning.MqttPublisher/EmqxLearning.MqttPublisher.csproj", "EmqxLearning.MqttPublisher/"]
RUN dotnet restore "EmqxLearning.MqttPublisher/EmqxLearning.MqttPublisher.csproj"
COPY . .
WORKDIR "/src/EmqxLearning.MqttPublisher"
RUN dotnet build "EmqxLearning.MqttPublisher.csproj" -c Release -o /app/build

FROM build AS publish
RUN dotnet publish "EmqxLearning.MqttPublisher.csproj" -c Release -o /app/publish

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .

ENTRYPOINT ["/bin/sh"]