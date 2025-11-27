# See https://aka.ms/customizecontainer to learn how to customize your debug container and how Visual Studio uses this Dockerfile to build your images for faster debugging.

# This stage is used when running from VS in fast mode (Default for Debug configuration)
FROM mcr.microsoft.com/dotnet/aspnet:9.0 AS base
USER $APP_UID
WORKDIR /app
EXPOSE 8080
EXPOSE 8081


# This stage is used to build the service project
FROM mcr.microsoft.com/dotnet/sdk:9.0 AS build
ARG BUILD_CONFIGURATION=Release
WORKDIR /src
COPY ["MongoMigrationWebApp/MongoMigrationWebApp.csproj", "MongoMigrationWebApp/"]
COPY ["OnlineMongoMigrationProcessor/OnlineMongoMigrationProcessor.csproj", "OnlineMongoMigrationProcessor/"]
RUN dotnet restore "./MongoMigrationWebApp/MongoMigrationWebApp.csproj"
COPY . .
WORKDIR "/src/MongoMigrationWebApp"
RUN dotnet build "./MongoMigrationWebApp.csproj" -c $BUILD_CONFIGURATION -o /app/build

# This stage is used to publish the service project to be copied to the final stage
FROM build AS publish
ARG BUILD_CONFIGURATION=Release
RUN dotnet publish "./MongoMigrationWebApp.csproj" -c $BUILD_CONFIGURATION -o /app/publish /p:UseAppHost=false

# This stage is used in production or when running from VS in regular mode (Default when not using the Debug configuration)
FROM base AS final

# Switch to root to install MongoDB tools
USER root

# Install MongoDB Database Tools
RUN apt-get update && \
    apt-get install -y wget gnupg && \
    wget -qO - https://www.mongodb.org/static/pgp/server-7.0.asc | apt-key add - && \
    echo "deb [ arch=amd64,arm64 ] https://repo.mongodb.org/apt/debian bookworm/mongodb-org/7.0 main" | tee /etc/apt/sources.list.d/mongodb-org-7.0.list && \
    apt-get update && \
    apt-get install -y mongodb-database-tools && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Switch back to non-root user for security
USER $APP_UID

WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "MongoMigrationWebApp.dll"]