using System.Reflection;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Npgsql;
using Taakht.Common.V1;

namespace Taakht.Platform;

public static class ServiceCollectionExtensions
{
    public const string DefaultDatabaseUrl = "postgres://taakht:taakht@localhost:5432/postgres?sslmode=disable";
    public const string DefaultKafkaBrokers = "localhost:9094";

    /// <summary>
    /// Registers NpgsqlDataSource (DATABASE_URL, else ConnectionStrings:Default), KafkaOptions (KAFKA_BROKERS)
    /// and the identity interceptors as singletons.
    /// </summary>
    public static IServiceCollection AddTaakhtPlatform(this IServiceCollection services, IConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(configuration);

        var dbUrl = configuration["DATABASE_URL"] ?? configuration.GetConnectionString("Default") ?? DefaultDatabaseUrl;
        var brokers = configuration["KAFKA_BROKERS"] ?? DefaultKafkaBrokers;

        services.AddSingleton(_ => DatabaseUrl.CreateDataSource(dbUrl));
        services.AddSingleton(new KafkaOptions(brokers));
        services.AddSingleton<ServerIdentityInterceptor>();
        services.AddSingleton<ClientIdentityInterceptor>();
        return services;
    }

    /// <summary>Adds the outbox relay hosted service.</summary>
    public static IServiceCollection AddTaakhtOutboxRelay(this IServiceCollection services)
        => services.AddHostedService<OutboxRelay>();

    /// <summary>Adds an event consumer hosted service. Handlers are keyed by envelope type (full proto name).</summary>
    public static IServiceCollection AddTaakhtEventConsumer(
        this IServiceCollection services,
        string group,
        IReadOnlyCollection<string> topics,
        IReadOnlyDictionary<string, Func<NpgsqlConnection, NpgsqlTransaction, Envelope, Task>> handlers)
        => services.AddHostedService(sp => new EventConsumer(
            sp.GetRequiredService<NpgsqlDataSource>(),
            sp.GetRequiredService<KafkaOptions>(),
            group,
            topics,
            handlers,
            sp.GetRequiredService<ILogger<EventConsumer>>()));

    /// <summary>Applies the embedded migrations of <paramref name="assembly"/>; call before the host starts serving.</summary>
    public static Task MigrateAsync(this IServiceProvider services, Assembly assembly, CancellationToken ct = default)
        => Migrator.MigrateAsync(services.GetRequiredService<NpgsqlDataSource>(), assembly, ct);
}
