using System.Reflection;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Npgsql;
using Taakht.Common.V1;

namespace Taakht.Platform;

public static class ServiceCollectionExtensions
{
    public const string DefaultDatabaseUrl = "postgres://taakht:taakht@127.0.0.1:5432/postgres?sslmode=disable";
    public const string DefaultKafkaBrokers = "127.0.0.1:9094";

    /// <summary>
    /// Registers NpgsqlDataSource (DATABASE_URL, else ConnectionStrings:Default; pool bounded by DB_MAX_CONNS,
    /// DB_MIN_CONNS and DB_ACQUIRE_TIMEOUT, see <see cref="DatabasePoolSettings"/>), KafkaOptions (KAFKA_BROKERS)
    /// and the overload and identity interceptors as singletons. Add <see cref="OverloadInterceptor"/> to the gRPC
    /// server options first (outermost), then <see cref="ServerIdentityInterceptor"/>.
    /// </summary>
    public static IServiceCollection AddTaakhtPlatform(this IServiceCollection services, IConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(configuration);

        var dbUrl = configuration["DATABASE_URL"] ?? configuration.GetConnectionString("Default") ?? DefaultDatabaseUrl;
        var brokers = configuration["KAFKA_BROKERS"] ?? DefaultKafkaBrokers;

        var pool = DatabasePoolSettings.FromConfiguration(configuration);

        services.AddSingleton(pool);
        services.AddSingleton(_ => DatabaseUrl.CreateDataSource(dbUrl, pool));
        services.AddSingleton(new KafkaOptions(brokers));
        services.AddSingleton<OverloadInterceptor>();
        services.AddSingleton<ServerIdentityInterceptor>();
        services.AddSingleton<ClientIdentityInterceptor>();
        services.AddHostedService<InternalAuthWarning>();
        return services;
    }

    /// <summary>Adds the outbox relay hosted service.</summary>
    public static IServiceCollection AddTaakhtOutboxRelay(this IServiceCollection services)
        => services.AddHostedService<OutboxRelay>();

    /// <summary>
    /// Adds the housekeeping hosted service that prunes published outbox rows and old processed_events rows.
    /// Configured by OUTBOX_RETENTION (24h), PROCESSED_EVENTS_RETENTION (7d) and PRUNE_INTERVAL (10m); an invalid value
    /// fails host startup.
    /// </summary>
    public static IServiceCollection AddTaakhtHousekeeping(this IServiceCollection services)
    {
        services.AddSingleton(sp => HousekeepingOptions.FromConfiguration(sp.GetRequiredService<IConfiguration>()));
        return services.AddHostedService<HousekeepingService>();
    }

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
