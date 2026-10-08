using Npgsql;
using Taakht.Platform;

namespace Taakht.Negotiation.Tests.Integration;

/// <summary>
/// Creates a throwaway database on the server named by TEST_DATABASE_URL, migrates it and drops it afterwards.
/// <see cref="DataSource"/> is null when the variable is not set (tests then skip themselves).
/// </summary>
public sealed class PostgresFixture : IAsyncLifetime
{
    private string? _adminConnectionString;
    private string? _databaseName;

    public NpgsqlDataSource? DataSource { get; private set; }

    public async Task InitializeAsync()
    {
        var url = Environment.GetEnvironmentVariable("TEST_DATABASE_URL");
        if (string.IsNullOrWhiteSpace(url))
        {
            return;
        }

        _adminConnectionString = DatabaseUrl.ToConnectionString(url);
        _databaseName = "negotiation_test_" + Guid.NewGuid().ToString("N");
        await using (var admin = new NpgsqlConnection(_adminConnectionString))
        {
            await admin.OpenAsync();
            await using var create = new NpgsqlCommand($"CREATE DATABASE \"{_databaseName}\"", admin);
            await create.ExecuteNonQueryAsync();
        }

        var builder = new NpgsqlConnectionStringBuilder(_adminConnectionString) { Database = _databaseName, MaxPoolSize = 50 };
        DataSource = NpgsqlDataSource.Create(builder.ConnectionString);
        await Migrator.MigrateAsync(DataSource, typeof(Program).Assembly);
    }

    public async Task DisposeAsync()
    {
        if (DataSource is null)
        {
            return;
        }

        await DataSource.DisposeAsync();
        NpgsqlConnection.ClearAllPools();
        await using var admin = new NpgsqlConnection(_adminConnectionString);
        await admin.OpenAsync();
        await using var drop = new NpgsqlCommand($"DROP DATABASE IF EXISTS \"{_databaseName}\" WITH (FORCE)", admin);
        await drop.ExecuteNonQueryAsync();
    }
}
