using Npgsql;

namespace Taakht.Platform.Tests;

public class DatabaseUrlTests
{
    [Fact]
    public void Parses_postgres_url()
    {
        var cs = new NpgsqlConnectionStringBuilder(
            DatabaseUrl.ToConnectionString("postgres://taakht:p%40ss@db.local:5433/negotiation?sslmode=disable"));

        Assert.Equal("db.local", cs.Host);
        Assert.Equal(5433, cs.Port);
        Assert.Equal("negotiation", cs.Database);
        Assert.Equal("taakht", cs.Username);
        Assert.Equal("p@ss", cs.Password);
        Assert.Equal(SslMode.Disable, cs.SslMode);
    }

    [Fact]
    public void Defaults_port_and_passes_through_plain_connection_strings()
    {
        var cs = new NpgsqlConnectionStringBuilder(DatabaseUrl.ToConnectionString("postgres://u:p@h/d"));
        Assert.Equal(5432, cs.Port);

        const string plain = "Host=h;Database=d";
        Assert.Equal(plain, DatabaseUrl.ToConnectionString(plain));
    }

    [Theory]
    [InlineData("postgres://u:p@localhost:5432/d", "127.0.0.1")]
    [InlineData("postgres://u:p@LOCALHOST/d", "127.0.0.1")]
    [InlineData("Host=localhost;Database=d", "127.0.0.1")]
    [InlineData("postgres://u:p@127.0.0.1/d", "127.0.0.1")]
    [InlineData("postgres://u:p@db.local/d", "db.local")]
    [InlineData("postgres://u:p@localhost.example.com/d", "localhost.example.com")]
    public void Rewrites_a_localhost_host_to_the_loopback_address(string url, string expectedHost) =>
        Assert.Equal(expectedHost, new NpgsqlConnectionStringBuilder(DatabaseUrl.ToConnectionString(url)).Host);

    [Fact]
    public void Rejects_unknown_sslmode()
    {
        Assert.Throws<FormatException>(() => DatabaseUrl.ToConnectionString("postgres://u:p@h/d?sslmode=nope"));
    }
}
