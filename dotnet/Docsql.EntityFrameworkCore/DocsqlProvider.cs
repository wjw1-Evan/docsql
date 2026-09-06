// EF Core support for docsql.
//
// Strategy: reuse EF Core's SQLite provider for the full SQL-generation and
// update pipeline (our engine's SQL surface tracks the SQLite-style dialect),
// swapping the ADO.NET connection for Docsql.Client. This gives LINQ,
// SaveChanges and EnsureCreated against a docsql server without maintaining
// a bespoke EF service stack.

using Docsql.Client;
using Microsoft.EntityFrameworkCore;

namespace Docsql.EntityFrameworkCore;

public static class DocsqlDbContextOptionsExtensions
{
    /// <summary>Use docsql as the backend, generating SQLite-compatible SQL.</summary>
    public static DbContextOptionsBuilder UseDocsql(
        this DbContextOptionsBuilder options,
        string connectionString)
    {
        var b = new DocsqlConnectionStringBuilder { ConnectionString = connectionString };
        var conn = new DocsqlConnection("Data Source=docsql")
        {
            // EF's SQLite layer inspects ConnectionString; the real endpoint
            // is carried separately.
            EndpointOverride = (b.Host, b.Port, b.Token),
        };
        return options.UseSqlite(conn);
    }

    /// <summary>Use docsql with an already-open connection.</summary>
    public static DbContextOptionsBuilder UseDocsql(
        this DbContextOptionsBuilder options,
        DocsqlConnection connection)
        => options.UseSqlite(connection);
}
