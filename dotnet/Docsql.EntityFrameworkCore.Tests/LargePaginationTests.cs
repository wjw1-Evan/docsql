using System.Text;
using Docsql.Client;
using Docsql.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;

public class PageRow
{
    public int Id { get; set; }
    public int Rank { get; set; }
    public string Name { get; set; } = "";
    public int Bucket { get; set; }
    public string Payload { get; set; } = "";
}

public class PageRowDb : DbContext
{
    private readonly string _cs;
    public PageRowDb(string cs) => _cs = cs;
    public DbSet<PageRow> Rows => Set<PageRow>();
    protected override void OnModelCreating(ModelBuilder b) =>
        b.Entity<PageRow>().ToTable("PageRows");
    protected override void OnConfiguring(DbContextOptionsBuilder o) => o.UseDocsql(_cs);
}

public sealed class LargePagerFixture : IDisposable
{
    public const int Total = 50_000;
    private readonly EfServerFixture _server;
    public string Cs => $"host=127.0.0.1;port={_server.Port}";

    public LargePagerFixture()
    {
        _server = new EfServerFixture();
        using var conn = new DocsqlConnection(Cs);
        conn.Open();
        using var tx = conn.BeginTransaction();
        using (var cmd = conn.CreateCommand())
        {
            cmd.Transaction = tx;
            cmd.CommandText =
                "CREATE TABLE PageRows (Id INT PRIMARY KEY, Rank INT, Name TEXT, Bucket INT, Payload TEXT)";
            cmd.ExecuteNonQuery();
        }
        const int batch = 500;
        for (var start = 1; start <= Total; start += batch)
        {
            var end = Math.Min(start + batch - 1, Total);
            var values = new StringBuilder();
            for (var i = start; i <= end; i++)
            {
                if (values.Length > 0) values.Append(',');
                values.Append('(').Append(i).Append(',').Append(i)
                    .Append(",'row-").Append(i.ToString("D6")).Append("',")
                    .Append(i % 7).Append(",'")
                    .Append('x', i % 50 == 0 ? 9_000 : 120)
                    .Append("')");
            }
            using var cmd = conn.CreateCommand();
            cmd.Transaction = tx;
            cmd.CommandText = "INSERT INTO PageRows (Id, Rank, Name, Bucket, Payload) VALUES " + values;
            cmd.ExecuteNonQuery();
        }
        tx.Commit();
    }

    public void Dispose() => _server.Dispose();
}

public sealed class LargePaginationTests : IClassFixture<LargePagerFixture>
{
    private readonly LargePagerFixture _fx;
    public LargePaginationTests(LargePagerFixture fx) => _fx = fx;
    private PageRowDb NewDb() => new(_fx.Cs);

    [Fact]
    public void Ordered_traversal_pages_cover_all_50k_rows_exactly_once()
    {
        const int pageSize = 2_500;
        using var db = NewDb();
        Assert.Equal(LargePagerFixture.Total, db.Rows.Count());

        var baseline = db.Rows.OrderBy(r => r.Rank).Select(r => r.Rank).ToList();
        Assert.Equal(Enumerable.Range(1, LargePagerFixture.Total), baseline);

        var seen = new List<int>(LargePagerFixture.Total);
        for (var page = 0; page * pageSize < LargePagerFixture.Total; page++)
        {
            var got = db.Rows.OrderBy(r => r.Rank)
                .Skip(page * pageSize).Take(pageSize)
                .Select(r => r.Rank).ToList();
            Assert.Equal(pageSize, got.Count);
            Assert.Equal(baseline.GetRange(page * pageSize, pageSize), got);
            seen.AddRange(got);
        }
        Assert.Equal(baseline, seen);
        Assert.Equal(LargePagerFixture.Total, seen.Distinct().Count());
    }

    [Fact]
    public void Deep_offset_skip_only_tail_and_past_end_are_exact()
    {
        using var db = NewDb();
        Assert.Equal(
            Enumerable.Range(47_501, 1_000),
            db.Rows.OrderBy(r => r.Rank).Skip(47_500).Take(1_000).Select(r => r.Rank).ToList());

        Assert.Equal(
            Enumerable.Range(LargePagerFixture.Total - 9, 10),
            db.Rows.OrderBy(r => r.Rank)
                .Skip(LargePagerFixture.Total - 10).Select(r => r.Rank).ToList());

        Assert.Empty(db.Rows.OrderBy(r => r.Rank).Skip(LargePagerFixture.Total).ToList());
        Assert.Empty(db.Rows.OrderBy(r => r.Rank)
            .Skip(LargePagerFixture.Total + 10_000).Take(100).ToList());

        Assert.Equal(
            Enumerable.Range(36_656, 1_000).Reverse(),
            db.Rows.OrderByDescending(r => r.Rank)
                .Skip(12_345).Take(1_000).Select(r => r.Rank).ToList());
    }

    [Fact]
    public void Filtered_and_composite_ordered_pagination_match_baseline()
    {
        using var db = NewDb();

        var filtered = db.Rows.Where(r => r.Bucket == 3).OrderBy(r => r.Rank)
            .Select(r => r.Rank).ToList();
        Assert.Equal(Enumerable.Range(1, LargePagerFixture.Total).Where(i => i % 7 == 3), filtered);
        Assert.Equal(filtered.Count, db.Rows.Count(r => r.Bucket == 3));

        const int pageSize = 1_000;
        for (var page = 0; page * pageSize < filtered.Count; page++)
        {
            var got = db.Rows.Where(r => r.Bucket == 3).OrderBy(r => r.Rank)
                .Skip(page * pageSize).Take(pageSize).Select(r => r.Rank).ToList();
            Assert.Equal(filtered.Skip(page * pageSize).Take(pageSize), got);
        }

        var composite = db.Rows.OrderBy(r => r.Bucket).ThenBy(r => r.Rank)
            .Select(r => r.Rank).ToList();
        Assert.Equal(LargePagerFixture.Total, composite.Count);
        const int compositePage = 5_000;
        for (var page = 0; page * compositePage < LargePagerFixture.Total; page++)
        {
            var got = db.Rows.OrderBy(r => r.Bucket).ThenBy(r => r.Rank)
                .Skip(page * compositePage).Take(compositePage).Select(r => r.Rank).ToList();
            Assert.Equal(composite.GetRange(page * compositePage, compositePage), got);
        }
    }

    [Fact]
    public void Overflow_chain_payloads_survive_paged_reads()
    {
        using var db = NewDb();
        foreach (var rank in new[] { 5_000, 25_000, 49_950, 50_000 })
        {
            var row = db.Rows.Single(r => r.Rank == rank);
            Assert.Equal($"row-{rank:D6}", row.Name);
            Assert.Equal(9_000, row.Payload.Length);
            Assert.Equal(new string('x', 9_000), row.Payload);
        }

        var window = db.Rows.OrderBy(r => r.Rank)
            .Skip(LargePagerFixture.Total - 100).Take(100).ToList();
        Assert.Equal(LargePagerFixture.Total - 99, window[0].Rank);
        Assert.Equal(LargePagerFixture.Total, window[^1].Rank);
        Assert.All(window, r => Assert.Equal(
            r.Rank % 50 == 0 ? 9_000 : 120, r.Payload.Length));
    }
}
