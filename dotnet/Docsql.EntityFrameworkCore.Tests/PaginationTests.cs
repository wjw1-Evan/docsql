using Docsql.Client;
using Docsql.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;

public class PagedItem
{
    public int Id { get; set; }
    public int Rank { get; set; }
    public string Name { get; set; } = "";
    public int Bucket { get; set; }
}

public class PagedItemDb : DbContext
{
    private readonly string _cs;
    public PagedItemDb(string cs) => _cs = cs;
    public DbSet<PagedItem> Items => Set<PagedItem>();
    protected override void OnModelCreating(ModelBuilder b) =>
        b.Entity<PagedItem>().ToTable("PagedItems");
    protected override void OnConfiguring(DbContextOptionsBuilder o) => o.UseDocsql(_cs);
}

public sealed class PaginationTests : IClassFixture<EfServerFixture>
{
    private readonly EfServerFixture _fx;
    public PaginationTests(EfServerFixture fx) => _fx = fx;
    private string Cs => $"host=127.0.0.1;port={_fx.Port}";

    private void Clean()
    {
        using var conn = new DocsqlConnection(Cs);
        conn.Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "DROP TABLE IF EXISTS PagedItems";
        cmd.ExecuteNonQuery();
    }

    private void Seed(int total)
    {
        using var db = new PagedItemDb(Cs);
        for (var i = 1; i <= total; i++)
            db.Items.Add(new PagedItem { Rank = i, Name = $"row-{i:D2}", Bucket = i % 5 });
        db.SaveChanges();
    }

    [Fact]
    public void Skip_and_Take_page_through_ordered_rows_without_gaps()
    {
        Clean();
        Seed(23);

        using var db = new PagedItemDb(Cs);
        Assert.Equal(
            Enumerable.Range(1, 23),
            db.Items.OrderBy(p => p.Rank).Select(p => p.Rank).ToList());

        const int pageSize = 10;
        var pages = new List<List<int>>();
        for (var skip = 0; skip <= 30; skip += pageSize)
            pages.Add(db.Items
                .OrderBy(p => p.Rank)
                .Skip(skip)
                .Take(pageSize)
                .Select(p => p.Rank)
                .ToList());

        Assert.Equal(new[] { 10, 10, 3, 0 }, pages.Select(p => p.Count).ToArray());
        Assert.Equal(Enumerable.Range(1, 10), pages[0]);
        Assert.Equal(Enumerable.Range(11, 10), pages[1]);
        Assert.Equal(new[] { 21, 22, 23 }, pages[2]);
        Assert.Empty(pages[3]);
        Assert.Equal(23, pages.SelectMany(p => p).Distinct().Count());
        Assert.Equal(23, db.Items.Count());

        Assert.Equal(
            new[] { 20, 19, 18, 17 },
            db.Items.OrderByDescending(p => p.Rank).Skip(3).Take(4).Select(p => p.Rank).ToList());
    }

    [Fact]
    public void Skip_without_Take_returns_the_tail_and_Take_alone_returns_the_head()
    {
        Clean();
        Seed(23);

        using var db = new PagedItemDb(Cs);
        Assert.Equal(
            new[] { 1, 2, 3 },
            db.Items.OrderBy(p => p.Rank).Take(3).Select(p => p.Rank).ToList());
        Assert.Equal(
            Enumerable.Range(21, 3),
            db.Items.OrderBy(p => p.Rank).Skip(20).Select(p => p.Rank).ToList());
        Assert.Empty(db.Items.OrderBy(p => p.Rank).Skip(23).ToList());
        Assert.Empty(db.Items.OrderBy(p => p.Rank).Skip(100).Take(5).ToList());
    }

    [Fact]
    public void Filter_and_pagination_combine()
    {
        Clean();
        Seed(23);

        using var db = new PagedItemDb(Cs);
        Assert.Equal(
            new[] { 1, 6, 11, 16, 21 },
            db.Items.Where(p => p.Bucket == 1).OrderBy(p => p.Rank).Select(p => p.Rank).ToList());
        Assert.Equal(5, db.Items.Count(p => p.Bucket == 1));

        Assert.Equal(
            new[] { 11, 16 },
            db.Items.Where(p => p.Bucket == 1).OrderBy(p => p.Rank)
                .Skip(2).Take(2).Select(p => p.Rank).ToList());
        Assert.Equal(
            new[] { 21 },
            db.Items.Where(p => p.Bucket == 1).OrderBy(p => p.Rank)
                .Skip(4).Take(2).Select(p => p.Rank).ToList());
    }
}
