using Arrowcol.Dmv;
using Google.Protobuf;
using Microsoft.Data.SqlClient;
using Xunit;
using static Arrowcol.Sdk.Tests.TestCatalog;
using ProtoType = Google.Protobuf.Reflection.FieldDescriptorProto.Types.Type;

namespace Arrowcol.Sdk.Tests;

/// <summary>Runs the packaged catalog against the SQL Server named by <c>ARROWCOL_TEST_SQLSERVER</c>.</summary>
public sealed class DmvSqlServerTests
{
    private static readonly string? ConnectionString = Environment.GetEnvironmentVariable("ARROWCOL_TEST_SQLSERVER");

    private static async Task<SqlConnection> OpenAsync()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set ARROWCOL_TEST_SQLSERVER to a SQL Server connection string");
        var connection = new SqlConnection(ConnectionString);
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        return connection;
    }

    private static IEnumerable<DmvDataset> SqlServer => Packaged.ForTarget("sqlserver");

    [Fact]
    public async Task PublicViewsExposeEveryCatalogColumn()
    {
        await using var connection = await OpenAsync();
        var writer = new DmvWriteOptions("http://127.0.0.1:9", "customer-1");
        var failures = new List<string>();
        foreach (var dataset in SqlServer.Where(dataset => dataset.Query is null && dataset.Source is not null &&
            dataset.Provenance is ["sql-server-2025"]))
        {
            var view = await DmvSql.ProbeAsync(connection, dataset.Source!, TestContext.Current.CancellationToken);
            var missing = dataset.Columns.Where(column => !DmvSql.IsWriterColumn(column.Name, writer) &&
                !view.TryGetValue(column.Source ?? column.Name, out _)).Select(column => column.Name).ToList();
            if (view.Count == 0) failures.Add($"{dataset.Source} is not exposed");
            else if (missing.Count > 0) failures.Add($"{dataset.Source} lacks {string.Join(", ", missing)}");
        }
        Assert.Empty(failures);
    }

    [Fact]
    public async Task EveryCollectableDatasetWritesWhatItReads()
    {
        await using var connection = await OpenAsync();
        var failures = new List<string>();
        var populated = 0;
        var collectable = SqlServer.Where(dataset => dataset.Collectable).ToList();
        foreach (var dataset in collectable)
        {
            var sink = new RecordingSink();
            var log = new List<string>();
            var result = await DmvWriter.CollectAsync(connection, dataset, Options(sink) with { Log = log.Add, Watermarks = new() },
                TestContext.Current.CancellationToken);
            if (result.BatchesDropped > 0 || result.RowsRead != result.RowsWritten || sink.Rows.Count() != result.RowsWritten)
                failures.Add($"{dataset.Id}: {result}");
            if (!dataset.Optional && log.Any(line => line.EndsWith("skipped", StringComparison.Ordinal)))
                failures.Add($"{dataset.Id}: {string.Join("; ", log)}");
            if (result.RowsRead > 0) populated++;
        }
        Assert.Empty(failures);
        Assert.True(populated * 2 > collectable.Count, $"only {populated} of {collectable.Count} datasets returned rows");
    }

    [Fact]
    public async Task WatermarkedViewsReadOnlyNewerRows()
    {
        await using var connection = await OpenAsync();
        var cancellation = TestContext.Current.CancellationToken;
        var verified = 0;
        foreach (var dataset in SqlServer.Where(dataset => dataset.Watermark is not null))
        {
            var field = DmvContract.Load(dataset.ContractPath!).Fields.Single(field => field.Name == dataset.Watermark);
            var watermarks = new DmvWatermarks();
            var first = new RecordingSink();
            var initial = await DmvWriter.CollectAsync(connection, dataset, Options(first) with { Watermarks = watermarks }, cancellation);
            if (initial.RowsRead == 0) continue;

            Assert.InRange(initial.RowsRead, 1, dataset.Top ?? long.MaxValue);
            Assert.True(watermarks.TryGet(dataset.Id, out var stored));
            var mark = stored is DateTime instant ? Micros(instant) : (long)stored!;
            Assert.Equal(first.Rows.Max(row => Read(row, field)), mark);

            var next = new RecordingSink();
            await DmvWriter.CollectAsync(connection, dataset, Options(next) with { Watermarks = watermarks }, cancellation);
            Assert.All(next.Rows, row => Assert.True(Read(row, field) > mark, $"{dataset.Id} re-read a row at or before its watermark"));

            object future = field.Kind switch
            {
                "timestamp" => new DateTime(9999, 12, 31, 0, 0, 0, DateTimeKind.Utc),
                "int32" => (long)int.MaxValue,
                _ => long.MaxValue,
            };
            var none = await DmvWriter.CollectAsync(connection, dataset,
                Options(new RecordingSink()) with { Watermarks = new(new Dictionary<string, object> { [dataset.Id] = future }) }, cancellation);
            Assert.Equal(0, none.RowsRead);
            verified++;
        }
        Assert.True(verified > 0, "no watermarked view returned rows");
    }

    [Fact]
    public async Task DescribedViewsAreBoundedProjectionsThatCollect()
    {
        await using var connection = await OpenAsync();
        var cancellation = TestContext.Current.CancellationToken;

        var described = await DmvCatalog.DescribeAsync(connection, "described", "sqlserver",
        [
            new DmvViewSpec("dm_exec_requests"),
            new DmvViewSpec("dm_exec_sessions") { Columns = ["login_name", "session_id", "no_such_column"] },
            new DmvViewSpec("dm_no_such_view"),
        ], cancellation);

        Assert.Equal(new[] { "described.dm_exec_requests", "described.dm_exec_sessions" }, described.Select(dataset => dataset.Id));
        Assert.All(described, dataset =>
        {
            Assert.Equal($"telemetry.described.{dataset.Dataset}", dataset.Table);
            Assert.Equal(new[] { "runtime" }, dataset.Provenance);
            Assert.Equal(new[] { "sqlserver" }, dataset.Targets);
            Assert.Equal(1000, dataset.Top);
            Assert.Equal(DmvCatalog.KeyColumns(), dataset.Columns.Take(DmvCatalog.KeyColumns().Count));
        });
        var requests = described[0].Columns.Select(column => column.Name).ToList();
        Assert.Contains("session_id", requests);
        Assert.DoesNotContain("sql_handle", requests);
        Assert.DoesNotContain("plan_handle", requests);
        Assert.DoesNotContain("context_info", requests);
        Assert.Equal(new[] { "login_name", "session_id" }, described[1].Columns.Skip(DmvCatalog.KeyColumns().Count).Select(column => column.Name));

        var temp = Directory.CreateTempSubdirectory("arrowcol-described-");
        try
        {
            var catalog = Packaged.With(described).CompileMissingContracts(temp.FullName);
            foreach (var dataset in described)
            {
                var sink = new RecordingSink();
                var result = await DmvWriter.CollectAsync(connection, catalog.Get(dataset.Id), Options(sink), cancellation);
                Assert.True(result.RowsRead > 0, $"{dataset.Id} returned no rows");
                Assert.Equal(result.RowsRead, result.RowsWritten);
            }
        }
        finally
        {
            temp.Delete(recursive: true);
        }
    }

    private static long Read(byte[] row, DmvContract.Field field)
    {
        var input = new CodedInputStream(row);
        for (uint tag; (tag = input.ReadTag()) != 0;)
        {
            if (WireFormat.GetTagFieldNumber(tag) != field.Number)
            {
                input.SkipLastField();
                continue;
            }
            return field.Wire switch
            {
                ProtoType.Sint32 => input.ReadSInt32(),
                ProtoType.Sint64 => input.ReadSInt64(),
                ProtoType.Int32 => input.ReadInt32(),
                _ => input.ReadInt64(),
            };
        }
        throw new InvalidDataException($"row has no {field.Name}");
    }
}
