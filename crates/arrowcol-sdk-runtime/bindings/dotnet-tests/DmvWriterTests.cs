using System.Collections.Concurrent;
using System.Data;
using Arrowcol.Client;
using Arrowcol.Dmv;
using Arrowcol.Telemetry.SqlserverDmvV2;
using Xunit;
using static Arrowcol.Sdk.Tests.TestCatalog;

namespace Arrowcol.Sdk.Tests;

public sealed class DmvWriterTests
{
    private static readonly DateTime Start = new(2026, 1, 2, 3, 4, 5, DateTimeKind.Utc);

    private static DataTableReader CpuRows(int count, Func<int, string>? machine = null)
    {
        var table = new DataTable();
        table.Columns.Add("sample_time_utc", typeof(DateTime));
        table.Columns.Add("machine_name", typeof(string));
        table.Columns.Add("avg_cpu_percent", typeof(decimal));
        for (var index = 0; index < count; index++)
            table.Rows.Add(Start.AddSeconds(index), machine?.Invoke(index) ?? $"m{index}", 1.5m);
        return table.CreateDataReader();
    }

    private static DataTableReader LatchRows(int count)
    {
        var table = new DataTable();
        table.Columns.Add("latch_class", typeof(string));
        table.Columns.Add("wait_time_ms", typeof(long));
        for (var index = 0; index < count; index++) table.Rows.Add($"latch{index}", (long)index);
        return table.CreateDataReader();
    }

    private static string Machine(byte[] row) => SqlserverCpuUtilization.Parser.ParseFrom(row).MachineName;

    [Fact]
    public async Task RetryReconnectsOnAFreshStream()
    {
        var pushes = 0;
        var sink = new RecordingSink((_, _) => Interlocked.Increment(ref pushes) == 1);

        var result = await DmvWriter.WriteAsync(CpuRows(3), Cpu, Options(sink), TestContext.Current.CancellationToken);

        Assert.Equal(new DmvWriteResult(3, 3, 0), result);
        var streams = sink.Streams;
        Assert.Equal(2, streams.Count);
        Assert.True(streams[0].Disposed);
        Assert.False(streams[0].Closed);
        Assert.Empty(streams[0].Flushed);
        Assert.True(streams[1].Closed && streams[1].Disposed);
        Assert.Equal(new[] { "m0", "m1", "m2" }, streams[1].Flushed.Single().Select(Machine));
    }

    [Fact]
    public async Task ContinuousWriterDropsFailedBatchWithoutCancellingOtherWriters()
    {
        var sink = new RecordingSink((dataset, rows) => dataset == Cpu && rows.Any(row => Machine(row) == "poison"));
        var log = new ConcurrentQueue<string>();
        var options = Options(sink) with { MaxBatchRows = 100, Log = log.Enqueue };
        var latch = Packaged.Get("dmv.dm_os_latch_stats");

        var results = await Task.WhenAll(
            DmvWriter.WriteAsync(CpuRows(300, index => index == 150 ? "poison" : $"m{index}"), Cpu, options, TestContext.Current.CancellationToken),
            DmvWriter.WriteAsync(LatchRows(300), latch, options, TestContext.Current.CancellationToken));

        Assert.Equal(new DmvWriteResult(300, 200, 1), results[0]);
        Assert.Equal(new DmvWriteResult(300, 300, 0), results[1]);
        Assert.Equal(
            Enumerable.Range(0, 300).Where(index => index / 100 != 1).Select(index => $"m{index}"),
            sink.For(Cpu).SelectMany(stream => stream.Flushed).SelectMany(batch => batch).Select(Machine));
        Assert.Equal(4, sink.For(Cpu).Count());
        Assert.Single(sink.For(latch));
        Assert.All(sink.Streams, stream => Assert.True(stream.Disposed));
        Assert.Single(log, line => line.Contains("dropped a batch of 100 rows", StringComparison.Ordinal));
    }

    [Fact]
    public async Task NonContinuousWriterThrowsAfterItsAttempts()
    {
        var sink = new RecordingSink((_, _) => true);

        var error = await Assert.ThrowsAsync<ArrowcolException>(() =>
            DmvWriter.WriteAsync(CpuRows(5), Cpu, Options(sink) with { Continuous = false }, TestContext.Current.CancellationToken));

        Assert.Contains("after 3 attempts", error.Message, StringComparison.Ordinal);
        Assert.Equal(3, sink.Streams.Count);
        Assert.All(sink.Streams, stream => Assert.True(stream.Disposed));
    }

    [Fact]
    public async Task BatchesStayWithinRowAndByteLimits()
    {
        var rows = new RecordingSink();
        await DmvWriter.WriteAsync(CpuRows(2500), Cpu, Options(rows), TestContext.Current.CancellationToken);
        Assert.Equal(new[] { 1024, 1024, 452 }, rows.Streams.Single().Flushed.Select(batch => batch.Length));

        var bytes = new RecordingSink();
        var result = await DmvWriter.WriteAsync(
            CpuRows(200, index => index == 7 ? new string('x', 4096) : new string('m', 100)), Cpu,
            Options(bytes) with { MaxBatchBytes = 1024 }, TestContext.Current.CancellationToken);

        Assert.Equal(new DmvWriteResult(200, 199, 1), result);
        var batches = bytes.Streams.Single().Flushed;
        Assert.True(batches.Count > 1);
        Assert.All(batches, batch => Assert.InRange(batch.Sum(row => row.Length + 5), 1, 1024));
    }

    [Fact]
    public async Task ReaderColumnsMapByNameAndEncodeContractTypes()
    {
        var table = new DataTable();
        table.Columns.Add("MachineName", typeof(string));
        table.Columns.Add("AVG_CPU_PERCENT", typeof(decimal));
        table.Columns.Add("sql_process_cpu_percent", typeof(long));
        table.Columns.Add("process_sample_time_utc", typeof(DateTimeOffset));
        table.Columns.Add("sql_server_instance_name", typeof(string));
        table.Columns.Add("unmapped", typeof(string));
        table.Rows.Add("host", 12.3456789012345m, 42L, new DateTimeOffset(Start, TimeSpan.Zero).ToOffset(TimeSpan.FromHours(2)), "reader", "x");
        var sink = new RecordingSink();
        var before = DateTime.UtcNow;

        await DmvWriter.WriteAsync(table.CreateDataReader(), Cpu, Options(sink) with
        {
            Constants = new Dictionary<string, object?> { ["sql_server_instance_name"] = "constant" },
        }, TestContext.Current.CancellationToken);

        var row = SqlserverCpuUtilization.Parser.ParseFrom(sink.Rows.Single());
        Assert.Equal("customer-1", row.CustomerId);
        Assert.Equal("host", row.MachineName);
        Assert.Equal("12.345678901234", row.AvgCpuPercent);
        Assert.Equal(42, row.SqlProcessCpuPercent);
        Assert.Equal(Micros(Start), row.ProcessSampleTimeUtc);
        Assert.Equal("constant", row.SqlServerInstanceName);
        Assert.InRange(row.SampleTimeUtc, Micros(before), Micros(DateTime.UtcNow));
        Assert.False(row.HasIdleCpuPercent);
    }
}
