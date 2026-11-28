using System.Collections.Concurrent;
using Arrowcol.Dmv;

namespace Arrowcol.Sdk.Tests;

/// <summary>Records flushed batches per stream; <c>fail</c> rejects a push before it is buffered.</summary>
internal sealed class RecordingSink(Func<DmvDataset, IReadOnlyList<byte[]>, bool>? fail = null) : IDmvSink
{
    private readonly ConcurrentQueue<RecordingStream> streams = new();

    public IReadOnlyList<RecordingStream> Streams => [.. streams];

    public IEnumerable<RecordingStream> For(DmvDataset dataset) => streams.Where(stream => stream.Dataset == dataset);

    public IEnumerable<byte[]> Rows => streams.SelectMany(stream => stream.Flushed).SelectMany(batch => batch);

    public Task<IDmvBatchStream> OpenAsync(DmvDataset dataset, DmvContract contract, DmvWriteOptions options, CancellationToken cancellationToken)
    {
        var stream = new RecordingStream(dataset, fail);
        streams.Enqueue(stream);
        return Task.FromResult<IDmvBatchStream>(stream);
    }
}

internal sealed class RecordingStream(DmvDataset dataset, Func<DmvDataset, IReadOnlyList<byte[]>, bool>? fail) : IDmvBatchStream
{
    private byte[][]? pending;

    public DmvDataset Dataset => dataset;
    public List<byte[][]> Flushed { get; } = [];
    public bool Closed { get; private set; }
    public bool Disposed { get; private set; }

    public Task PushBatchAsync(IReadOnlyList<byte[]> rows)
    {
        ObjectDisposedException.ThrowIf(Disposed, this);
        if (fail?.Invoke(dataset, rows) == true) throw new IOException("injected push failure");
        pending = [.. rows];
        return Task.CompletedTask;
    }

    public Task FlushAsync()
    {
        Flushed.Add(pending ?? throw new InvalidOperationException("flush without a pushed batch"));
        pending = null;
        return Task.CompletedTask;
    }

    public Task CloseAsync()
    {
        Closed = true;
        return Task.CompletedTask;
    }

    public ValueTask DisposeAsync()
    {
        Disposed = true;
        return ValueTask.CompletedTask;
    }
}

internal static class TestCatalog
{
    public static DmvCatalog Packaged { get; } = DmvCatalog.LoadPackaged();

    public static DmvDataset Cpu => Packaged.Get("sqlserver_dmv_v2.sqlserver_cpu_utilization");

    public static DmvWriteOptions Options(RecordingSink sink) =>
        new("http://127.0.0.1:9", "customer-1") { Sink = sink, RetryDelay = TimeSpan.Zero };

    public static long Micros(DateTime utc) => (utc.Ticks - DateTime.UnixEpoch.Ticks) / 10;
}
