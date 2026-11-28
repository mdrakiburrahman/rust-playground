using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Arrowcol.Client;

namespace Arrowcol.Dmv;

/// <summary>How rows reach the collector. Constants fill writer-owned columns such as the source identity.</summary>
public sealed record DmvWriteOptions(string Endpoint, string CustomerId)
{
    /// <summary>Drop a batch after its attempts fail and keep going; when false the failure is thrown.</summary>
    public bool Continuous { get; init; } = true;
    public int MaxBatchRows { get; init; } = 1024;
    public int MaxBatchBytes { get; init; } = 8 * 1024 * 1024;
    /// <summary>Total tries per batch. Each retry reconnects on a fresh stream.</summary>
    public int Attempts { get; init; } = 3;
    /// <summary>Delay before retry n is n times this value.</summary>
    public TimeSpan RetryDelay { get; init; } = TimeSpan.FromSeconds(2);
    public TimeSpan? CommandTimeout { get; init; }
    /// <summary>Column values that override the reader, for example <c>source_server_name</c>.</summary>
    public IReadOnlyDictionary<string, object?> Constants { get; init; } = new Dictionary<string, object?>();
    /// <summary>Per-dataset high-water marks for datasets with a watermark column.</summary>
    public DmvWatermarks? Watermarks { get; init; }
    public Action<string>? Log { get; init; }
    internal IDmvSink? Sink { get; init; }

    internal void Validate()
    {
        ArgumentException.ThrowIfNullOrEmpty(Endpoint);
        ArgumentException.ThrowIfNullOrEmpty(CustomerId);
        if (MaxBatchRows is < 1 or > 1_000_000) throw new ArgumentOutOfRangeException(nameof(MaxBatchRows));
        if (MaxBatchBytes is < 1024 or > 12 * 1024 * 1024) throw new ArgumentOutOfRangeException(nameof(MaxBatchBytes));
        if (Attempts < 1) throw new ArgumentOutOfRangeException(nameof(Attempts));
        if (RetryDelay < TimeSpan.Zero) throw new ArgumentOutOfRangeException(nameof(RetryDelay));
    }
}

/// <summary>Rows read from the source, rows the collector admitted, and batches dropped after retries.</summary>
public readonly record struct DmvWriteResult(long RowsRead, long RowsWritten, int BatchesDropped);

/// <summary>Thread-safe high-water marks keyed by dataset id. Values are UTC <see cref="DateTime"/> or <see cref="long"/>.</summary>
public sealed class DmvWatermarks(IEnumerable<KeyValuePair<string, object>>? initial = null)
{
    private readonly ConcurrentDictionary<string, object> values = new(initial ?? [], StringComparer.Ordinal);
    public bool TryGet(string id, out object? value)
    {
        var found = values.TryGetValue(id, out var current);
        value = current;
        return found;
    }
    public void Set(string id, object value) => values[id] = value is DateTime or long
        ? value : throw new ArgumentException("Watermarks are UTC DateTime or long values", nameof(value));
    public IReadOnlyDictionary<string, object> Snapshot() => new Dictionary<string, object>(values, StringComparer.Ordinal);
}

internal interface IDmvSink
{
    Task<IDmvBatchStream> OpenAsync(DmvDataset dataset, DmvContract contract, DmvWriteOptions options, CancellationToken cancellationToken);
}

internal interface IDmvBatchStream : IAsyncDisposable
{
    Task PushBatchAsync(IReadOnlyList<byte[]> rows);
    Task FlushAsync();
    Task CloseAsync();
}

internal sealed class ArrowcolSink : IDmvSink
{
    public static readonly ArrowcolSink Instance = new();

    public async Task<IDmvBatchStream> OpenAsync(DmvDataset dataset, DmvContract contract, DmvWriteOptions options, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var bound = ArrowcolContract.Load(contract.Path).BindRaw(contract.DescriptorSet, contract.ModelName);
        var stream = await ArrowcolStream<byte[]>.ConnectAsync(options.Endpoint, bound, new StreamOptions
        {
            BatchRows = options.MaxBatchRows,
            BatchBytes = 16 * 1024 * 1024,
            BufferBytes = 64 * 1024 * 1024,
        }).ConfigureAwait(false);
        return new Stream(stream);
    }

    private sealed class Stream(ArrowcolStream<byte[]> stream) : IDmvBatchStream
    {
        public Task PushBatchAsync(IReadOnlyList<byte[]> rows) => stream.PushBatchAsync(rows);
        public Task FlushAsync() => stream.FlushAsync();
        public Task CloseAsync() => stream.CloseAsync();
        public ValueTask DisposeAsync() => stream.DisposeAsync();
    }
}

/// <summary>Streams catalog datasets to a local collector without per-view code.</summary>
public static class DmvWriter
{
    /// <summary>
    /// Runs the dataset's bounded projection (or verbatim aggregate) and writes the rows. Views this engine does not
    /// expose write nothing; errors from optional datasets are logged and skipped.
    /// </summary>
    public static async Task<DmvWriteResult> CollectAsync(
        DbConnection connection, DmvDataset dataset, DmvWriteOptions options, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(connection);
        ArgumentNullException.ThrowIfNull(dataset);
        ArgumentNullException.ThrowIfNull(options);
        if (!dataset.Collectable) throw new InvalidOperationException($"{dataset.Id} is a schema-only table; write it with WriteAsync");
        options.Validate();
        try
        {
            await DmvSql.OpenAsync(connection, cancellationToken).ConfigureAwait(false);
            await using var command = connection.CreateCommand();
            if (dataset.Query is not null) command.CommandText = dataset.Query;
            else
            {
                var view = await DmvSql.ProbeAsync(connection, dataset.Source!, cancellationToken).ConfigureAwait(false);
                if (view.Count == 0)
                {
                    options.Log?.Invoke($"{dataset.Id}: {dataset.Source} is not exposed by this engine; skipped");
                    return default;
                }
                object? watermark = null;
                var hasWatermark = dataset.Watermark is not null && options.Watermarks?.TryGet(dataset.Id, out watermark) == true;
                command.CommandText = DmvSql.Select(dataset, view, options, hasWatermark);
                if (hasWatermark)
                    DmvSql.Add(command, "@watermark",
                        DmvSql.ParameterType(dataset.Columns.First(column => column.Name == dataset.Watermark).Type), watermark!);
            }
            if (options.CommandTimeout is { } timeout) command.CommandTimeout = (int)Math.Ceiling(timeout.TotalSeconds);
            await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            return await WriteAsync(reader, dataset, options, cancellationToken).ConfigureAwait(false);
        }
        catch (DbException error) when (dataset.Optional && !cancellationToken.IsCancellationRequested)
        {
            options.Log?.Invoke($"{dataset.Id}: optional dataset skipped: {error.Message}");
            return default;
        }
    }

    /// <summary>
    /// Writes every reader row as one contract row. Reader columns match table columns case-insensitively, then
    /// ignoring underscores; missing columns are null. Batches hold at most MaxBatchRows rows and MaxBatchBytes.
    /// </summary>
    public static async Task<DmvWriteResult> WriteAsync(
        DbDataReader reader, DmvDataset dataset, DmvWriteOptions options, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(reader);
        ArgumentNullException.ThrowIfNull(dataset);
        ArgumentNullException.ThrowIfNull(options);
        options.Validate();
        var contract = DmvContract.Load(dataset.ContractPath
            ?? throw new InvalidOperationException($"{dataset.Id} has no contract; use CompileMissingContracts or the runtime catalog"));
        var sink = options.Sink ?? ArrowcolSink.Instance;
        var plan = Plan(reader, dataset, contract, options);
        var watermarkIndex = dataset.Watermark is null ? -1 : contract.Fields.ToList().FindIndex(field => field.Name == dataset.Watermark);
        object? watermark = null;
        var pending = new List<byte[]>();
        long pendingBytes = 0, read = 0, written = 0;
        var dropped = 0;
        IDmvBatchStream? stream = null;

        async Task DiscardAsync()
        {
            if (stream is null) return;
            try { await stream.DisposeAsync().ConfigureAwait(false); }
            catch (Exception error) { options.Log?.Invoke($"{dataset.Id}: stream dispose failed: {error.Message}"); }
            stream = null;
        }

        async Task SendAsync()
        {
            if (pending.Count == 0) return;
            for (var attempt = 1; ; attempt++)
            {
                try
                {
                    stream ??= await sink.OpenAsync(dataset, contract, options, cancellationToken).ConfigureAwait(false);
                    await stream.PushBatchAsync(pending).ConfigureAwait(false);
                    await stream.FlushAsync().ConfigureAwait(false);
                    written += pending.Count;
                    break;
                }
                catch (Exception error) when (!cancellationToken.IsCancellationRequested)
                {
                    options.Log?.Invoke($"{dataset.Id}: batch of {pending.Count} rows failed on attempt {attempt}/{options.Attempts}: {error.Message}");
                    await DiscardAsync().ConfigureAwait(false);
                    if (attempt >= options.Attempts)
                    {
                        if (!options.Continuous)
                            throw new ArrowcolException($"{dataset.Id}: batch failed after {attempt} attempts: {error.Message}");
                        dropped++;
                        options.Log?.Invoke($"{dataset.Id}: dropped a batch of {pending.Count} rows");
                        break;
                    }
                    await Task.Delay(options.RetryDelay * attempt, cancellationToken).ConfigureAwait(false);
                }
            }
            pending.Clear();
            pendingBytes = 0;
        }

        try
        {
            var values = new object?[contract.Fields.Count];
            var sampled = DateTime.UtcNow;
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                read++;
                for (var index = 0; index < values.Length; index++) values[index] = plan[index](reader, sampled);
                if (watermarkIndex >= 0 && Mark(values[watermarkIndex]) is { } mark &&
                    (watermark is null || Comparer<object>.Default.Compare(mark, watermark) > 0))
                    watermark = mark;
                var row = contract.Encode(values);
                if (row.Length + 5 > options.MaxBatchBytes)
                {
                    dropped++;
                    options.Log?.Invoke($"{dataset.Id}: dropped a {row.Length}-byte row larger than MaxBatchBytes");
                    continue;
                }
                if (pending.Count == options.MaxBatchRows || pendingBytes + row.Length + 5 > options.MaxBatchBytes)
                    await SendAsync().ConfigureAwait(false);
                pending.Add(row);
                pendingBytes += row.Length + 5;
            }
            await SendAsync().ConfigureAwait(false);
            if (stream is not null)
            {
                try { await stream.CloseAsync().ConfigureAwait(false); }
                catch (Exception error) when (options.Continuous && !cancellationToken.IsCancellationRequested)
                {
                    options.Log?.Invoke($"{dataset.Id}: close failed after flushed batches: {error.Message}");
                }
            }
            if (watermark is not null) options.Watermarks?.Set(dataset.Id, watermark);
            return new DmvWriteResult(read, written, dropped);
        }
        finally
        {
            await DiscardAsync().ConfigureAwait(false);
        }
    }

    private static Func<DbDataReader, DateTime, object?>[] Plan(
        DbDataReader reader, DmvDataset dataset, DmvContract contract, DmvWriteOptions options)
    {
        var exact = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        var loose = new Dictionary<string, List<int>>(StringComparer.OrdinalIgnoreCase);
        for (var ordinal = 0; ordinal < reader.FieldCount; ordinal++)
        {
            var name = reader.GetName(ordinal);
            if (!exact.TryAdd(name, ordinal)) throw new InvalidDataException($"{dataset.Id}: reader returns column {name} twice");
            var key = name.Replace("_", "", StringComparison.Ordinal);
            if (!loose.TryGetValue(key, out var ordinals)) loose[key] = ordinals = [];
            ordinals.Add(ordinal);
        }
        var used = new HashSet<int>();
        var plan = contract.Fields.Select<DmvContract.Field, Func<DbDataReader, DateTime, object?>>(field =>
        {
            if (options.Constants.TryGetValue(field.Name, out var constant)) return (_, _) => constant;
            if (field.Name == DmvCatalog.CustomerId) return (_, _) => options.CustomerId;
            if (!exact.TryGetValue(field.Name, out var ordinal))
            {
                if (!loose.TryGetValue(field.Name.Replace("_", "", StringComparison.Ordinal), out var ordinals))
                {
                    if (field.Name == DmvCatalog.SampleTime) return (_, sampled) => sampled;
                    return (_, _) => null;
                }
                if (ordinals.Count > 1)
                    throw new InvalidDataException($"{dataset.Id}: column {field.Name} matches several reader columns");
                ordinal = ordinals[0];
            }
            if (!used.Add(ordinal)) throw new InvalidDataException($"{dataset.Id}: reader column {reader.GetName(ordinal)} matches several table columns");
            return (row, _) => Read(row, ordinal);
        }).ToArray();
        var ignored = Enumerable.Range(0, reader.FieldCount).Where(ordinal => !used.Contains(ordinal)).Select(reader.GetName).ToList();
        if (ignored.Count > 0) options.Log?.Invoke($"{dataset.Id}: ignoring reader columns {string.Join(", ", ignored)}");
        return plan;
    }

    private static object? Read(DbDataReader reader, int ordinal)
    {
        if (reader.IsDBNull(ordinal)) return null;
        try { return reader.GetValue(ordinal); }
        catch (OverflowException) { return reader.GetProviderSpecificValue(ordinal)?.ToString(); }
    }

    private static object? Mark(object? value) => value switch
    {
        DateTime instant => instant.Kind == DateTimeKind.Local ? instant.ToUniversalTime() : DateTime.SpecifyKind(instant, DateTimeKind.Utc),
        DateTimeOffset instant => instant.UtcDateTime,
        null => null,
        _ => System.Convert.ToInt64(value, System.Globalization.CultureInfo.InvariantCulture),
    };
}
