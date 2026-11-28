using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Reflection;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Arrowcol.Dmv;
using Microsoft.Data.SqlClient;

// Cold-start, crash and warm-restart smoke for the packaged Windows runtime. Set ARROWCOL_SMOKE_SQLSERVER to
// collect every sqlserver DMV dataset from a live engine; otherwise synthetic rows are written.
var sql = Environment.GetEnvironmentVariable("ARROWCOL_SMOKE_SQLSERVER");
// Views the catalog describes at runtime. The first six exist only on cloud engines and are skipped elsewhere;
// dm_os_wait_stats and dm_os_schedulers have no per-view table, so a box engine still exercises the path.
string[] described =
[
    "dm_clr_blessed_assemblies", "dm_os_memory_block_allocator_stats", "dm_xtp_consumer_memory_usage",
    "dm_xtp_memory_stats", "dm_xtp_transaction_events", "dm_db_resource_governor_configuration",
    "dm_os_wait_stats", "dm_os_schedulers",
];
int[] ports = [8817, 32310, 32311, 32312];

if (args is ["--hold", var held])
{
    var runtime = await WindowsLocalRuntime.StartAsync(new(held) { Catalog = await CatalogAsync(), Log = Console.Error.WriteLine });
    Console.WriteLine("ready");
    await runtime.Completion;
    return;
}

var output = AppContext.BaseDirectory;
var runtimeDirectory = Path.Combine(output, WindowsLocalRuntime.RuntimeFolder);
var work = Path.Combine(Path.GetTempPath(), $"arrowcol dmv smoke {Guid.NewGuid():N}");
var root = Path.Combine(work, "root");
var clock = Stopwatch.StartNew();
void Log(string message) => Console.WriteLine($"[{clock.Elapsed.TotalSeconds,6:0.0}s] {message}");
var expected = new Dictionary<string, long>(StringComparer.Ordinal);
try
{
    foreach (var name in new[] { DmvCatalog.FileName, Path.Combine(runtimeDirectory, "runtime.json") })
        if (!File.Exists(Path.Combine(output, name))) throw new FileNotFoundException("Missing packaged content", name);

    var tampered = Path.Combine(work, "tampered");
    Directory.CreateDirectory(tampered);
    foreach (var file in Directory.EnumerateFiles(runtimeDirectory)) File.Copy(file, Path.Combine(tampered, Path.GetFileName(file)));
    File.AppendAllText(Path.Combine(tampered, "config.sdk.yaml"), "# tampered\n");
    await RejectedAsync<InvalidDataException>("tampered runtime", new(Path.Combine(work, "tampered-root")) { RuntimeDirectory = tampered });

    var catalog = await CatalogAsync();
    await using (var runtime = await WindowsLocalRuntime.StartAsync(new(root) { Catalog = catalog, Log = Log }))
    {
        Log($"cold start served {runtime.Catalog.Datasets.Count} tables");
        if (sql is not null && runtime.Catalog.Datasets.Count(dataset => dataset.Provenance.Contains("runtime")) < 2)
            throw new InvalidOperationException("runtime-described datasets were not served");
        await RejectedAsync<IOException>("second owner", new(root) { Catalog = catalog });
        await WriteAsync(runtime, collect: true);
        await runtime.StopAsync();
        await runtime.Completion;
        Log("drained and stopped");
    }
    await PortsFreeAsync(TimeSpan.Zero);
    VerifyDelta(Path.Combine(root, "delta"));

    var start = new ProcessStartInfo(Environment.ProcessPath!) { RedirectStandardOutput = true, UseShellExecute = false };
    if (Path.GetFileNameWithoutExtension(start.FileName).Equals("dotnet", StringComparison.OrdinalIgnoreCase))
        start.ArgumentList.Add(Assembly.GetEntryAssembly()!.Location);
    start.ArgumentList.Add("--hold");
    start.ArgumentList.Add(root);
    using (var holder = Process.Start(start)!)
    {
        using var ready = new CancellationTokenSource(TimeSpan.FromMinutes(3));
        if (await holder.StandardOutput.ReadLineAsync(ready.Token) != "ready") throw new InvalidOperationException("holder did not start");
        holder.Kill();
        await holder.WaitForExitAsync();
    }
    await PortsFreeAsync(TimeSpan.FromSeconds(15));
    Log("killing the owner took its catalog and collector down");

    await using (var runtime = await WindowsLocalRuntime.StartAsync(new(root) { Catalog = catalog, Log = Log }))
    {
        Log("warm restart after crash");
        await WriteAsync(runtime, collect: false);
        await runtime.StopAsync();
    }
    await PortsFreeAsync(TimeSpan.Zero);
    VerifyDelta(Path.Combine(root, "delta"));
    Log($"OK: {expected.Values.Sum()} rows across {expected.Count(table => table.Value > 0)} Delta tables");
}
finally
{
    for (var attempt = 0; Directory.Exists(work); attempt++)
    {
        try { Directory.Delete(work, recursive: true); }
        catch (IOException) when (attempt < 20) { await Task.Delay(500); }
    }
}

async Task<DmvCatalog> CatalogAsync()
{
    var catalog = DmvCatalog.LoadPackaged();
    if (sql is null) return catalog;
    await using var connection = new SqlConnection(sql);
    await connection.OpenAsync();
    return catalog.With(await DmvCatalog.DescribeAsync(connection, "dmv", "sqlserver", described.Select(view => new DmvViewSpec(view))));
}

async Task WriteAsync(WindowsLocalRuntime runtime, bool collect)
{
    var options = new DmvWriteOptions(runtime.Endpoint, "smoke-customer")
    {
        Continuous = false,
        Constants = new Dictionary<string, object?> { ["source_server_name"] = "smoke-server" },
        Log = Log,
    };
    async Task Record(DmvDataset dataset, Task<DmvWriteResult> write)
    {
        var result = await write;
        if (result.RowsWritten != result.RowsRead || result.BatchesDropped != 0)
            throw new InvalidOperationException($"{dataset.Id}: wrote {result.RowsWritten} of {result.RowsRead} rows");
        expected[dataset.Table] = expected.GetValueOrDefault(dataset.Table) + result.RowsWritten;
    }
    if (collect && sql is not null)
    {
        await using var connection = new SqlConnection(sql);
        await connection.OpenAsync();
        var datasets = runtime.Catalog.ForTarget("sqlserver").Where(dataset => dataset.Family == "dmv" && dataset.Collectable).ToList();
        foreach (var dataset in datasets) await Record(dataset, DmvWriter.CollectAsync(connection, dataset, options));
        Log($"collected {datasets.Count} live datasets, {datasets.Count(dataset => expected.GetValueOrDefault(dataset.Table) > 0)} populated");
    }
    foreach (var (id, rows) in new[] { ("sqlserver_dmv_v2.sqlserver_cpu_utilization", 3), ("dmv.dm_os_latch_stats", 2500), ("sqlmi.wait_stats", 7) })
    {
        var dataset = runtime.Catalog.Get(id);
        await Record(dataset, DmvWriter.WriteAsync(Synthetic(dataset, rows), dataset, options));
    }
    Log("wrote synthetic legacy, per-view and PaaS rows");
}

static DbDataReader Synthetic(DmvDataset dataset, int rows)
{
    var table = new DataTable();
    var columns = dataset.Columns.Where(column => column.Name is not (DmvCatalog.CustomerId or DmvCatalog.SampleTime)
        && !DmvCatalog.IdentityColumns.Contains(column.Name)).ToList();
    foreach (var column in columns) table.Columns.Add(column.Name, ClrType(column.Type));
    for (var row = 0; row < rows; row++)
        table.Rows.Add(columns.Select(column => row % 5 == 4 && column.Nullable ? DBNull.Value : Value(column.Type, row)).ToArray());
    return table.CreateDataReader();
}

static Type ClrType(string type) => type switch
{
    "BOOLEAN" => typeof(bool),
    "SMALLINT" => typeof(short),
    "INT" => typeof(int),
    "BIGINT" => typeof(long),
    "FLOAT" => typeof(float),
    "DOUBLE" => typeof(double),
    "DATE" or "TIMESTAMP" => typeof(DateTime),
    _ when type.StartsWith("DECIMAL", StringComparison.Ordinal) => typeof(decimal),
    _ => typeof(string),
};

static object Value(string type, int row) => type switch
{
    "BOOLEAN" => row % 2 == 0,
    "SMALLINT" => (short)row,
    "INT" => row,
    "BIGINT" => row * 1_000_000_007L,
    "FLOAT" => row / 4f,
    "DOUBLE" => row / 3d,
    "DATE" or "TIMESTAMP" => DateTime.UtcNow.Date.AddSeconds(row),
    _ when type.StartsWith("DECIMAL", StringComparison.Ordinal) => row + 0.123456789012m,
    _ => $"row-{row}",
};

async Task RejectedAsync<T>(string what, WindowsLocalRuntimeOptions options) where T : Exception
{
    try
    {
        await (await WindowsLocalRuntime.StartAsync(options)).DisposeAsync();
    }
    catch (T error)
    {
        Log($"{what} rejected: {error.Message}");
        return;
    }
    throw new InvalidOperationException($"{what} was not rejected");
}

async Task PortsFreeAsync(TimeSpan within)
{
    var deadline = DateTime.UtcNow + within;
    foreach (var port in ports)
    {
        while (true)
        {
            var listener = new TcpListener(IPAddress.Loopback, port);
            try
            {
                listener.Start();
                listener.Stop();
                break;
            }
            catch (SocketException) when (DateTime.UtcNow < deadline)
            {
                await Task.Delay(250);
            }
        }
    }
    foreach (var name in new[] { "arrowcol-catalog-rust", "arrowcol-dataflow-rust" })
        if (Process.GetProcessesByName(name) is { Length: > 0 } left)
            throw new InvalidOperationException($"{left.Length} {name} process(es) outlived their owner");
}

void VerifyDelta(string delta)
{
    foreach (var (table, rows) in expected.Where(table => table.Value > 0))
    {
        var log = Path.Combine([delta, .. table.Split('.'), "_delta_log"]);
        var files = new Dictionary<string, long>(StringComparer.Ordinal);
        foreach (var commit in Directory.EnumerateFiles(log, "*.json").Order(StringComparer.Ordinal))
            foreach (var line in File.ReadLines(commit).Where(line => line.Length > 0))
            {
                using var action = JsonDocument.Parse(line);
                if (action.RootElement.TryGetProperty("add", out var add))
                {
                    using var stats = JsonDocument.Parse(add.GetProperty("stats").GetString()!);
                    files[add.GetProperty("path").GetString()!] = stats.RootElement.GetProperty("numRecords").GetInt64();
                }
                else if (action.RootElement.TryGetProperty("remove", out var remove))
                    files.Remove(remove.GetProperty("path").GetString()!);
            }
        if (files.Values.Sum() != rows) throw new InvalidOperationException($"{table}: Delta holds {files.Values.Sum()} rows, wrote {rows}");
    }
    Log($"Delta row counts match for {expected.Count(table => table.Value > 0)} tables");
}
