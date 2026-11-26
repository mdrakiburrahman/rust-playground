using System;
using System.IO;
using System.Runtime.InteropServices;
using Arrowcol.Client;
using Arrowcol.Telemetry.SqlserverDmv;

var assembly = typeof(ArrowcolContract).Assembly;
var library = NativeLibrary.Load("arrowcol_sdk_ffi", assembly, null);
try
{
    foreach (var export in new[] {
        "arrowcol_open", "arrowcol_push", "arrowcol_flush",
        "arrowcol_close", "arrowcol_destroy", "arrowcol_free_error"
    })
        NativeLibrary.GetExport(library, export);
    Console.WriteLine($"ArrowCol native API loaded via NuGet on {RuntimeInformation.RuntimeIdentifier}");
}
finally
{
    NativeLibrary.Free(library);
}

const string dataset = "sqlserver_cpu_utilization";
var output = AppContext.BaseDirectory;
var definition = Path.Combine(output, "arrow-ddl", $"{dataset}.sql");
if (!File.Exists(definition))
    throw new FileNotFoundException("Missing packaged DMV definition.", definition);

var contract = ArrowcolContract.Load(Path.Combine(output, "arrow-contracts", dataset));
var measured = new SqlserverCpuUtilization
{
    CustomerId = "package-exact",
    AvgCpuPercent = "99999999999999999999999999.123456789012",
    SampleTimeUtc = 1700000000123456,
};
var unmeasured = new SqlserverCpuUtilization { CustomerId = "package-null" };
if (SqlserverCpuUtilization.Descriptor.FullName != "arrowcol.telemetry.sqlserver_dmv.SqlserverCpuUtilization" ||
    !measured.HasAvgCpuPercent || unmeasured.HasAvgCpuPercent || unmeasured.HasSampleTimeUtc)
    throw new InvalidOperationException("The generated DMV model does not preserve payload presence.");

if (args.Length != 0)
{
    await using var stream = await ArrowcolStream<SqlserverCpuUtilization>.ConnectAsync(args[0], contract);
    await stream.PushAsync(measured);
    await stream.PushAsync(unmeasured);
    await stream.FlushAsync();
    await stream.CloseAsync();
}
