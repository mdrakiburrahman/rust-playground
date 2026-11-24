using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Arrowcol.Generated;
// The prepared demo's Schema.cs aliases Events to the ordinary protobuf message.
// For your own project reference and aliases, see docs/architecture/arrowcol-platform/sdk-developer-guide.md.

string? endpoint = null;
int seconds = 300, intervalMs = 1000;
for (int i = 0; i < args.Length; i += 2)
{
    if (i + 1 == args.Length) throw new ArgumentException("Each option requires a value");
    switch (args[i]) {
        case "--endpoint": endpoint = args[i + 1]; break;
        case "--duration-seconds": seconds = int.Parse(args[i + 1]); break;
        case "--interval-ms": intervalMs = int.Parse(args[i + 1]); break;
        default: throw new ArgumentException($"Unknown option: {args[i]}");
    }
}
if (seconds is < 1 or > 86400 || intervalMs is < 1 or > 60000)
    throw new ArgumentException("Duration must be 1..86400 seconds; interval 1..60000 ms");
endpoint ??= Environment.GetEnvironmentVariable("ARROWCOL_ENDPOINT")
    ?? throw new ArgumentException("Set ARROWCOL_ENDPOINT or pass --endpoint");
if (string.IsNullOrWhiteSpace(endpoint))
    throw new ArgumentException("ARROWCOL_ENDPOINT or --endpoint must not be blank");
using var stop = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) => { e.Cancel = true; stop.Cancel(); };
await using var stream = await ArrowcolEventsStream.ConnectAsync(endpoint);
var clock = Stopwatch.StartNew();
long id = 0;
while (clock.Elapsed.TotalSeconds < seconds && !stop.IsCancellationRequested)
{
    // Edit event values here; the generated adapter supplies the Arrow schema and routing.
    await stream.PushAsync(new Events {
        EventId = id,
        ObservedAt = 1_700_000_000_000_000 + id,
        Payload = $"csharp-event-{id}"
    });
    id++;
    try { await Task.Delay(Math.Min(intervalMs, Math.Max(0, seconds * 1000 - (int)clock.ElapsedMilliseconds)), stop.Token); }
    catch (OperationCanceledException) when (stop.IsCancellationRequested) { break; }
}
await stream.FlushAsync();
await stream.CloseAsync();
Console.WriteLine($"{{\"language\":\"csharp\",\"admitted_rows\":{id},\"sink_commit_verified\":false}}");
