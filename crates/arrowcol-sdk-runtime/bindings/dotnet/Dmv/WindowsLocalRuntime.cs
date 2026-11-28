using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Sockets;
using System.Runtime.InteropServices;
using System.Runtime.Versioning;
using System.Security.AccessControl;
using System.Security.Cryptography;
using System.Security.Principal;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Arrowcol.Client;
using Microsoft.Win32.SafeHandles;

namespace Arrowcol.Dmv;

/// <summary>Where the local runtime keeps its state, and which catalog it serves.</summary>
/// <param name="Root">Absolute, service-owned directory on local NTFS.</param>
public sealed record WindowsLocalRuntimeOptions(string Root)
{
    /// <summary>The packaged runtime; defaults to <c>&lt;AppContext.BaseDirectory&gt;/arrowcol-runtime</c>.</summary>
    public string? RuntimeDirectory { get; init; }
    /// <summary>Datasets to register; defaults to <see cref="DmvCatalog.LoadPackaged"/>.</summary>
    public DmvCatalog? Catalog { get; init; }
    public TimeSpan StartupTimeout { get; init; } = TimeSpan.FromMinutes(3);
    /// <summary>Bound for each process to exit during <see cref="WindowsLocalRuntime.StopAsync"/>.</summary>
    public TimeSpan DrainTimeout { get; init; } = TimeSpan.FromSeconds(85);
    public Action<string>? Log { get; init; }
}

/// <summary>
/// Owns a loopback ArrowCol catalog and collector on Windows: verifies the packaged executables, registers every
/// catalog table, waits for readiness and drains to Delta on stop. Child processes die with the owning process.
/// </summary>
public sealed class WindowsLocalRuntime : IAsyncDisposable
{
    public const string RuntimeFolder = "arrowcol-runtime";
    private const string CatalogExe = "arrowcol-catalog-rust.exe";
    private const string DataflowExe = "arrowcol-dataflow-rust.exe";
    private const string ConfigFile = "config.sdk.yaml";
    private const string CatalogListen = "127.0.0.1:8817";
    private const string ReadyUrl = "http://127.0.0.1:32311/readyz";
    private const string StatusUrl = "http://127.0.0.1:32312/api/v1/status";
    private const string ShutdownUrl = "http://127.0.0.1:32312/api/v1/groups/shutdown?wait=false&timeout_secs=60";
    private static readonly int[] Ports = [8817, 32310, 32311, 32312];
    private static readonly TimeSpan Poll = TimeSpan.FromMilliseconds(500);

    private readonly WindowsLocalRuntimeOptions options;
    private readonly string root;
    private readonly HttpClient http = new(new SocketsHttpHandler { UseProxy = false }) { Timeout = TimeSpan.FromSeconds(5) };
    private readonly TaskCompletionSource completion = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly SemaphoreSlim gate = new(1, 1);
    private readonly List<FileStream> pinned = [];
    private readonly List<BoundedLog> logs = [];
    private FileStream? ownerLock;
    private JobHandle? job;
    private Process? catalogProcess;
    private Process? dataflowProcess;
    private BoundedLog? catalogLog;
    private BoundedLog? dataflowLog;
    private string runtimeDirectory = "";
    private string keyFile = "";
    private volatile bool running;
    private volatile bool stopping;
    private bool stopped;

    /// <summary>Flight endpoint for <see cref="DmvWriteOptions"/>.</summary>
    public string Endpoint => "http://127.0.0.1:32310";
    /// <summary>The served catalog; every dataset has a <see cref="DmvDataset.ContractPath"/>.</summary>
    public DmvCatalog Catalog { get; private set; } = null!;
    /// <summary>Delta warehouse: <c>&lt;Root&gt;/delta/&lt;catalog&gt;/&lt;schema&gt;/&lt;table&gt;</c>.</summary>
    public string DeltaRoot { get; private set; } = "";
    /// <summary>Completes after a clean stop; faults if a process exits unexpectedly or the drain fails.</summary>
    public Task Completion => completion.Task;

    private WindowsLocalRuntime(WindowsLocalRuntimeOptions options)
    {
        this.options = options;
        root = Path.GetFullPath(options.Root);
    }

    public static async Task<WindowsLocalRuntime> StartAsync(
        WindowsLocalRuntimeOptions options, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(options);
        if (!OperatingSystem.IsWindows()) throw new PlatformNotSupportedException("The ArrowCol local runtime requires Windows.");
        if (string.IsNullOrWhiteSpace(options.Root) || !Path.IsPathFullyQualified(options.Root))
            throw new ArgumentException("Root must be an absolute path.", nameof(options));
        if (options.StartupTimeout <= TimeSpan.Zero || options.DrainTimeout <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(options), "Timeouts must be positive.");
        var runtime = new WindowsLocalRuntime(options);
        try
        {
            await runtime.StartCoreAsync(cancellationToken).ConfigureAwait(false);
            return runtime;
        }
        catch
        {
            runtime.Release();
            throw;
        }
    }

    [SupportedOSPlatform("windows")]
    private async Task StartCoreAsync(CancellationToken cancellationToken)
    {
        using var startup = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        startup.CancelAfter(options.StartupTimeout);
        Directory.CreateDirectory(root);
        RejectReparsePoint(root);
        ownerLock = AcquireOwner(Path.Combine(root, "owner.lock"));
        runtimeDirectory = Path.GetFullPath(options.RuntimeDirectory ?? Path.Combine(AppContext.BaseDirectory, RuntimeFolder));
        VerifyRuntime();
        var state = Prepare("state");
        var ddl = Prepare("ddl");
        var contracts = Prepare("contracts");
        var logDirectory = Prepare("logs");
        DeltaRoot = Prepare("delta");
        keyFile = EnsureKey(Path.Combine(root, "identity.key"));
        foreach (var port in Ports) EnsurePortFree(port);

        Catalog = (options.Catalog ?? DmvCatalog.LoadPackaged()).CompileMissingContracts(contracts);
        foreach (var stale in Directory.EnumerateFiles(ddl, "*.sql")) File.Delete(stale);
        foreach (var dataset in Catalog.Datasets)
            File.WriteAllText(Path.Combine(ddl, dataset.Id + ".sql"), dataset.Ddl, new UTF8Encoding(false));

        job = JobHandle.Create();
        catalogLog = OpenLog(Path.Combine(logDirectory, "catalog.log"));
        dataflowLog = OpenLog(Path.Combine(logDirectory, "dataflow.log"));
        catalogProcess = Launch(CatalogExe, catalogLog,
            "--listen", CatalogListen, "--warehouse-root", new Uri(DeltaRoot).AbsoluteUri, "--state-dir", state,
            "--ddl-dir", ddl, "--ddl-auth-mode", "signed_identity", "--ingestion-layout", "--shutdown-on-stdin-eof");
        await WaitReadyAsync(catalogProcess, catalogLog, "catalog", CatalogHealthyAsync, startup.Token, cancellationToken)
            .ConfigureAwait(false);
        dataflowProcess = Launch(DataflowExe, dataflowLog, "--config", Path.Combine(runtimeDirectory, ConfigFile));
        await WaitReadyAsync(dataflowProcess, dataflowLog, "collector", async token =>
            await OkAsync(ReadyUrl, token).ConfigureAwait(false) && await OkAsync(StatusUrl, token).ConfigureAwait(false),
            startup.Token, cancellationToken).ConfigureAwait(false);
        running = true;
        foreach (var (process, name, log) in new[] { (catalogProcess, "catalog", catalogLog), (dataflowProcess, "collector", dataflowLog) })
            process.Exited += (_, _) => Unexpected(process, name, log);
        foreach (var (process, name, log) in new[] { (catalogProcess, "catalog", catalogLog), (dataflowProcess, "collector", dataflowLog) })
            if (process.HasExited) Unexpected(process, name, log);
        options.Log?.Invoke($"ArrowCol runtime ready at {Endpoint} with {Catalog.Datasets.Count} tables under {DeltaRoot}");
    }

    /// <summary>
    /// Drains the collector to Delta, then the catalog, each bounded by the drain timeout. Stop producers first. Throws
    /// <see cref="ArrowcolException"/> if either process does not exit cleanly; the processes are gone either way.
    /// </summary>
    public async Task StopAsync(CancellationToken cancellationToken = default)
    {
        await gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (stopped) return;
            stopping = true;
            var failures = new List<string>();
            try
            {
                if (dataflowProcess is { HasExited: false })
                {
                    try
                    {
                        using var response = await http.PostAsync(ShutdownUrl, null, cancellationToken).ConfigureAwait(false);
                        if (response.StatusCode != HttpStatusCode.Accepted)
                            failures.Add($"collector shutdown returned HTTP {(int)response.StatusCode}");
                    }
                    catch (Exception error) when (error is HttpRequestException or TaskCanceledException && !cancellationToken.IsCancellationRequested)
                    {
                        failures.Add($"collector shutdown request failed: {error.Message}");
                    }
                }
                await ExitAsync(dataflowProcess, dataflowLog, "collector", failures, cancellationToken).ConfigureAwait(false);
                if (catalogProcess is { HasExited: false }) catalogProcess.StandardInput.Close();
                await ExitAsync(catalogProcess, catalogLog, "catalog", failures, cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                stopped = true;
                Release();
            }
            if (failures.Count > 0)
            {
                var error = new ArrowcolException($"ArrowCol runtime did not drain cleanly: {string.Join("; ", failures)}");
                completion.TrySetException(error);
                throw error;
            }
            completion.TrySetResult();
        }
        finally
        {
            gate.Release();
        }
    }

    public async ValueTask DisposeAsync()
    {
        try
        {
            await StopAsync().ConfigureAwait(false);
        }
        catch (Exception error)
        {
            options.Log?.Invoke(error.Message);
        }
        gate.Dispose();
        http.Dispose();
    }

    private void Unexpected(Process process, string name, BoundedLog log)
    {
        if (!running || stopping) return;
        var error = new ArrowcolException($"ArrowCol {name} exited unexpectedly with code {process.ExitCode}{log.Tail()}");
        options.Log?.Invoke(error.Message);
        completion.TrySetException(error);
    }

    private async Task ExitAsync(Process? process, BoundedLog? log, string name, List<string> failures, CancellationToken cancellationToken)
    {
        if (process is null) return;
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeout.CancelAfter(options.DrainTimeout);
        try
        {
            await process.WaitForExitAsync(timeout.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            failures.Add($"{name} did not exit within {options.DrainTimeout.TotalSeconds:0}s{log?.Tail()}");
            return;
        }
        if (process.ExitCode != 0) failures.Add($"{name} exited with code {process.ExitCode}{log?.Tail()}");
    }

    [SupportedOSPlatform("windows")]
    private Process Launch(string executable, BoundedLog log, params string[] arguments)
    {
        var info = new ProcessStartInfo(Path.Combine(runtimeDirectory, executable))
        {
            UseShellExecute = false,
            CreateNoWindow = true,
            RedirectStandardInput = true,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            WorkingDirectory = root,
        };
        foreach (var argument in arguments) info.ArgumentList.Add(argument);
        foreach (var name in info.Environment.Keys.Where(key => key.StartsWith("ARROWCOL_", StringComparison.OrdinalIgnoreCase)).ToList())
            info.Environment.Remove(name);
        info.Environment["ARROWCOL_CATALOG_DDL_IDENTITY_SECRET_FILE"] = keyFile;
        info.Environment["ARROWCOL_DELTA_ROOT"] = DeltaRoot.Replace('\\', '/');
        info.Environment["ARROWCOL_PARTITION_GRANULARITY"] = "hour";
        if (!info.Environment.ContainsKey("RUST_LOG")) info.Environment["RUST_LOG"] = "warn";
        var process = new Process { StartInfo = info, EnableRaisingEvents = true };
        process.OutputDataReceived += (_, line) => log.Write(line.Data);
        process.ErrorDataReceived += (_, line) => log.Write(line.Data);
        process.Start();
        try
        {
            job!.Assign(process);
        }
        catch
        {
            process.Kill();
            process.Dispose();
            throw;
        }
        process.BeginOutputReadLine();
        process.BeginErrorReadLine();
        return process;
    }

    private static async Task WaitReadyAsync(
        Process process, BoundedLog log, string name, Func<CancellationToken, Task<bool>> probe,
        CancellationToken startup, CancellationToken caller)
    {
        while (true)
        {
            if (process.HasExited)
                throw new ArrowcolException($"ArrowCol {name} exited during startup with code {process.ExitCode}{log.Tail()}");
            try
            {
                if (await probe(startup).ConfigureAwait(false)) return;
                await Task.Delay(Poll, startup).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (startup.IsCancellationRequested && !caller.IsCancellationRequested)
            {
                throw new TimeoutException($"ArrowCol {name} did not become ready{log.Tail()}");
            }
        }
    }

    [SupportedOSPlatform("windows")]
    private async Task<bool> CatalogHealthyAsync(CancellationToken cancellationToken)
    {
        var info = new ProcessStartInfo(Path.Combine(runtimeDirectory, CatalogExe))
        {
            UseShellExecute = false,
            CreateNoWindow = true,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
        };
        foreach (var argument in new[] { "--listen", CatalogListen, "--ddl-auth-mode", "signed_identity", "--healthcheck" })
            info.ArgumentList.Add(argument);
        info.Environment["ARROWCOL_CATALOG_DDL_IDENTITY_SECRET_FILE"] = keyFile;
        using var check = new Process { StartInfo = info };
        check.OutputDataReceived += (_, _) => { };
        check.ErrorDataReceived += (_, _) => { };
        check.Start();
        job!.Assign(check);
        check.BeginOutputReadLine();
        check.BeginErrorReadLine();
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeout.CancelAfter(TimeSpan.FromSeconds(10));
        try
        {
            await check.WaitForExitAsync(timeout.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            check.Kill();
            if (cancellationToken.IsCancellationRequested) throw;
            return false;
        }
        return check.ExitCode == 0;
    }

    private async Task<bool> OkAsync(string url, CancellationToken cancellationToken)
    {
        try
        {
            using var response = await http.GetAsync(url, cancellationToken).ConfigureAwait(false);
            return response.StatusCode == HttpStatusCode.OK;
        }
        catch (Exception error) when (error is HttpRequestException or TaskCanceledException && !cancellationToken.IsCancellationRequested)
        {
            return false;
        }
    }

    private void VerifyRuntime()
    {
        RejectReparsePoint(runtimeDirectory);
        var path = Path.Combine(runtimeDirectory, "runtime.json");
        if (!File.Exists(path))
            throw new FileNotFoundException("The ArrowCol Windows runtime is missing; reference Rust.Playground from a Windows x64 project.", path);
        var manifest = JsonSerializer.Deserialize<RuntimeManifest>(File.ReadAllBytes(path), DmvCatalog.Json);
        if (manifest is not { Version: 1, Files: { } files } || !new[] { CatalogExe, DataflowExe, ConfigFile }.All(files.ContainsKey))
            throw new InvalidDataException($"Unsupported ArrowCol runtime manifest {path}");
        foreach (var (name, digest) in files)
        {
            if (Path.GetFileName(name) != name) throw new InvalidDataException($"Invalid runtime file name {name}");
            var file = Path.Combine(runtimeDirectory, name);
            RejectReparsePoint(file);
            // Hold each verified file open without write or delete sharing so it cannot change before it runs.
            var stream = new FileStream(file, FileMode.Open, FileAccess.Read, FileShare.Read);
            pinned.Add(stream);
            if (!string.Equals(Convert.ToHexString(SHA256.HashData(stream)), digest, StringComparison.OrdinalIgnoreCase))
                throw new InvalidDataException($"ArrowCol runtime file {name} does not match its SHA-256 in runtime.json");
        }
    }

    private string Prepare(string name)
    {
        var path = Path.Combine(root, name);
        Directory.CreateDirectory(path);
        RejectReparsePoint(path);
        return path;
    }

    private static void RejectReparsePoint(string path)
    {
        if ((File.GetAttributes(path) & FileAttributes.ReparsePoint) != 0)
            throw new IOException($"{path} is a reparse point; use a real local directory.");
    }

    private static FileStream AcquireOwner(string path)
    {
        try
        {
            var stream = new FileStream(path, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None, 1, FileOptions.DeleteOnClose);
            stream.Write(Encoding.ASCII.GetBytes(Environment.ProcessId.ToString(System.Globalization.CultureInfo.InvariantCulture)));
            stream.Flush();
            return stream;
        }
        catch (IOException error)
        {
            throw new IOException($"Another ArrowCol runtime owns {Path.GetDirectoryName(path)}.", error);
        }
    }

    [SupportedOSPlatform("windows")]
    private static string EnsureKey(string path)
    {
        if (File.Exists(path))
        {
            RejectReparsePoint(path);
            var existing = File.ReadAllText(path, Encoding.ASCII).TrimEnd('\r', '\n');
            if (existing.Length < 64 || !existing.All(Uri.IsHexDigit))
                throw new InvalidDataException($"{path} is not a 32-byte hex identity key; delete it to start over.");
            return path;
        }
        var security = new FileSecurity();
        security.SetAccessRuleProtection(isProtected: true, preserveInheritance: false);
        foreach (var identity in new IdentityReference[]
        {
            WindowsIdentity.GetCurrent().User!,
            new SecurityIdentifier(WellKnownSidType.LocalSystemSid, null),
            new SecurityIdentifier(WellKnownSidType.BuiltinAdministratorsSid, null),
        })
            security.AddAccessRule(new FileSystemAccessRule(identity, FileSystemRights.FullControl, AccessControlType.Allow));
        using var stream = new FileInfo(path).Create(FileMode.CreateNew, FileSystemRights.FullControl, FileShare.None, 4096, FileOptions.None, security);
        stream.Write(Encoding.ASCII.GetBytes(Convert.ToHexString(RandomNumberGenerator.GetBytes(32)).ToLowerInvariant()));
        return path;
    }

    private static void EnsurePortFree(int port)
    {
        var listener = new TcpListener(IPAddress.Loopback, port);
        try
        {
            listener.Start();
        }
        catch (SocketException error)
        {
            throw new IOException($"127.0.0.1:{port} is in use; stop the process that owns it before starting ArrowCol.", error);
        }
        finally
        {
            listener.Stop();
        }
    }

    private BoundedLog OpenLog(string path)
    {
        var log = new BoundedLog(path);
        logs.Add(log);
        return log;
    }

    private void Release()
    {
        running = false;
        stopping = true;
        foreach (var process in new[] { dataflowProcess, catalogProcess })
        {
            if (process is null) continue;
            try
            {
                if (!process.HasExited) process.Kill(entireProcessTree: true);
            }
            catch (InvalidOperationException)
            {
            }
            process.Dispose();
        }
        dataflowProcess = catalogProcess = null;
        job?.Dispose();
        job = null;
        foreach (var stream in pinned) stream.Dispose();
        pinned.Clear();
        foreach (var log in logs) log.Dispose();
        logs.Clear();
        ownerLock?.Dispose();
        ownerLock = null;
    }

    private sealed record RuntimeManifest(int Version, Dictionary<string, string>? Files);

    private sealed class BoundedLog(string path) : IDisposable
    {
        private const long Limit = 2 * 1024 * 1024;
        private readonly Queue<string> tail = new();
        private StreamWriter? writer = Open(path);
        private static StreamWriter Open(string path) =>
            new(new FileStream(path, FileMode.Append, FileAccess.Write, FileShare.Read), new UTF8Encoding(false)) { AutoFlush = true };

        public void Write(string? line)
        {
            if (line is null) return;
            lock (tail)
            {
                if (writer is null) return;
                if (tail.Count == 20) tail.Dequeue();
                tail.Enqueue(line);
                writer.WriteLine(line);
                if (writer.BaseStream.Length < Limit) return;
                writer.Dispose();
                File.Move(path, path + ".1", overwrite: true);
                writer = Open(path);
            }
        }

        public string Tail()
        {
            lock (tail) return tail.Count == 0 ? "" : $"; last output:{Environment.NewLine}{string.Join(Environment.NewLine, tail)}";
        }

        public void Dispose()
        {
            lock (tail)
            {
                writer?.Dispose();
                writer = null;
            }
        }
    }

    private sealed class JobHandle : SafeHandleZeroOrMinusOneIsInvalid
    {
        private const uint KillOnJobClose = 0x2000;
        private const int ExtendedLimitInformation = 9;

        private JobHandle() : base(ownsHandle: true)
        {
        }

        public static JobHandle Create()
        {
            var job = CreateJobObjectW(IntPtr.Zero, null);
            if (job.IsInvalid) throw new Win32Exception();
            var limits = new ExtendedLimits { Basic = new BasicLimits { LimitFlags = KillOnJobClose } };
            if (!SetInformationJobObject(job, ExtendedLimitInformation, ref limits, (uint)Marshal.SizeOf<ExtendedLimits>()))
            {
                var error = new Win32Exception();
                job.Dispose();
                throw error;
            }
            return job;
        }

        public void Assign(Process process)
        {
            if (!AssignProcessToJobObject(this, process.Handle)) throw new Win32Exception();
        }

        protected override bool ReleaseHandle() => CloseHandle(handle);

        [DllImport("kernel32.dll", SetLastError = true, CharSet = CharSet.Unicode)]
        private static extern JobHandle CreateJobObjectW(IntPtr attributes, string? name);
        [DllImport("kernel32.dll", SetLastError = true)]
        private static extern bool SetInformationJobObject(JobHandle job, int infoClass, ref ExtendedLimits info, uint length);
        [DllImport("kernel32.dll", SetLastError = true)]
        private static extern bool AssignProcessToJobObject(JobHandle job, IntPtr process);
        [DllImport("kernel32.dll", SetLastError = true)]
        private static extern bool CloseHandle(IntPtr handle);

        [StructLayout(LayoutKind.Sequential)]
        private struct BasicLimits
        {
            public long PerProcessUserTimeLimit;
            public long PerJobUserTimeLimit;
            public uint LimitFlags;
            public nuint MinimumWorkingSetSize;
            public nuint MaximumWorkingSetSize;
            public uint ActiveProcessLimit;
            public nuint Affinity;
            public uint PriorityClass;
            public uint SchedulingClass;
        }

        [StructLayout(LayoutKind.Sequential)]
        private struct ExtendedLimits
        {
            public BasicLimits Basic;
            public ulong ReadOperationCount, WriteOperationCount, OtherOperationCount;
            public ulong ReadTransferCount, WriteTransferCount, OtherTransferCount;
            public nuint ProcessMemoryLimit, JobMemoryLimit, PeakProcessMemoryUsed, PeakJobMemoryUsed;
        }
    }
}
