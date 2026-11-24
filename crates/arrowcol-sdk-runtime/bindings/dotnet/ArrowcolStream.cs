using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.InteropServices;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using Google.Protobuf.Reflection;

namespace Arrowcol.Client;

public sealed record StreamOptions
{
    public int BatchRows { get; init; } = 1024;
    public int BatchBytes { get; init; } = 1024 * 1024;
    public int BufferBytes { get; init; } = 32 * 1024 * 1024;
    public int FlushIntervalMs { get; init; } = 1000;
    public int TimeoutMs { get; init; } = 30_000;
}

public interface IArrowcolContract<T>
{
    byte[] Encode(T value);
    NativeApi Native { get; }
}

// Generated adapters supply these delegates; application code never passes pointers.
public sealed record NativeApi(
    NativeApi.OpenCall Open, NativeApi.PushCall Push, NativeApi.HandleCall Flush,
    NativeApi.HandleCall Close, Action<IntPtr> Destroy, Action<IntPtr> FreeError)
{
    public delegate IntPtr OpenCall(string endpoint, string options, out IntPtr handle);
    public delegate IntPtr PushCall(IntPtr handle, byte[] bytes, nuint length, byte batch);
    public delegate IntPtr HandleCall(IntPtr handle);
}

/// <summary>Shared producer. Flush/Close confirm collector admission, not Delta/Kusto publication.</summary>
public sealed class ArrowcolStream<T> : IAsyncDisposable
{
    private readonly IArrowcolContract<T> contract;
    private readonly SemaphoreSlim gate = new(1, 1);
    private readonly int maxRows;
    private readonly StreamHandle handle;

    private sealed class StreamHandle : SafeHandle
    {
        private readonly Action<IntPtr> destroy;
        public StreamHandle(IntPtr value, Action<IntPtr> destroy) : base(IntPtr.Zero, true)
        { SetHandle(value); this.destroy = destroy; }
        public override bool IsInvalid => handle == IntPtr.Zero;
        protected override bool ReleaseHandle() { destroy(handle); return true; }
    }

    private ArrowcolStream(IArrowcolContract<T> contract, IntPtr handle, int maxRows)
    { this.contract = contract; this.handle = new StreamHandle(handle, contract.Native.Destroy); this.maxRows = maxRows; }

    public static async Task<ArrowcolStream<T>> ConnectAsync(
        string endpoint, IArrowcolContract<T> contract, StreamOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(endpoint);
        if (endpoint.Contains('\0')) throw new ArgumentException("Endpoint contains NUL", nameof(endpoint));
        ArgumentNullException.ThrowIfNull(contract);
        options ??= new();
        var json = JsonSerializer.Serialize(options, new JsonSerializerOptions {
            PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower
        });
        return await Task.Run(() => {
            Check(contract.Native, contract.Native.Open(endpoint, json, out var handle));
            return new ArrowcolStream<T>(contract, handle, options.BatchRows);
        }).ConfigureAwait(false);
    }

    public static Task<ArrowcolStream<T>> ConnectAsync(
        string endpoint, ArrowcolContract contract, StreamOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(contract);
        if (Activator.CreateInstance(typeof(T)) is not IMessage model)
            throw new ArgumentException("T must be a generated Google.Protobuf message with a default constructor");
        return ConnectAsync(endpoint, contract.Bind<T>(model.Descriptor), options);
    }

    public Task PushAsync(T value)
    {
        var bytes = contract.Encode(value);
        return ExecuteAsync(() => contract.Native.Push(handle.DangerousGetHandle(), bytes, (nuint)bytes.Length, 0));
    }

    public Task PushBatchAsync(IEnumerable<T> values)
    {
        using var buffer = new MemoryStream();
        int count = 0;
        foreach (var value in values)
        {
            if (++count > maxRows) throw new ArgumentException("Batch exceeds BatchRows", nameof(values));
            var bytes = contract.Encode(value);
            if (buffer.Length + bytes.Length + 5 > 16 * 1024 * 1024)
                throw new ArgumentException("Encoded batch exceeds 16 MiB", nameof(values));
            uint size = (uint)bytes.Length;
            while (size >= 128) { buffer.WriteByte((byte)((size & 127) | 128)); size >>= 7; }
            buffer.WriteByte((byte)size);
            buffer.Write(bytes);
        }
        var encoded = buffer.ToArray();
        return ExecuteAsync(() => contract.Native.Push(handle.DangerousGetHandle(), encoded, (nuint)encoded.Length, 1));
    }

    public Task FlushAsync() => ExecuteAsync(() => contract.Native.Flush(handle.DangerousGetHandle()));
    public Task CloseAsync() => ExecuteAsync(() => contract.Native.Close(handle.DangerousGetHandle()));

    private async Task ExecuteAsync(Func<IntPtr> action)
    {
        await gate.WaitAsync().ConfigureAwait(false);
        try {
            if (handle.IsClosed || handle.IsInvalid) throw new ObjectDisposedException(nameof(ArrowcolStream<T>));
            // Native calls may wait for bounded capacity or an acknowledgement; don't block a UI thread.
            await Task.Run(() => Check(contract.Native, action())).ConfigureAwait(false);
        } finally { GC.KeepAlive(handle); gate.Release(); }
    }

    private static void Check(NativeApi api, IntPtr error)
    {
        if (error == IntPtr.Zero) return;
        try { throw new ArrowcolException(Marshal.PtrToStringUTF8(error) ?? "Native SDK failure"); }
        finally { api.FreeError(error); }
    }

    /// <summary>Releases resources only. Explicitly await CloseAsync to observe delivery errors.</summary>
    public async ValueTask DisposeAsync()
    {
        await gate.WaitAsync().ConfigureAwait(false);
        try {
            await Task.Run(handle.Dispose).ConfigureAwait(false);
        } finally { gate.Release(); }
    }
}

public sealed class ArrowcolException(string message) : Exception(message);

/// <summary>A canonical contract directory. Native open validates all artifacts before connecting.</summary>
public sealed class ArrowcolContract
{
    public string Path { get; }
    private ArrowcolContract(string path) { Path = path; }
    public static ArrowcolContract Load(string path)
    {
        ArgumentException.ThrowIfNullOrEmpty(path);
        if (path.Contains('\0')) throw new ArgumentException("Contract path contains NUL", nameof(path));
        path = System.IO.Path.GetFullPath(path);
        foreach (var name in new[] { "manifest.json", "schema.arrow", "descriptor.pb" })
            if (!File.Exists(System.IO.Path.Combine(path, name)))
                throw new FileNotFoundException($"Missing contract artifact: {name}");
        return new ArrowcolContract(path);
    }

    internal IArrowcolContract<T> Bind<T>(MessageDescriptor descriptor)
    {
        var files = new FileDescriptorSet();
        var seen = new HashSet<string>();
        void Add(FileDescriptor file)
        {
            if (!seen.Add(file.Name)) return;
            foreach (var dependency in file.Dependencies) Add(dependency);
            files.File.Add(FileDescriptorProto.Parser.ParseFrom(file.SerializedData));
        }
        Add(descriptor.File);
        var bytes = files.ToByteArray();
        var api = new NativeApi(
            (string endpoint, string options, out IntPtr handle) =>
                SharedNative.Open(endpoint, Path, bytes, (nuint)bytes.Length, descriptor.FullName, options, out handle),
            SharedNative.Push, SharedNative.Flush, SharedNative.Close, SharedNative.Destroy, SharedNative.FreeError);
        return new ProtobufContract<T>(api);
    }

    private sealed class ProtobufContract<T>(NativeApi api) : IArrowcolContract<T>
    {
        public NativeApi Native => api;
        public byte[] Encode(T value) => value is IMessage message
            ? message.ToByteArray() : throw new ArgumentException("Expected a non-null protobuf message", nameof(value));
    }
}

internal static class SharedNative
{
    private const string Library = "arrowcol_sdk_ffi";
    [DllImport(Library, EntryPoint = "arrowcol_open", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr Open(
        [MarshalAs(UnmanagedType.LPUTF8Str)] string endpoint,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string contractPath,
        byte[] descriptors, nuint descriptorLength,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string modelName,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string options, out IntPtr handle);
    [DllImport(Library, EntryPoint = "arrowcol_push", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr Push(IntPtr handle, byte[] bytes, nuint length, byte batch);
    [DllImport(Library, EntryPoint = "arrowcol_flush", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr Flush(IntPtr handle);
    [DllImport(Library, EntryPoint = "arrowcol_close", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr Close(IntPtr handle);
    [DllImport(Library, EntryPoint = "arrowcol_destroy", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void Destroy(IntPtr handle);
    [DllImport(Library, EntryPoint = "arrowcol_free_error", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void FreeError(IntPtr error);
}
