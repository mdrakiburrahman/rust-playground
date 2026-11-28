using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text.Json;
using Google.Protobuf;
using Google.Protobuf.Reflection;
using ProtoType = Google.Protobuf.Reflection.FieldDescriptorProto.Types.Type;

namespace Arrowcol.Dmv;

/// <summary>
/// Encodes rows for one contract directory. Field numbers and wire types come from <c>descriptor.pb</c>; value
/// conversion follows the manifest's Arrow types. Values that cannot be represented exactly become null.
/// </summary>
internal sealed class DmvContract
{
    private static readonly ConcurrentDictionary<string, DmvContract> Cache = new(StringComparer.Ordinal);
    private static readonly DateTime Epoch = DateTime.UnixEpoch;

    public string Path { get; }
    public string ModelName { get; }
    public byte[] DescriptorSet { get; }
    public IReadOnlyList<Field> Fields { get; }

    internal sealed record Field(string Name, int Number, ProtoType Wire, string Kind, bool Nullable, int Precision, int Scale, string? Unit);

    private DmvContract(string path, string modelName, byte[] descriptorSet, IReadOnlyList<Field> fields)
    { Path = path; ModelName = modelName; DescriptorSet = descriptorSet; Fields = fields; }

    public static DmvContract Load(string path) => Cache.GetOrAdd(System.IO.Path.GetFullPath(path), Read);

    private static DmvContract Read(string path)
    {
        var descriptorSet = File.ReadAllBytes(System.IO.Path.Combine(path, "descriptor.pb"));
        var files = FileDescriptorSet.Parser.ParseFrom(descriptorSet);
        var file = files.File.SingleOrDefault(file => file.Name == "schema.proto") ?? files.File.Last();
        var message = file.MessageType.Count == 1
            ? file.MessageType[0] : throw new InvalidDataException($"{path}: schema.proto must declare one message");
        var wire = message.Field.ToDictionary(field => field.Name, StringComparer.Ordinal);
        using var manifest = JsonDocument.Parse(File.ReadAllBytes(System.IO.Path.Combine(path, "manifest.json")));
        var fields = manifest.RootElement.GetProperty("schema").GetProperty("fields").EnumerateArray().Select(field =>
        {
            var name = field.GetProperty("name").GetString()!;
            var arrow = field.GetProperty("mapping").GetProperty("arrow");
            if (!wire.TryGetValue(name, out var proto) || proto.Number != field.GetProperty("field_number").GetInt32())
                throw new InvalidDataException($"{path}: descriptor does not match manifest field {name}");
            return new Field(name, proto.Number, proto.Type, arrow.GetProperty("kind").GetString()!,
                field.GetProperty("nullable").GetBoolean(),
                arrow.TryGetProperty("precision", out var precision) ? precision.GetInt32() : 0,
                arrow.TryGetProperty("scale", out var scale) ? scale.GetInt32() : 0,
                arrow.TryGetProperty("unit", out var unit) ? unit.GetString() : null);
        }).ToList();
        var model = string.IsNullOrEmpty(file.Package) ? message.Name : $"{file.Package}.{message.Name}";
        return new DmvContract(path, model, descriptorSet, fields);
    }

    /// <summary>Encodes one row; <paramref name="values"/> is indexed like <see cref="Fields"/>.</summary>
    public byte[] Encode(object?[] values)
    {
        using var buffer = new MemoryStream();
        var output = new CodedOutputStream(buffer, leaveOpen: true);
        for (var index = 0; index < Fields.Count; index++)
        {
            var field = Fields[index];
            var value = Convert(field, values[index]);
            if (value is null)
            {
                if (!field.Nullable) throw new InvalidDataException($"Column {field.Name} is NOT NULL but has no value");
                continue;
            }
            Write(output, field, value);
        }
        output.Flush();
        return buffer.ToArray();
    }

    internal static object? Convert(Field field, object? value)
    {
        if (value is null or DBNull) return null;
        try
        {
            return field.Kind switch
            {
                "utf8" or "large_utf8" or "utf8_view" => Text(value),
                "boolean" => value switch
                {
                    bool flag => flag,
                    string text => text.Trim() switch { "1" or "true" or "True" or "TRUE" => true, "0" or "false" or "False" or "FALSE" => false, _ => null },
                    _ => System.Convert.ToDecimal(value, CultureInfo.InvariantCulture) != 0,
                },
                "int8" => Integer(value, sbyte.MinValue, sbyte.MaxValue),
                "int16" => Integer(value, short.MinValue, short.MaxValue),
                "int32" => Integer(value, int.MinValue, int.MaxValue),
                "int64" => Integer(value, long.MinValue, long.MaxValue),
                "u_int8" => Integer(value, 0, byte.MaxValue),
                "u_int16" => Integer(value, 0, ushort.MaxValue),
                "u_int32" => Integer(value, 0, uint.MaxValue),
                "u_int64" => value is ulong unsigned ? unsigned : Integer(value, 0, long.MaxValue),
                "float32" => System.Convert.ToSingle(value, CultureInfo.InvariantCulture),
                "float64" => System.Convert.ToDouble(value, CultureInfo.InvariantCulture),
                "decimal128" => Decimal(value, field.Precision, field.Scale),
                "date32" => Instant(value) is { } date ? (long)(date.Date - Epoch).TotalDays : null,
                "timestamp" => Instant(value) is { } instant ? Ticks(instant.Ticks - Epoch.Ticks, field.Unit) : null,
                _ => throw new InvalidDataException($"Unsupported contract type {field.Kind} for {field.Name}"),
            };
        }
        catch (Exception error) when (error is FormatException or InvalidCastException or OverflowException)
        {
            return null;
        }
    }

    private static string Text(object value) => value switch
    {
        string text => text,
        Guid guid => guid.ToString("D"),
        TimeSpan time => time.ToString("c", CultureInfo.InvariantCulture),
        DateTime instant => instant.ToString("O", CultureInfo.InvariantCulture),
        DateTimeOffset instant => instant.ToString("O", CultureInfo.InvariantCulture),
        bool flag => flag ? "true" : "false",
        byte[] bytes => System.Convert.ToHexString(bytes),
        IFormattable formattable => formattable.ToString(null, CultureInfo.InvariantCulture),
        _ => value.ToString() ?? "",
    };

    private static object? Integer(object value, long minimum, long maximum)
    {
        long result = value switch
        {
            bool flag => flag ? 1 : 0,
            ulong unsigned when unsigned > long.MaxValue => throw new OverflowException(),
            string text => long.Parse(text.Trim(), NumberStyles.Integer, CultureInfo.InvariantCulture),
            float or double or decimal => (long)Math.Round(System.Convert.ToDecimal(value, CultureInfo.InvariantCulture), MidpointRounding.ToEven),
            _ => System.Convert.ToInt64(value, CultureInfo.InvariantCulture),
        };
        return result < minimum || result > maximum ? null : result;
    }

    private static string? Decimal(object value, int precision, int scale)
    {
        decimal number = value switch
        {
            string text => decimal.Parse(text.Trim(), NumberStyles.Float, CultureInfo.InvariantCulture),
            double or float when !double.IsFinite(System.Convert.ToDouble(value, CultureInfo.InvariantCulture)) => throw new OverflowException(),
            _ => System.Convert.ToDecimal(value, CultureInfo.InvariantCulture),
        };
        number = Math.Round(number, Math.Min(scale, 28), MidpointRounding.ToEven);
        var limit = precision - scale >= 29 ? decimal.MaxValue : Pow10(precision - scale);
        return Math.Abs(number) >= limit ? null : number.ToString(CultureInfo.InvariantCulture);
    }

    private static decimal Pow10(int exponent)
    {
        var result = 1m;
        for (var index = 0; index < exponent; index++) result *= 10;
        return result;
    }

    private static DateTime? Instant(object value) => value switch
    {
        DateTime instant => instant.Kind == DateTimeKind.Local ? instant.ToUniversalTime() : instant,
        DateTimeOffset instant => instant.UtcDateTime,
        string text => DateTimeOffset.Parse(text, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal).UtcDateTime,
        _ => null,
    };

    private static long Ticks(long ticks, string? unit) => unit switch
    {
        "second" => FloorDiv(ticks, TimeSpan.TicksPerSecond),
        "millisecond" => FloorDiv(ticks, TimeSpan.TicksPerMillisecond),
        "nanosecond" => checked(ticks * 100),
        _ => FloorDiv(ticks, 10),
    };

    private static long FloorDiv(long value, long divisor)
    {
        var quotient = Math.DivRem(value, divisor, out var remainder);
        return remainder < 0 ? quotient - 1 : quotient;
    }

    private static void Write(CodedOutputStream output, Field field, object value)
    {
        switch (field.Wire)
        {
            case ProtoType.String:
                output.WriteTag(field.Number, WireFormat.WireType.LengthDelimited);
                output.WriteString((string)value);
                break;
            case ProtoType.Bool:
                output.WriteTag(field.Number, WireFormat.WireType.Varint);
                output.WriteBool((bool)value);
                break;
            case ProtoType.Float:
                output.WriteTag(field.Number, WireFormat.WireType.Fixed32);
                output.WriteFloat(System.Convert.ToSingle(value, CultureInfo.InvariantCulture));
                break;
            case ProtoType.Double:
                output.WriteTag(field.Number, WireFormat.WireType.Fixed64);
                output.WriteDouble(System.Convert.ToDouble(value, CultureInfo.InvariantCulture));
                break;
            case ProtoType.Sint32:
                output.WriteTag(field.Number, WireFormat.WireType.Varint);
                output.WriteSInt32(checked((int)(long)value));
                break;
            case ProtoType.Sint64:
                output.WriteTag(field.Number, WireFormat.WireType.Varint);
                output.WriteSInt64((long)value);
                break;
            case ProtoType.Int32:
                output.WriteTag(field.Number, WireFormat.WireType.Varint);
                output.WriteInt32(checked((int)(long)value));
                break;
            case ProtoType.Int64:
                output.WriteTag(field.Number, WireFormat.WireType.Varint);
                output.WriteInt64((long)value);
                break;
            case ProtoType.Uint32:
                output.WriteTag(field.Number, WireFormat.WireType.Varint);
                output.WriteUInt32(checked((uint)(long)value));
                break;
            case ProtoType.Uint64:
                output.WriteTag(field.Number, WireFormat.WireType.Varint);
                output.WriteUInt64(value is ulong unsigned ? unsigned : checked((ulong)(long)value));
                break;
            default:
                throw new InvalidDataException($"Unsupported wire type {field.Wire} for {field.Name}");
        }
    }
}
