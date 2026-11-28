using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;

namespace Arrowcol.Dmv;

/// <summary>One table column. <paramref name="Source"/> names the view column when it differs.</summary>
public sealed record DmvColumn(string Name, string Type, bool Nullable = true, string? Source = null);

/// <summary>A collectable view or a schema-only table, with its machine-readable contract.</summary>
public sealed record DmvDataset
{
    public required string Family { get; init; }
    public required string Dataset { get; init; }
    public string Id => $"{Family}.{Dataset}";
    public required string Table { get; init; }
    /// <summary>Two-part view name, such as <c>sys.dm_os_wait_stats</c>; null for schema-only tables.</summary>
    public string? Source { get; init; }
    /// <summary>Verbatim bounded or aggregated SELECT that replaces the generated projection.</summary>
    public string? Query { get; init; }
    public int? Top { get; init; }
    public string? OrderBy { get; init; }
    public string? Watermark { get; init; }
    public TimeSpan Interval { get; init; } = TimeSpan.FromMinutes(5);
    /// <summary>Query errors are logged and skipped, for views that some editions cannot serve.</summary>
    public bool Optional { get; init; }
    public IReadOnlyList<string> Targets { get; init; } = [];
    public IReadOnlyList<string> Provenance { get; init; } = [];
    public required IReadOnlyList<DmvColumn> Columns { get; init; }
    /// <summary>Absolute contract directory; null until packaged or compiled.</summary>
    public string? ContractPath { get; init; }
    public bool Collectable => Query is not null || Source is not null;

    public string Ddl => $"CREATE TABLE {Table} (\n    {string.Join(",\n    ", Columns.Select(column =>
        $"{column.Name} {column.Type}{(column.Nullable ? "" : " NOT NULL")}"))}\n) USING DELTA;\n";

    private static readonly Regex Identifier = new("^[a-z_][a-z0-9_]*$", RegexOptions.CultureInvariant);
    private static readonly Regex TypeName = new(
        @"^(?:STRING|BOOLEAN|SMALLINT|INT|BIGINT|FLOAT|DOUBLE|DATE|TIMESTAMP|DECIMAL\((\d{1,2}),(\d{1,2})\))$", RegexOptions.CultureInvariant);
    private static readonly Regex ObjectName = new("^[A-Za-z_][A-Za-z0-9_]*$", RegexOptions.CultureInvariant);
    private static readonly Regex OrderTerm = new("^([a-z_][a-z0-9_]*)(?: (ASC|DESC))?$", RegexOptions.CultureInvariant | RegexOptions.IgnoreCase);

    internal static bool IsIdentifier(string name) => Identifier.IsMatch(name);

    internal void Validate()
    {
        var parts = Table.Split('.');
        if (parts.Length != 3 || !parts.All(IsIdentifier)) throw Invalid("table must be catalog.schema.table in lower snake case");
        if (!IsIdentifier(Family) || !IsIdentifier(Dataset)) throw Invalid("family and dataset must be lower snake case");
        if (Source is not null && (Source.Split('.') is not [var schema, var view] || !ObjectName.IsMatch(schema) || !ObjectName.IsMatch(view)))
            throw Invalid("source must be a two-part object name");
        if (Top is <= 0) throw Invalid("top must be positive");
        if (Query is not null && (Top is not null || Watermark is not null || OrderBy is not null))
            throw Invalid("a verbatim query carries its own bounds; top, watermark and orderBy do not apply");
        if (Interval <= TimeSpan.Zero) throw Invalid("interval must be positive");
        var names = new HashSet<string>(StringComparer.Ordinal);
        foreach (var column in Columns)
        {
            if (!IsIdentifier(column.Name) || !names.Add(column.Name)) throw Invalid($"column {column.Name} is invalid or duplicated");
            var type = TypeName.Match(column.Type);
            if (!type.Success) throw Invalid($"column {column.Name} has unsupported type {column.Type}");
            if (type.Groups[1].Success && (int.Parse(type.Groups[1].Value, CultureInfo.InvariantCulture) is var precision &&
                (precision is < 1 or > 38 || int.Parse(type.Groups[2].Value, CultureInfo.InvariantCulture) > precision)))
                throw Invalid($"column {column.Name} has an invalid decimal precision or scale");
            if (column.Source is not null && !ObjectName.IsMatch(column.Source)) throw Invalid($"column {column.Name} has an invalid source");
        }
        if (Columns.FirstOrDefault(column => column.Name == DmvCatalog.CustomerId) is not { Type: "STRING", Nullable: false })
            throw Invalid("customer_id STRING NOT NULL is required");
        if (Watermark is not null && (!names.Contains(Watermark) ||
            Columns.First(column => column.Name == Watermark).Type is not ("TIMESTAMP" or "BIGINT" or "INT")))
            throw Invalid("watermark must be a TIMESTAMP, BIGINT or INT column");
        if (OrderBy is not null && (Watermark is not null || OrderBy.Split(", ").Any(term =>
            OrderTerm.Match(term) is not { Success: true } match || !names.Contains(match.Groups[1].Value))))
            throw Invalid("orderBy must list table columns with optional ASC or DESC and cannot combine with watermark");
        if (Collectable && !names.Contains(DmvCatalog.SampleTime)) throw Invalid("collectable datasets need sample_time_utc");
    }

    private InvalidDataException Invalid(string message) => new($"DMV dataset {Family}.{Dataset}: {message}");
}

/// <summary>Bounds a runtime-described view. Projections are explicit; binary, XML and unlisted MAX columns are omitted.</summary>
public sealed record DmvViewSpec(string View)
{
    public int? Top { get; init; } = 1000;
    public string? OrderBy { get; init; }
    public string? Watermark { get; init; }
    public TimeSpan Interval { get; init; } = TimeSpan.FromMinutes(5);
    public IReadOnlyList<string> Exclude { get; init; } = [];
    /// <summary>When set, only these view columns, in this order. MAX columns are collected only when listed here.</summary>
    public IReadOnlyList<string>? Columns { get; init; }
}

/// <summary>The packaged DMV catalog plus any caller-supplied or runtime-described datasets.</summary>
public sealed class DmvCatalog
{
    public const string FileName = "arrowcol-dmv-catalog.json";
    public const string CustomerId = "customer_id";
    public const string SampleTime = "sample_time_utc";
    public static IReadOnlyList<string> IdentityColumns { get; } =
        ["source_server_name", "source_database_name", "source_resource_id", "source_tenant_id"];

    private readonly Dictionary<string, DmvDataset> byId;
    public IReadOnlyList<DmvDataset> Datasets { get; }

    private DmvCatalog(IReadOnlyList<DmvDataset> datasets)
    {
        byId = new(StringComparer.Ordinal);
        var tables = new HashSet<string>(StringComparer.Ordinal);
        foreach (var dataset in datasets)
        {
            dataset.Validate();
            if (!byId.TryAdd(dataset.Id, dataset) || !tables.Add(dataset.Table))
                throw new InvalidDataException($"Duplicate DMV dataset {dataset.Id} or table {dataset.Table}");
        }
        Datasets = datasets;
    }

    /// <summary>Loads <c>arrowcol-dmv-catalog.json</c>, which the package copies to the application output.</summary>
    public static DmvCatalog LoadPackaged(string? baseDirectory = null) =>
        Load(Path.Combine(baseDirectory ?? AppContext.BaseDirectory, FileName));

    public static DmvCatalog Load(string path)
    {
        var root = Path.GetDirectoryName(Path.GetFullPath(path))!;
        var file = JsonSerializer.Deserialize<CatalogFile>(File.ReadAllBytes(path), Json)
            ?? throw new InvalidDataException("Empty DMV catalog");
        if (file.Version != 1) throw new InvalidDataException($"Unsupported DMV catalog version {file.Version}");
        return new(file.Datasets.Select(entry => new DmvDataset
        {
            Family = entry.Family,
            Dataset = entry.Dataset,
            Table = entry.Table,
            Source = entry.Source,
            Query = entry.Query,
            Top = entry.Top,
            OrderBy = entry.OrderBy,
            Watermark = entry.Watermark,
            Interval = TimeSpan.ParseExact(entry.Interval, "c", CultureInfo.InvariantCulture),
            Optional = entry.Optional,
            Targets = entry.Targets,
            Provenance = entry.Provenance,
            Columns = entry.Columns,
            ContractPath = entry.Contract is null ? null : Path.GetFullPath(Path.Combine(root, entry.Contract)),
        }).ToList());
    }

    public IEnumerable<DmvDataset> ForTarget(string target) =>
        Datasets.Where(dataset => dataset.Targets.Contains(target, StringComparer.OrdinalIgnoreCase));

    public DmvDataset Get(string id) =>
        byId.TryGetValue(id, out var dataset) ? dataset : throw new KeyNotFoundException($"Unknown DMV dataset {id}");

    /// <summary>Returns a catalog that also contains <paramref name="datasets"/>. Ids and tables must stay unique.</summary>
    public DmvCatalog With(IEnumerable<DmvDataset> datasets) => new([.. Datasets, .. datasets]);

    /// <summary>
    /// Compiles a contract for each dataset without one into <paramref name="directory"/>/&lt;id&gt;. Contracts are
    /// machine-local and rebuilt on every call so they always match the dataset.
    /// </summary>
    public DmvCatalog CompileMissingContracts(string directory) => new(Datasets.Select(dataset =>
    {
        if (dataset.ContractPath is not null) return dataset;
        var path = Path.GetFullPath(Path.Combine(directory, dataset.Id));
        if (Directory.Exists(path)) Directory.Delete(path, recursive: true);
        Arrowcol.Client.SharedNative.Compile(dataset.Ddl, path);
        return dataset with { ContractPath = path };
    }).ToList());

    /// <summary>
    /// Describes views from the connected engine's metadata as <paramref name="family"/>.&lt;view&gt; datasets.
    /// Absent views are omitted. Results stay on this machine unless the caller persists them.
    /// </summary>
    public static async Task<IReadOnlyList<DmvDataset>> DescribeAsync(
        DbConnection connection, string family, string target, IEnumerable<DmvViewSpec> views,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(connection);
        var described = new List<DmvDataset>();
        foreach (var spec in views)
        {
            if (!DmvDataset.IsIdentifier(spec.View)) throw new ArgumentException($"Invalid view name {spec.View}", nameof(views));
            var metadata = await DmvSql.ProbeAsync(connection, $"sys.{spec.View}", cancellationToken).ConfigureAwait(false);
            if (metadata.Count == 0) continue;
            var reserved = new HashSet<string>([CustomerId, SampleTime, .. IdentityColumns], StringComparer.Ordinal);
            var selected = spec.Columns ?? metadata.Keys.Where(name => !spec.Exclude.Contains(name, StringComparer.OrdinalIgnoreCase));
            var columns = new List<DmvColumn>();
            foreach (var requested in selected)
            {
                if (!metadata.TryGetValue(requested, out var column)) continue;
                var name = column.Name.ToLowerInvariant();
                var type = DmvSql.CatalogType(column, listed: spec.Columns is not null);
                if (type is null || !DmvDataset.IsIdentifier(name) || !reserved.Add(name)) continue;
                columns.Add(new DmvColumn(name, type, Source: name == column.Name ? null : column.Name));
            }
            var dataset = new DmvDataset
            {
                Family = family,
                Dataset = spec.View,
                Table = $"telemetry.{family}.{spec.View}",
                Source = $"sys.{spec.View}",
                Top = spec.Top,
                OrderBy = spec.OrderBy,
                Watermark = spec.Watermark,
                Interval = spec.Interval,
                Targets = [target],
                Provenance = ["runtime"],
                Columns = [.. KeyColumns(), .. columns],
            };
            dataset.Validate();
            described.Add(dataset);
        }
        return described;
    }

    /// <summary>The leading columns of every per-view table: customer, sample time and source identity.</summary>
    public static IReadOnlyList<DmvColumn> KeyColumns() =>
        [new(CustomerId, "STRING", Nullable: false), new(SampleTime, "TIMESTAMP"),
         .. IdentityColumns.Select(name => new DmvColumn(name, "STRING"))];

    internal static readonly JsonSerializerOptions Json = new()
    {
        PropertyNameCaseInsensitive = true,
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
    };

    private sealed record CatalogFile(int Version, IReadOnlyList<CatalogEntry> Datasets);

    private sealed record CatalogEntry(
        string Family, string Dataset, string Table, string? Source, string? Query, int? Top, string? OrderBy,
        string? Watermark, string Interval, bool Optional, IReadOnlyList<string> Targets, IReadOnlyList<string> Provenance,
        IReadOnlyList<DmvColumn> Columns, string? Contract);
}
