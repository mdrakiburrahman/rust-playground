using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Arrowcol.Dmv;

/// <summary>A view column as the engine reports it in <c>sys.all_columns</c>.</summary>
internal sealed record SqlColumn(string Name, string Type, int MaxLength);

/// <summary>A view's columns in engine order, looked up case-insensitively.</summary>
internal sealed class ViewColumns
{
    private readonly Dictionary<string, SqlColumn> byName = new(StringComparer.OrdinalIgnoreCase);
    public IReadOnlyList<string> Keys { get; }
    public int Count => Keys.Count;

    public ViewColumns(IEnumerable<SqlColumn> columns)
    {
        foreach (var column in columns) byName.TryAdd(column.Name, column);
        Keys = byName.Values.Select(column => column.Name).ToList();
    }

    public bool TryGetValue(string name, out SqlColumn column) => byName.TryGetValue(name, out column!);
}

internal static class DmvSql
{
    private const string Probe = """
        SELECT c.name, COALESCE(t.name, N''), c.max_length
        FROM sys.all_columns AS c
        LEFT JOIN sys.types AS t ON t.user_type_id = c.system_type_id
        WHERE c.object_id = OBJECT_ID(@source)
        ORDER BY c.column_id
        """;

    private static readonly ConcurrentDictionary<string, ViewColumns> Probes = new(StringComparer.OrdinalIgnoreCase);

    /// <summary>Returns the view's columns, or none when this engine or edition does not expose it.</summary>
    public static async Task<ViewColumns> ProbeAsync(DbConnection connection, string source, CancellationToken cancellationToken)
    {
        await OpenAsync(connection, cancellationToken).ConfigureAwait(false);
        var key = $"{connection.DataSource}|{connection.Database}|{source}";
        if (Probes.TryGetValue(key, out var cached)) return cached;
        await using var command = connection.CreateCommand();
        command.CommandText = Probe;
        Add(command, "@source", DbType.String, source);
        var columns = new List<SqlColumn>();
        await using (var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false))
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                columns.Add(new SqlColumn(reader.GetString(0), reader.GetString(1).ToLowerInvariant(), Convert.ToInt32(reader.GetValue(2), CultureInfo.InvariantCulture)));
        var result = new ViewColumns(columns);
        if (Probes.Count > 4096) Probes.Clear();
        return Probes[key] = result;
    }

    /// <summary>
    /// Maps an engine type to a catalog type. Binary, XML, variant, spatial and CLR columns are never collected;
    /// MAX text columns only when the caller listed them.
    /// </summary>
    public static string? CatalogType(SqlColumn column, bool listed) => column.Type switch
    {
        "bit" => "BOOLEAN",
        "tinyint" or "smallint" => "SMALLINT",
        "int" => "INT",
        "bigint" => "BIGINT",
        "real" => "FLOAT",
        "float" => "DOUBLE",
        "decimal" or "numeric" or "money" or "smallmoney" => "DECIMAL(38,12)",
        "date" => "DATE",
        "datetime" or "datetime2" or "smalldatetime" or "datetimeoffset" => "TIMESTAMP",
        "char" or "varchar" or "nchar" or "nvarchar" when column.MaxLength != -1 || listed => "STRING",
        "uniqueidentifier" or "time" => "STRING",
        _ => null,
    };

    /// <summary>Columns filled by the writer rather than projected from the view.</summary>
    public static bool IsWriterColumn(string name, DmvWriteOptions options) =>
        name is DmvCatalog.CustomerId or DmvCatalog.SampleTime || DmvCatalog.IdentityColumns.Contains(name) ||
        options.Constants.ContainsKey(name);

    /// <summary>
    /// Builds the bounded SELECT for a view. Columns this engine lacks project as NULL so every edition writes the
    /// same table; <c>TOP</c>, the watermark and the order come only from the validated dataset.
    /// </summary>
    public static string Select(DmvDataset dataset, ViewColumns view, DmvWriteOptions options, bool hasWatermark)
    {
        var sql = new StringBuilder("SELECT ");
        if (dataset.Top is { } top) sql.Append("TOP (").Append(top).Append(") ");
        sql.Append("SYSUTCDATETIME() AS [").Append(DmvCatalog.SampleTime).Append(']');
        foreach (var column in dataset.Columns.Where(column => !IsWriterColumn(column.Name, options)))
        {
            sql.Append(", ");
            if (view.TryGetValue(column.Source ?? column.Name, out var source)) sql.Append(Quote(source.Name));
            else sql.Append("NULL");
            sql.Append(" AS [").Append(column.Name).Append(']');
        }
        var parts = dataset.Source!.Split('.');
        sql.Append(" FROM ").Append(Quote(parts[0])).Append('.').Append(Quote(parts[1]));
        if (dataset.Watermark is { } watermark)
        {
            var source = SourceOf(dataset, view, watermark);
            if (hasWatermark) sql.Append(" WHERE ").Append(source).Append(" > @watermark");
            sql.Append(" ORDER BY ").Append(source).Append(" DESC");
        }
        else if (dataset.OrderBy is not null)
        {
            sql.Append(" ORDER BY ").Append(string.Join(", ", dataset.OrderBy.Split(", ").Select(term =>
            {
                var pieces = term.Split(' ');
                return SourceOf(dataset, view, pieces[0]) + (pieces.Length > 1 ? " " + pieces[1].ToUpperInvariant() : "");
            })));
        }
        return sql.ToString();
    }

    private static string SourceOf(DmvDataset dataset, ViewColumns view, string name)
    {
        var column = dataset.Columns.First(column => column.Name == name);
        return view.TryGetValue(column.Source ?? column.Name, out var source)
            ? Quote(source.Name)
            : throw new InvalidOperationException($"{dataset.Id}: ordering column {name} is missing from {dataset.Source}");
    }

    public static DbType ParameterType(string catalogType) => catalogType switch
    {
        "TIMESTAMP" => DbType.DateTime2,
        "BIGINT" => DbType.Int64,
        "INT" => DbType.Int32,
        _ => throw new InvalidOperationException($"Unsupported watermark type {catalogType}"),
    };

    public static void Add(DbCommand command, string name, DbType type, object value)
    {
        var parameter = command.CreateParameter();
        parameter.ParameterName = name;
        parameter.DbType = type;
        parameter.Value = value;
        command.Parameters.Add(parameter);
    }

    public static async Task OpenAsync(DbConnection connection, CancellationToken cancellationToken)
    {
        if (connection.State != ConnectionState.Open) await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
    }

    private static string Quote(string name) => "[" + name.Replace("]", "]]", StringComparison.Ordinal) + "]";
}
