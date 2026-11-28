using System.Text.Json.Nodes;
using Arrowcol.Dmv;
using Xunit;

namespace Arrowcol.Sdk.Tests;

public sealed class DmvCatalogTests
{
    [Fact]
    public void PackagedDdlIsTheDdlTheRuntimeWrites()
    {
        var files = Directory.GetFiles(Path.Combine(AppContext.BaseDirectory, "arrow-ddl"), "*.sql")
            .ToDictionary(path => Path.GetFileNameWithoutExtension(path), StringComparer.Ordinal);
        var contracts = TestCatalog.Packaged.Datasets.ToDictionary(dataset => Path.GetFileName(dataset.ContractPath!), StringComparer.Ordinal);

        Assert.Equal(files.Keys.Order(StringComparer.Ordinal), contracts.Keys.Order(StringComparer.Ordinal));
        Assert.All(contracts, pair => Assert.Equal(File.ReadAllText(files[pair.Key]), pair.Value.Ddl));
    }

    [Fact]
    public void CompiledContractsMatchPackagedContracts()
    {
        var temp = Directory.CreateTempSubdirectory("arrowcol-dmv-");
        try
        {
            var json = JsonNode.Parse(File.ReadAllText(Path.Combine(AppContext.BaseDirectory, DmvCatalog.FileName)))!;
            foreach (var entry in json["datasets"]!.AsArray()) entry!.AsObject().Remove("contract");
            var path = Path.Combine(temp.FullName, DmvCatalog.FileName);
            File.WriteAllText(path, json.ToJsonString());

            var compiled = DmvCatalog.Load(path).CompileMissingContracts(Path.Combine(temp.FullName, "contracts"));

            Assert.Equal(TestCatalog.Packaged.Datasets.Count, compiled.Datasets.Count);
            Assert.All(compiled.Datasets, dataset =>
            {
                var packaged = TestCatalog.Packaged.Get(dataset.Id).ContractPath!;
                var names = Directory.GetFiles(packaged).Select(Path.GetFileName).Order(StringComparer.Ordinal).ToList();
                Assert.Equal(names, Directory.GetFiles(dataset.ContractPath!).Select(Path.GetFileName).Order(StringComparer.Ordinal));
                foreach (var name in names)
                    Assert.True(File.ReadAllBytes(Path.Combine(packaged, name!)).AsSpan().SequenceEqual(
                        File.ReadAllBytes(Path.Combine(dataset.ContractPath!, name!))), $"{dataset.Id}/{name} differs");
            });
        }
        finally
        {
            temp.Delete(recursive: true);
        }
    }

    [Fact]
    public void CatalogRejectsUnsafeOrAmbiguousDatasets()
    {
        var latch = TestCatalog.Packaged.Get("dmv.dm_os_latch_stats");
        DmvDataset[] invalid =
        [
            latch with { Dataset = "dm_os_latch_stats_copy" },
            latch with { Query = "SELECT TOP (10) * FROM sys.dm_os_latch_stats", Top = 10 },
            latch with { Source = "sys.dm_os_latch_stats; DROP TABLE t" },
            latch with { OrderBy = "wait_time_ms DESC; --" },
            latch with { Watermark = "latch_class" },
            latch with { Columns = latch.Columns.Where(column => column.Name != DmvCatalog.CustomerId).ToList() },
            latch with { Columns = [.. latch.Columns, new DmvColumn("payload", "BINARY")] },
        ];

        Assert.All(invalid, dataset => Assert.Throws<InvalidDataException>(() => TestCatalog.Packaged.With([dataset])));
    }
}
