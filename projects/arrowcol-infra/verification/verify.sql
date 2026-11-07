:on error exit

SET NOCOUNT ON;
SET XACT_ABORT ON;
SET ANSI_NULLS ON;
SET ANSI_PADDING ON;
SET ANSI_WARNINGS ON;
SET ARITHABORT ON;
SET CONCAT_NULL_YIELDS_NULL ON;
SET QUOTED_IDENTIFIER ON;
SET NUMERIC_ROUNDABORT OFF;

IF DB_ID(N'arrowcol') IS NULL
    THROW 52000, 'arrowcol database does not exist.', 1;

IF SUSER_ID(N'arrowcol_app') IS NULL
    THROW 52004, 'arrowcol_app login is missing.', 1;

IF SUSER_ID(N'arrowcol_migrator') IS NULL
    THROW 52005, 'arrowcol_migrator login is missing.', 1;

USE [arrowcol];

DECLARE @required_tables table ([name] sysname PRIMARY KEY);
INSERT INTO @required_tables ([name])
VALUES
    (N'schema_migrations'),
    (N'routing_collectors'),
    (N'routing_pressure_latest'),
    (N'routing_pressure_history'),
    (N'routing_table_leases');

IF EXISTS (
    SELECT 1
    FROM @required_tables AS expected
    LEFT JOIN sys.tables AS actual
        ON actual.[name] = expected.[name]
       AND actual.[schema_id] = SCHEMA_ID(N'arrowcol')
    WHERE actual.[object_id] IS NULL
)
    THROW 52010, 'A required routing or migration table is missing.', 1;

DECLARE @removed_tables table ([name] sysname PRIMARY KEY);
INSERT INTO @removed_tables ([name])
VALUES
    (N'collector_instances'),
    (N'collector_pressure_latest'),
    (N'collector_pressure_history'),
    (N'schema_change_sagas'),
    (N'table_schema_versions'),
    (N'table_schema_heads'),
    (N'catalog_tables'),
    (N'catalog_fields'),
    (N'schema_artifact_chunks'),
    (N'table_fencing_epochs'),
    (N'table_leases'),
    (N'delivery_receipts'),
    (N'receipt_events'),
    (N'delivery_receipts_v2'),
    (N'receipt_events_v2');

IF EXISTS (
    SELECT 1
    FROM @removed_tables AS removed
    INNER JOIN sys.tables AS actual
        ON actual.[name] = removed.[name]
       AND actual.[schema_id] = SCHEMA_ID(N'arrowcol')
)
    THROW 52019, 'Retired SQL catalog or delivery-state tables remain.', 1;

IF (SELECT COUNT_BIG(*) FROM [arrowcol].[schema_migrations]) <> 10
    THROW 52011, 'Unexpected Arrowcol migration count.', 1;

IF EXISTS (
    SELECT 1
    FROM [arrowcol].[schema_migrations]
    WHERE LEN([checksum_sha256]) <> 64
       OR [checksum_sha256] LIKE '%[^0-9a-f]%'
)
    THROW 52012, 'An Arrowcol migration checksum is invalid.', 1;

IF EXISTS (
    SELECT 1
    FROM sys.foreign_keys AS foreign_key
    INNER JOIN sys.tables AS tables
        ON tables.[object_id] = foreign_key.[parent_object_id]
    WHERE tables.[schema_id] = SCHEMA_ID(N'arrowcol')
      AND (foreign_key.[is_disabled] = 1 OR foreign_key.[is_not_trusted] = 1)
)
    THROW 52013, 'A routing foreign key is disabled or untrusted.', 1;

DECLARE @required_indexes table ([name] sysname PRIMARY KEY);
INSERT INTO @required_indexes ([name])
VALUES
    (N'IX_routing_collectors_pool_ready'),
    (N'IX_routing_collectors_instance_key'),
    (N'IX_routing_pressure_latest_fresh'),
    (N'IX_routing_pressure_history_collector'),
    (N'IX_routing_table_leases_holder'),
    (N'IX_routing_table_leases_expiry');

IF EXISTS (
    SELECT 1
    FROM @required_indexes AS expected
    LEFT JOIN sys.indexes AS actual
        ON actual.[name] = expected.[name]
       AND OBJECT_SCHEMA_NAME(actual.[object_id]) = N'arrowcol'
    WHERE actual.[index_id] IS NULL OR actual.[is_disabled] = 1
)
    THROW 52015, 'A required routing index is missing or disabled.', 1;

IF EXISTS (
    SELECT 1
    FROM sys.columns AS columns
    INNER JOIN sys.tables AS tables
        ON tables.[object_id] = columns.[object_id]
    INNER JOIN sys.types AS types
        ON types.[user_type_id] = columns.[user_type_id]
    WHERE tables.[schema_id] = SCHEMA_ID(N'arrowcol')
      AND (types.[name] = N'image'
        OR (types.[name] IN (N'varbinary', N'binary') AND columns.[max_length] = -1)
        OR columns.[is_filestream] = 1)
)
    THROW 52016, 'Routing SQL tables must not contain Arrow payload byte storage.', 1;

PRINT N'Arrowcol SQL Server contains routing state only.';