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
VALUES (N'schema_migrations'), (N'routing_collectors'), (N'routing_pressure_latest'),
    (N'routing_pressure_history'), (N'routing_table_leases'), (N'routing_run_pin_sets'),
    (N'routing_run_table_initializations'), (N'routing_run_table_pins');

IF EXISTS (
    SELECT 1 FROM @required_tables expected
    LEFT JOIN sys.tables actual ON actual.[name] = expected.[name]
        AND actual.[schema_id] = SCHEMA_ID(N'arrowcol')
    WHERE actual.[object_id] IS NULL
)
    THROW 52010, 'A required routing or migration table is missing.', 1;

DECLARE @removed_tables table ([name] sysname PRIMARY KEY);
INSERT INTO @removed_tables ([name])
VALUES (N'collector_instances'), (N'collector_pressure_latest'), (N'collector_pressure_history'),
    (N'schema_change_sagas'), (N'table_schema_versions'), (N'table_schema_heads'),
    (N'catalog_tables'), (N'catalog_fields'), (N'schema_artifact_chunks'),
    (N'table_fencing_epochs'), (N'table_leases'), (N'delivery_receipts'),
    (N'receipt_events'), (N'delivery_receipts_v2'), (N'receipt_events_v2');

IF EXISTS (
    SELECT 1 FROM @removed_tables removed
    JOIN sys.tables actual ON actual.[name] = removed.[name]
        AND actual.[schema_id] = SCHEMA_ID(N'arrowcol')
)
    THROW 52019, 'Retired SQL catalog or delivery-state tables remain.', 1;

IF (SELECT COUNT_BIG(*) FROM [arrowcol].[schema_migrations]) <> 11
    THROW 52011, 'Unexpected Arrowcol migration count.', 1;
IF NOT EXISTS (SELECT 1 FROM [arrowcol].[schema_migrations] WHERE [version] = N'0011')
    THROW 52011, 'The immutable OAT authority migration is missing.', 1;
IF EXISTS (
    SELECT 1 FROM [arrowcol].[schema_migrations]
    WHERE LEN([checksum_sha256]) <> 64 OR [checksum_sha256] LIKE '%[^0-9a-f]%'
)
    THROW 52012, 'An Arrowcol migration checksum is invalid.', 1;

IF EXISTS (
    SELECT 1 FROM sys.foreign_keys fk
    JOIN sys.tables t ON t.[object_id] = fk.[parent_object_id]
    WHERE t.[schema_id] = SCHEMA_ID(N'arrowcol')
      AND (fk.[is_disabled] = 1 OR fk.[is_not_trusted] = 1)
)
    THROW 52013, 'A routing foreign key is disabled or untrusted.', 1;
IF EXISTS (
    SELECT 1 FROM sys.check_constraints ck
    JOIN sys.tables t ON t.[object_id] = ck.[parent_object_id]
    WHERE t.[schema_id] = SCHEMA_ID(N'arrowcol')
      AND (ck.[is_disabled] = 1 OR ck.[is_not_trusted] = 1)
)
    THROW 52013, 'A routing check constraint is disabled or untrusted.', 1;

DECLARE @required_constraints table ([name] sysname PRIMARY KEY);
INSERT INTO @required_constraints ([name])
VALUES (N'FK_routing_run_table_initializations_set'), (N'FK_routing_run_table_pins_initialization'),
    (N'FK_routing_run_table_pins_expiry'), (N'CK_routing_run_pin_sets_count'),
    (N'CK_routing_run_pin_sets_digest'), (N'CK_routing_run_pin_sets_expiry'),
    (N'CK_routing_run_table_initializations_pool'), (N'CK_routing_run_table_initializations_identity'),
    (N'CK_routing_run_table_initializations_state'), (N'CK_routing_run_table_pins_epoch');
IF EXISTS (
    SELECT 1 FROM @required_constraints expected
    LEFT JOIN sys.objects actual ON actual.[name] = expected.[name]
        AND actual.[schema_id] = SCHEMA_ID(N'arrowcol') AND actual.[type] IN ('F', 'C')
    WHERE actual.[object_id] IS NULL
)
    THROW 52013, 'A required OAT authority constraint is missing.', 1;

DECLARE @required_indexes table ([name] sysname PRIMARY KEY);
INSERT INTO @required_indexes ([name])
VALUES (N'IX_routing_collectors_pool_ready'), (N'IX_routing_collectors_instance_key'),
    (N'IX_routing_pressure_latest_fresh'), (N'IX_routing_pressure_history_collector'),
    (N'IX_routing_table_leases_holder'), (N'IX_routing_table_leases_expiry'),
    (N'IX_routing_run_table_pins_key_expiry'), (N'PK_routing_run_pin_sets'),
    (N'UQ_routing_run_pin_sets_expiry'), (N'PK_routing_run_table_initializations'),
    (N'PK_routing_run_table_pins');
IF EXISTS (
    SELECT 1 FROM @required_indexes expected
    LEFT JOIN sys.indexes actual ON actual.[name] = expected.[name]
        AND OBJECT_SCHEMA_NAME(actual.[object_id]) = N'arrowcol'
    WHERE actual.[index_id] IS NULL OR actual.[is_disabled] = 1
)
    THROW 52015, 'A required routing index is missing or disabled.', 1;

DECLARE @required_triggers table ([name] sysname PRIMARY KEY);
INSERT INTO @required_triggers ([name])
VALUES (N'TR_routing_run_pin_sets_immutable'), (N'TR_routing_run_table_initializations_transition'),
    (N'TR_routing_run_table_pins_bind');
IF EXISTS (
    SELECT 1 FROM @required_triggers expected
    LEFT JOIN sys.triggers actual ON actual.[name] = expected.[name]
        AND OBJECT_SCHEMA_NAME(actual.[parent_id]) = N'arrowcol'
    WHERE actual.[object_id] IS NULL OR actual.[is_disabled] = 1
)
    THROW 52020, 'An OAT authority trigger is missing or disabled.', 1;

IF EXISTS (
    SELECT 1 FROM [arrowcol].[routing_run_pin_sets] s
    WHERE s.expected_table_count <>
        (SELECT COUNT_BIG(*) FROM [arrowcol].[routing_run_table_initializations] m WHERE m.run_id = s.run_id)
)
    THROW 52021, 'An OAT run has an incomplete or enlarged expected table set.', 1;
IF OBJECT_ID(N'arrowcol.routing_run_pin_set_digest', N'FN') IS NULL
    THROW 52021, 'The OAT v1 digest function is missing.', 1;
IF EXISTS (
    SELECT 1 FROM [arrowcol].[routing_run_pin_sets] s
    WHERE s.expected_table_set_sha256 <> [arrowcol].[routing_run_pin_set_digest](s.run_id)
        OR [arrowcol].[routing_run_pin_set_digest](s.run_id) IS NULL
)
    THROW 52021, 'An OAT expected set no longer matches its immutable v1 SHA-256 digest.', 1;
IF EXISTS (
    SELECT 1 FROM [arrowcol].[routing_run_table_initializations] m
    LEFT JOIN [arrowcol].[routing_run_table_pins] p ON p.run_id = m.run_id
        AND p.implementation_pool = m.implementation_pool AND p.catalog_name = m.catalog_name
        AND p.schema_name = m.schema_name AND p.table_name = m.table_name
    WHERE (m.state = 'bound' AND p.run_id IS NULL) OR (m.state = 'pending' AND p.run_id IS NOT NULL)
)
    THROW 52022, 'OAT initialization marker and owner row disagree.', 1;
IF EXISTS (
    SELECT 1 FROM [arrowcol].[routing_run_table_pins] p
    JOIN [arrowcol].[routing_run_pin_sets] s ON s.run_id = p.run_id
    WHERE p.expires_at_utc <> s.expires_at_utc
)
    THROW 52023, 'An OAT pin has a different expiry from its immutable run set.', 1;

DECLARE @app_ok bit = 1;
DECLARE @digest_run_id uniqueidentifier = ISNULL(
    (SELECT TOP (1) run_id FROM [arrowcol].[routing_run_pin_sets] ORDER BY run_id), NEWID());
DECLARE @expected_app_digest char(64) = [arrowcol].[routing_run_pin_set_digest](@digest_run_id);
DECLARE @actual_app_digest char(64);
EXECUTE AS USER = 'arrowcol_app';
BEGIN TRY
    IF EXISTS (
        SELECT 1 FROM (VALUES (N'arrowcol.routing_run_pin_sets'),
            (N'arrowcol.routing_run_table_initializations'), (N'arrowcol.routing_run_table_pins')) authority([name])
        WHERE ISNULL(HAS_PERMS_BY_NAME(authority.[name], 'OBJECT', 'SELECT'), 0) <> 1
            OR ISNULL(HAS_PERMS_BY_NAME(authority.[name], 'OBJECT', 'INSERT'), 1) <> 0
            OR ISNULL(HAS_PERMS_BY_NAME(authority.[name], 'OBJECT', 'UPDATE'), 1) <> 0
            OR ISNULL(HAS_PERMS_BY_NAME(authority.[name], 'OBJECT', 'DELETE'), 1) <> 0
    )
        SET @app_ok = 0;
    IF ISNULL(HAS_PERMS_BY_NAME(N'arrowcol.routing_run_pin_set_digest', 'OBJECT', 'EXECUTE'), 0) <> 1
        SET @app_ok = 0;
    SET @actual_app_digest = [arrowcol].[routing_run_pin_set_digest](@digest_run_id);
    IF ISNULL(@actual_app_digest, '') <> ISNULL(@expected_app_digest, '')
        SET @app_ok = 0;
END TRY
BEGIN CATCH
    REVERT;
    THROW;
END CATCH;
REVERT;
IF @app_ok = 0
    THROW 52024, 'The app must read OAT authority and invoke its digest function, but must not mutate authority tables.', 1;

DECLARE @migrator_ok bit = 1;
EXECUTE AS USER = 'arrowcol_migrator';
IF EXISTS (
    SELECT 1 FROM (VALUES (N'arrowcol.routing_run_pin_sets'),
        (N'arrowcol.routing_run_table_initializations'), (N'arrowcol.routing_run_table_pins')) authority([name])
    WHERE ISNULL(HAS_PERMS_BY_NAME(authority.[name], 'OBJECT', 'SELECT'), 0) <> 1
        OR ISNULL(HAS_PERMS_BY_NAME(authority.[name], 'OBJECT', 'INSERT'), 0) <> 1
)
    SET @migrator_ok = 0;
REVERT;
IF @migrator_ok = 0
    THROW 52025, 'The trusted finite setup job lacks existing migrator read/write permissions.', 1;

IF EXISTS (
    SELECT 1 FROM sys.columns c
    JOIN sys.tables t ON t.[object_id] = c.[object_id]
    JOIN sys.types ty ON ty.[user_type_id] = c.[user_type_id]
    WHERE t.[schema_id] = SCHEMA_ID(N'arrowcol')
      AND (ty.[name] = N'image'
        OR (ty.[name] IN (N'varbinary', N'binary') AND c.[max_length] = -1)
        OR c.[is_filestream] = 1)
)
    THROW 52016, 'Routing SQL tables must not contain Arrow payload byte storage.', 1;

PRINT N'Arrowcol SQL Server contains routing state and immutable OAT pin authority only.';
