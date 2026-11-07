:on error exit
:setvar MigrationVersion "0010"
:setvar MigrationDescription "remove SQL catalog and Direct delivery state"

SET NOCOUNT ON;
SET XACT_ABORT ON;
SET ANSI_NULLS ON;
SET ANSI_PADDING ON;
SET ANSI_WARNINGS ON;
SET ARITHABORT ON;
SET CONCAT_NULL_YIELDS_NULL ON;
SET QUOTED_IDENTIFIER ON;
SET NUMERIC_ROUNDABORT OFF;

BEGIN TRY
    BEGIN TRANSACTION;

    DECLARE @lock_result int;
    EXEC @lock_result = sys.sp_getapplock
        @Resource = N'arrowcol.schema_migrations',
        @LockMode = N'Exclusive',
        @LockOwner = N'Transaction',
        @LockTimeout = 60000,
        @DbPrincipal = N'public';

    IF @lock_result < 0
        THROW 51000, 'Could not acquire the Arrowcol migration lock.', 1;

    DECLARE @existing_checksum char(64);
    SELECT @existing_checksum = [checksum_sha256]
    FROM [arrowcol].[schema_migrations] WITH (UPDLOCK, HOLDLOCK)
    WHERE [version] = N'$(MigrationVersion)';

    IF @existing_checksum IS NOT NULL
    BEGIN
        IF @existing_checksum <> '$(MIGRATION_CHECKSUM)'
            THROW 51010, 'Migration 0010 checksum does not match the recorded version.', 1;

        COMMIT TRANSACTION;
        PRINT N'Migration 0010 already applied';
        RETURN;
    END;

    IF OBJECT_ID(N'arrowcol.receipt_events', N'U') IS NOT NULL DROP TABLE [arrowcol].[receipt_events];
    IF OBJECT_ID(N'arrowcol.receipt_events_v2', N'U') IS NOT NULL DROP TABLE [arrowcol].[receipt_events_v2];
    IF OBJECT_ID(N'arrowcol.delivery_receipts', N'U') IS NOT NULL DROP TABLE [arrowcol].[delivery_receipts];
    IF OBJECT_ID(N'arrowcol.delivery_receipts_v2', N'U') IS NOT NULL DROP TABLE [arrowcol].[delivery_receipts_v2];
    IF OBJECT_ID(N'arrowcol.table_leases', N'U') IS NOT NULL DROP TABLE [arrowcol].[table_leases];
    IF OBJECT_ID(N'arrowcol.table_fencing_epochs', N'U') IS NOT NULL DROP TABLE [arrowcol].[table_fencing_epochs];
    IF OBJECT_ID(N'arrowcol.collector_pressure_latest', N'U') IS NOT NULL DROP TABLE [arrowcol].[collector_pressure_latest];
    IF OBJECT_ID(N'arrowcol.collector_pressure_history', N'U') IS NOT NULL DROP TABLE [arrowcol].[collector_pressure_history];
    IF OBJECT_ID(N'arrowcol.table_schema_heads', N'U') IS NOT NULL DROP TABLE [arrowcol].[table_schema_heads];
    IF OBJECT_ID(N'arrowcol.catalog_fields', N'U') IS NOT NULL DROP TABLE [arrowcol].[catalog_fields];
    IF OBJECT_ID(N'arrowcol.schema_artifact_chunks', N'U') IS NOT NULL DROP TABLE [arrowcol].[schema_artifact_chunks];
    IF OBJECT_ID(N'arrowcol.table_schema_versions', N'U') IS NOT NULL DROP TABLE [arrowcol].[table_schema_versions];
    IF OBJECT_ID(N'arrowcol.schema_change_sagas', N'U') IS NOT NULL DROP TABLE [arrowcol].[schema_change_sagas];
    IF OBJECT_ID(N'arrowcol.catalog_tables', N'U') IS NOT NULL DROP TABLE [arrowcol].[catalog_tables];
    IF OBJECT_ID(N'arrowcol.collector_instances', N'U') IS NOT NULL DROP TABLE [arrowcol].[collector_instances];

    INSERT INTO [arrowcol].[schema_migrations] ([version], [description], [checksum_sha256])
    VALUES (N'$(MigrationVersion)', N'$(MigrationDescription)', '$(MIGRATION_CHECKSUM)');

    COMMIT TRANSACTION;
    PRINT N'Applied migration 0010';
END TRY
BEGIN CATCH
    IF XACT_STATE() <> 0
        ROLLBACK TRANSACTION;
    THROW;
END CATCH;
GO