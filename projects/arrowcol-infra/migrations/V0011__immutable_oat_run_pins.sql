:on error exit
:setvar MigrationVersion "0011"
:setvar MigrationDescription "immutable OAT run pin authority"

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
            THROW 51011, 'Migration 0011 checksum does not match the recorded version.', 1;
        COMMIT TRANSACTION;
        PRINT N'Migration 0011 already applied';
        RETURN;
    END;

    IF NOT EXISTS (SELECT 1 FROM [arrowcol].[schema_migrations] WHERE [version] = N'0010')
        THROW 51011, 'Migration 0011 requires migration 0010.', 1;
    IF USER_ID(N'arrowcol_app') IS NULL OR USER_ID(N'arrowcol_migrator') IS NULL
        THROW 51011, 'OAT authority requires the existing app and migrator users.', 1;

    CREATE TABLE [arrowcol].[routing_run_pin_sets] (
        [run_id] uniqueidentifier NOT NULL,
        [expected_table_count] int NOT NULL,
        [expected_table_set_sha256] char(64) COLLATE Latin1_General_100_BIN2 NOT NULL,
        [expires_at_utc] datetime2(7) NOT NULL,
        [created_at_utc] datetime2(7) NOT NULL
            CONSTRAINT [DF_routing_run_pin_sets_created] DEFAULT SYSUTCDATETIME(),
        CONSTRAINT [PK_routing_run_pin_sets] PRIMARY KEY CLUSTERED ([run_id]),
        CONSTRAINT [UQ_routing_run_pin_sets_expiry] UNIQUE ([run_id], [expires_at_utc]),
        CONSTRAINT [CK_routing_run_pin_sets_count] CHECK ([expected_table_count] > 0),
        CONSTRAINT [CK_routing_run_pin_sets_digest] CHECK (
            LEN([expected_table_set_sha256]) = 64
            AND [expected_table_set_sha256] NOT LIKE '%[^0-9a-f]%'
        ),
        CONSTRAINT [CK_routing_run_pin_sets_expiry] CHECK ([expires_at_utc] > [created_at_utc])
    );

    CREATE TABLE [arrowcol].[routing_run_table_initializations] (
        [run_id] uniqueidentifier NOT NULL,
        [implementation_pool] varchar(16) NOT NULL,
        [catalog_name] nvarchar(128) NOT NULL,
        [schema_name] nvarchar(128) NOT NULL,
        [table_name] nvarchar(128) NOT NULL,
        [state] varchar(8) NOT NULL
            CONSTRAINT [DF_routing_run_table_initializations_state] DEFAULT 'pending',
        CONSTRAINT [PK_routing_run_table_initializations] PRIMARY KEY CLUSTERED (
            [run_id], [implementation_pool], [catalog_name], [schema_name], [table_name]
        ),
        CONSTRAINT [FK_routing_run_table_initializations_set] FOREIGN KEY ([run_id])
            REFERENCES [arrowcol].[routing_run_pin_sets] ([run_id]),
        CONSTRAINT [CK_routing_run_table_initializations_pool]
            CHECK ([implementation_pool] IN ('direct', 'dataflow')),
        CONSTRAINT [CK_routing_run_table_initializations_identity] CHECK (
            LEN([catalog_name]) BETWEEN 1 AND 128
            AND LEN([schema_name]) BETWEEN 1 AND 128
            AND LEN([table_name]) BETWEEN 1 AND 128
        ),
        CONSTRAINT [CK_routing_run_table_initializations_state] CHECK ([state] IN ('pending', 'bound'))
    );

    CREATE TABLE [arrowcol].[routing_run_table_pins] (
        [run_id] uniqueidentifier NOT NULL,
        [implementation_pool] varchar(16) NOT NULL,
        [catalog_name] nvarchar(128) NOT NULL,
        [schema_name] nvarchar(128) NOT NULL,
        [table_name] nvarchar(128) NOT NULL,
        [holder_instance_id] uniqueidentifier NOT NULL,
        [lease_id] uniqueidentifier NOT NULL,
        [fencing_epoch] bigint NOT NULL,
        [expires_at_utc] datetime2(7) NOT NULL,
        CONSTRAINT [PK_routing_run_table_pins] PRIMARY KEY CLUSTERED (
            [run_id], [implementation_pool], [catalog_name], [schema_name], [table_name]
        ),
        CONSTRAINT [FK_routing_run_table_pins_initialization] FOREIGN KEY (
            [run_id], [implementation_pool], [catalog_name], [schema_name], [table_name]
        ) REFERENCES [arrowcol].[routing_run_table_initializations] (
            [run_id], [implementation_pool], [catalog_name], [schema_name], [table_name]
        ),
        CONSTRAINT [FK_routing_run_table_pins_expiry] FOREIGN KEY ([run_id], [expires_at_utc])
            REFERENCES [arrowcol].[routing_run_pin_sets] ([run_id], [expires_at_utc]),
        CONSTRAINT [CK_routing_run_table_pins_epoch] CHECK ([fencing_epoch] > 0)
    );

    -- Lease rows rotate after expiry; historical pins must not reference their mutable identity.
    CREATE INDEX [IX_routing_run_table_pins_key_expiry]
        ON [arrowcol].[routing_run_table_pins] (
            [implementation_pool], [catalog_name], [schema_name], [table_name], [expires_at_utc]
        ) INCLUDE ([run_id], [holder_instance_id], [lease_id], [fencing_epoch]);

    -- Hex UTF-8 fields make separators unambiguous and match the Rust v1 preimage exactly.
    EXEC(N'CREATE FUNCTION [arrowcol].[routing_run_pin_set_digest] (@run_id uniqueidentifier)
        RETURNS char(64) AS
        BEGIN
            DECLARE @rows varchar(max);
            SELECT @rows = STRING_AGG(
                CONVERT(varchar(max), e.pool_hex + '':'' + e.catalog_hex + '':'' + e.schema_hex + '':'' + e.table_hex),
                CHAR(10)
            ) WITHIN GROUP (ORDER BY e.pool_hex COLLATE Latin1_General_100_BIN2,
                e.catalog_hex COLLATE Latin1_General_100_BIN2,
                e.schema_hex COLLATE Latin1_General_100_BIN2,
                e.table_hex COLLATE Latin1_General_100_BIN2)
            FROM arrowcol.routing_run_table_initializations m
            CROSS APPLY (SELECT
                LOWER(CONVERT(varchar(max), CONVERT(varbinary(max),
                    CONVERT(varchar(max), CONVERT(nvarchar(max), m.implementation_pool) COLLATE Latin1_General_100_BIN2_UTF8)), 2)),
                LOWER(CONVERT(varchar(max), CONVERT(varbinary(max),
                    CONVERT(varchar(max), m.catalog_name COLLATE Latin1_General_100_BIN2_UTF8)), 2)),
                LOWER(CONVERT(varchar(max), CONVERT(varbinary(max),
                    CONVERT(varchar(max), m.schema_name COLLATE Latin1_General_100_BIN2_UTF8)), 2)),
                LOWER(CONVERT(varchar(max), CONVERT(varbinary(max),
                    CONVERT(varchar(max), m.table_name COLLATE Latin1_General_100_BIN2_UTF8)), 2))
            ) e(pool_hex, catalog_hex, schema_hex, table_hex)
            WHERE m.run_id = @run_id;
            RETURN LOWER(CONVERT(char(64), HASHBYTES(''SHA2_256'', CONVERT(varbinary(max),
                CONVERT(varchar(max), ''arrowcol-oat-pin-set-v1'') + CHAR(10) + @rows + CHAR(10))), 2));
        END;');

    EXEC(N'CREATE TRIGGER [arrowcol].[TR_routing_run_pin_sets_immutable]
        ON [arrowcol].[routing_run_pin_sets] AFTER UPDATE, DELETE AS
        BEGIN
            SET NOCOUNT ON;
            IF EXISTS (SELECT 1 FROM deleted)
                THROW 51101, ''OAT run pin sets are immutable tombstones.'', 1;
        END;');

    EXEC(N'CREATE TRIGGER [arrowcol].[TR_routing_run_table_initializations_transition]
        ON [arrowcol].[routing_run_table_initializations] AFTER INSERT, UPDATE, DELETE AS
        BEGIN
            SET NOCOUNT ON;
            IF EXISTS (SELECT 1 FROM deleted)
            BEGIN
                IF UPDATE(run_id) OR UPDATE(implementation_pool) OR UPDATE(catalog_name)
                    OR UPDATE(schema_name) OR UPDATE(table_name)
                    THROW 51102, ''OAT expected table keys cannot change.'', 1;
                IF EXISTS (
                    SELECT 1 FROM deleted d
                    LEFT JOIN inserted i ON i.run_id = d.run_id
                        AND i.implementation_pool = d.implementation_pool
                        AND i.catalog_name = d.catalog_name AND i.schema_name = d.schema_name
                        AND i.table_name = d.table_name
                    WHERE i.run_id IS NULL OR d.state <> ''pending'' OR i.state <> ''bound''
                )
                    THROW 51102, ''OAT markers only transition from pending to bound; deletion is forbidden.'', 1;
                IF EXISTS (
                    SELECT 1 FROM inserted i
                    LEFT JOIN arrowcol.routing_run_table_pins p ON p.run_id = i.run_id
                        AND p.implementation_pool = i.implementation_pool
                        AND p.catalog_name = i.catalog_name AND p.schema_name = i.schema_name
                        AND p.table_name = i.table_name
                    WHERE p.run_id IS NULL
                )
                    THROW 51102, ''A bound OAT marker requires its immutable owner row.'', 1;
            END
            ELSE
            BEGIN
                IF EXISTS (SELECT 1 FROM inserted WHERE state <> ''pending'')
                    THROW 51102, ''New OAT markers must be pending.'', 1;
                IF EXISTS (
                    SELECT 1 FROM arrowcol.routing_run_pin_sets s WITH (UPDLOCK, HOLDLOCK)
                    WHERE s.run_id IN (SELECT run_id FROM inserted)
                      AND (s.expires_at_utc <= SYSUTCDATETIME()
                          OR s.expected_table_set_sha256 <> arrowcol.routing_run_pin_set_digest(s.run_id)
                          OR s.expected_table_count <>
                          (SELECT COUNT_BIG(*) FROM arrowcol.routing_run_table_initializations m
                           WHERE m.run_id = s.run_id))
                )
                    THROW 51102, ''Insert the complete OAT expected set with the exact v1 digest in one statement.'', 1;
            END;
        END;');

    EXEC(N'CREATE TRIGGER [arrowcol].[TR_routing_run_table_pins_bind]
        ON [arrowcol].[routing_run_table_pins] AFTER INSERT, UPDATE, DELETE AS
        BEGIN
            SET NOCOUNT ON;
            IF EXISTS (SELECT 1 FROM deleted)
                THROW 51103, ''OAT owner rows are immutable, including after expiry.'', 1;
            IF EXISTS (
                SELECT 1 FROM inserted i
                JOIN arrowcol.routing_run_table_initializations m WITH (UPDLOCK, HOLDLOCK)
                    ON m.run_id = i.run_id AND m.implementation_pool = i.implementation_pool
                    AND m.catalog_name = i.catalog_name AND m.schema_name = i.schema_name
                    AND m.table_name = i.table_name
                LEFT JOIN arrowcol.routing_table_leases l WITH (UPDLOCK, HOLDLOCK)
                    ON l.implementation_pool = i.implementation_pool
                    AND l.catalog_name = i.catalog_name AND l.schema_name = i.schema_name
                    AND l.table_name = i.table_name
                WHERE m.state <> ''pending'' OR i.expires_at_utc <= SYSUTCDATETIME()
                    OR l.lease_id IS NULL OR l.state <> ''active''
                    OR l.holder_instance_id <> i.holder_instance_id OR l.lease_id <> i.lease_id
                    OR l.fencing_epoch <> i.fencing_epoch OR l.expires_at_utc <= SYSUTCDATETIME()
            )
                THROW 51103, ''OAT initialization requires a pending key and its SQL-returned live lease.'', 1;
            UPDATE m SET state = ''bound''
            FROM arrowcol.routing_run_table_initializations m
            JOIN inserted i ON i.run_id = m.run_id AND i.implementation_pool = m.implementation_pool
                AND i.catalog_name = m.catalog_name AND i.schema_name = m.schema_name
                AND i.table_name = m.table_name;
        END;');

    -- db_datawriter is inherited by the app; explicit object DENYs take precedence.
    DENY INSERT, UPDATE, DELETE ON OBJECT::[arrowcol].[routing_run_pin_sets] TO [arrowcol_app];
    DENY INSERT, UPDATE, DELETE ON OBJECT::[arrowcol].[routing_run_table_initializations] TO [arrowcol_app];
    DENY INSERT, UPDATE, DELETE ON OBJECT::[arrowcol].[routing_run_table_pins] TO [arrowcol_app];
    GRANT SELECT ON OBJECT::[arrowcol].[routing_run_pin_sets] TO [arrowcol_app];
    GRANT SELECT ON OBJECT::[arrowcol].[routing_run_table_initializations] TO [arrowcol_app];
    GRANT SELECT ON OBJECT::[arrowcol].[routing_run_table_pins] TO [arrowcol_app];
    GRANT EXECUTE ON OBJECT::[arrowcol].[routing_run_pin_set_digest] TO [arrowcol_app];

    INSERT INTO [arrowcol].[schema_migrations] ([version], [description], [checksum_sha256])
    VALUES (N'$(MigrationVersion)', N'$(MigrationDescription)', '$(MIGRATION_CHECKSUM)');

    COMMIT TRANSACTION;
    PRINT N'Applied migration 0011';
END TRY
BEGIN CATCH
    IF XACT_STATE() <> 0
        ROLLBACK TRANSACTION;
    THROW;
END CATCH;
GO
