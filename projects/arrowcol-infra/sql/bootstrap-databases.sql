:on error exit

SET NOCOUNT ON;
SET XACT_ABORT ON;

IF DB_ID(N'arrowcol') IS NULL
BEGIN
    PRINT N'Creating arrowcol database';
    CREATE DATABASE [arrowcol];
    ALTER DATABASE [arrowcol] SET RECOVERY SIMPLE;
    ALTER DATABASE [arrowcol] SET READ_COMMITTED_SNAPSHOT ON;
END;

GO

IF SUSER_ID(N'arrowcol_app') IS NULL
    CREATE LOGIN [arrowcol_app]
        WITH PASSWORD = N'$(ARROWCOL_APP_PASSWORD_SQL)',
        CHECK_POLICY = ON,
        CHECK_EXPIRATION = OFF;
ELSE
    ALTER LOGIN [arrowcol_app]
        WITH PASSWORD = N'$(ARROWCOL_APP_PASSWORD_SQL)',
        CHECK_POLICY = ON,
        CHECK_EXPIRATION = OFF;

IF SUSER_ID(N'arrowcol_migrator') IS NULL
    CREATE LOGIN [arrowcol_migrator]
        WITH PASSWORD = N'$(ARROWCOL_MIGRATOR_PASSWORD_SQL)',
        CHECK_POLICY = ON,
        CHECK_EXPIRATION = OFF;
ELSE
    ALTER LOGIN [arrowcol_migrator]
        WITH PASSWORD = N'$(ARROWCOL_MIGRATOR_PASSWORD_SQL)',
        CHECK_POLICY = ON,
        CHECK_EXPIRATION = OFF;

GO

USE [arrowcol];
GO

IF USER_ID(N'arrowcol_app') IS NULL
    CREATE USER [arrowcol_app] FOR LOGIN [arrowcol_app] WITH DEFAULT_SCHEMA = [dbo];

IF USER_ID(N'arrowcol_migrator') IS NULL
    CREATE USER [arrowcol_migrator] FOR LOGIN [arrowcol_migrator] WITH DEFAULT_SCHEMA = [dbo];

IF NOT EXISTS (
    SELECT 1
    FROM sys.database_role_members AS drm
    INNER JOIN sys.database_principals AS role_principal
        ON role_principal.principal_id = drm.role_principal_id
    INNER JOIN sys.database_principals AS member_principal
        ON member_principal.principal_id = drm.member_principal_id
    WHERE role_principal.name = N'db_datareader'
      AND member_principal.name = N'arrowcol_app'
)
    ALTER ROLE [db_datareader] ADD MEMBER [arrowcol_app];

IF NOT EXISTS (
    SELECT 1
    FROM sys.database_role_members AS drm
    INNER JOIN sys.database_principals AS role_principal
        ON role_principal.principal_id = drm.role_principal_id
    INNER JOIN sys.database_principals AS member_principal
        ON member_principal.principal_id = drm.member_principal_id
    WHERE role_principal.name = N'db_datawriter'
      AND member_principal.name = N'arrowcol_app'
)
    ALTER ROLE [db_datawriter] ADD MEMBER [arrowcol_app];

IF NOT EXISTS (
    SELECT 1
    FROM sys.database_role_members AS drm
    INNER JOIN sys.database_principals AS role_principal
        ON role_principal.principal_id = drm.role_principal_id
    INNER JOIN sys.database_principals AS member_principal
        ON member_principal.principal_id = drm.member_principal_id
    WHERE role_principal.name = N'db_owner'
      AND member_principal.name = N'arrowcol_migrator'
)
    ALTER ROLE [db_owner] ADD MEMBER [arrowcol_migrator];
GO
