-- Version 1. Opt-in bounded runtime namespace, independent of legacy and gate.
IF SCHEMA_ID('delta_authority') IS NULL EXEC('CREATE SCHEMA delta_authority');
IF OBJECT_ID('delta_authority.state') IS NULL
BEGIN
 CREATE TABLE delta_authority.state(id int PRIMARY KEY CHECK(id=1), payload nvarchar(max) NOT NULL);
 INSERT delta_authority.state VALUES(1,N'{"revision":0,"controller":null,"controller_expires":0,"controller_lease":0,"members":{},"retired":[],"tables":{}}');
END;
IF OBJECT_ID('delta_authority.receipts') IS NULL
CREATE TABLE delta_authority.receipts(
 table_id varchar(100) COLLATE Latin1_General_100_BIN2 NOT NULL,
 operation_id varchar(100) COLLATE Latin1_General_100_BIN2 NOT NULL,
 version bigint NOT NULL, digest char(64) NOT NULL,
 completed_at datetime2 NOT NULL DEFAULT SYSUTCDATETIME(),
 PRIMARY KEY(table_id,operation_id), UNIQUE(table_id,version)
);
