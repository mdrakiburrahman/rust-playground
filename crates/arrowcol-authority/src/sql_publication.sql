-- Opt-in bounded local publication gate; not the legacy catalog/receipt schema.
IF SCHEMA_ID('publication_gate') IS NULL EXEC('CREATE SCHEMA publication_gate');
IF OBJECT_ID('publication_gate.owners') IS NULL
CREATE TABLE publication_gate.owners (
    table_id varchar(100) COLLATE Latin1_General_100_BIN2 PRIMARY KEY,
    identity_json nvarchar(2000) COLLATE Latin1_General_100_BIN2 NOT NULL,
    active bit NOT NULL,
    expires datetime2 NOT NULL,
    head bigint NOT NULL DEFAULT -1,
    published bigint NOT NULL DEFAULT -1,
    CHECK (published <= head)
);
IF OBJECT_ID('publication_gate.decisions') IS NULL
CREATE TABLE publication_gate.decisions (
    table_id varchar(100) COLLATE Latin1_General_100_BIN2 NOT NULL,
    version bigint NOT NULL,
    operation_id varchar(100) COLLATE Latin1_General_100_BIN2 NOT NULL,
    digest char(64) NOT NULL,
    payload nvarchar(max) COLLATE Latin1_General_100_BIN2 NOT NULL,
    accepted_at datetime2 NOT NULL DEFAULT SYSUTCDATETIME(),
    PRIMARY KEY (table_id, version),
    UNIQUE (table_id, operation_id),
    FOREIGN KEY (table_id) REFERENCES publication_gate.owners(table_id)
);
IF OBJECT_ID('publication_gate.receipts') IS NULL
CREATE TABLE publication_gate.receipts (
    table_id varchar(100) COLLATE Latin1_General_100_BIN2 NOT NULL,
    version bigint NOT NULL,
    digest char(64) NOT NULL,
    materialized_at datetime2 NOT NULL DEFAULT SYSUTCDATETIME(),
    PRIMARY KEY (table_id, version),
    FOREIGN KEY (table_id, version) REFERENCES publication_gate.decisions(table_id, version)
);
