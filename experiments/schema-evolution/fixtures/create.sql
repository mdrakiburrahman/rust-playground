CREATE TABLE schema_evolution.public.foo (
    client STRING NOT NULL,
    phase STRING NOT NULL,
    value BIGINT NOT NULL,
    tenant STRING NOT NULL
)
USING DELTA
PARTITIONED BY (tenant)
TBLPROPERTIES (
    'arrowcol.maintenance.enabled' = 'false',
    'delta.columnMapping.mode' = 'none',
    'delta.enableDeletionVectors' = 'false'
)
