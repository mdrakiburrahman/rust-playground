CREATE TABLE telemetry.prod.sdk_events (
    event_id BIGINT NOT NULL,
    observed_at TIMESTAMP NOT NULL,
    trace_id BINARY NOT NULL,
    payload BINARY,
    amount DECIMAL(20,4),
    samples ARRAY<STRUCT<name: STRING, value: DOUBLE>>,
    attributes MAP<STRING, STRING>,
    event_date DATE NOT NULL,
    tenant STRING NOT NULL
)
USING DELTA
PARTITIONED BY (tenant)
