CREATE TABLE demo.public.events (
  event_id BIGINT NOT NULL,
  observed_at TIMESTAMP NOT NULL,
  payload STRING
) USING DELTA
