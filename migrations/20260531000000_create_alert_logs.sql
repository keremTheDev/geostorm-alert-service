CREATE TABLE IF NOT EXISTS alert_logs (
    id BIGSERIAL PRIMARY KEY,
    activity_id TEXT NOT NULL,
    alert_level TEXT NOT NULL,
    details TEXT NOT NULL,
    event_timestamp TIMESTAMPTZ NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
