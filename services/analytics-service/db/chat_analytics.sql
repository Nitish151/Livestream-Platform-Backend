CREATE TABLE IF NOT EXISTS chat_analytics (
    event_type      LowCardinality(String),
    stream_id       UUID,
    user_id         UUID,
    timestamp       DateTime64(3, 'UTC'),
    country_code    LowCardinality(FixedString(2))
) ENGINE = MergeTree()
ORDER BY (stream_id, timestamp);
