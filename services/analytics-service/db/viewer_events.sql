CREATE TABLE IF NOT EXISTS viewer_events ( 
    event_type      LowCardinality(String), 
    stream_id       UUID, 
    user_id         UUID, 
    session_id      UUID, 
    timestamp       DateTime64(3, 'UTC'), 
    watch_duration  UInt32, 
    country_code    LowCardinality(FixedString(2)), 
    region          LowCardinality(String), 
    user_agent      String, 
    rendition       LowCardinality(String) 
) ENGINE = MergeTree() 
ORDER BY (stream_id, timestamp) 
PARTITION BY toYYYYMM(timestamp) 
TTL toDateTime(timestamp) + INTERVAL 90 DAY;
