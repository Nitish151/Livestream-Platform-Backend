$viewer_events = @"
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
TTL toDate(timestamp) + INTERVAL 90 DAY;
"@

$peak_viewers_mv = @"
CREATE MATERIALIZED VIEW IF NOT EXISTS peak_viewers_mv 
ENGINE = AggregatingMergeTree() 
ORDER BY (stream_id, minute) 
AS SELECT stream_id, 
    toStartOfMinute(timestamp) AS minute, 
    uniqState(user_id)         AS unique_viewers 
FROM viewer_events WHERE event_type = 'STREAM_JOIN' 
GROUP BY stream_id, minute;
"@

$chat_analytics = @"
CREATE TABLE IF NOT EXISTS chat_analytics (
    event_type      LowCardinality(String),
    stream_id       UUID,
    user_id         UUID,
    timestamp       DateTime64(3, 'UTC'),
    country_code    LowCardinality(FixedString(2))
) ENGINE = MergeTree()
ORDER BY (stream_id, timestamp);
"@

$clickhouseAuth = @('-u', 'default:devpass')

Write-Host "Checking ClickHouse connection..."
curl.exe -s @clickhouseAuth http://localhost:8123

Write-Host "`nCreating viewer_events table..."
curl.exe -s @clickhouseAuth -X POST http://localhost:8123 -d $viewer_events

Write-Host "`nCreating peak_viewers_mv view..."
curl.exe -s @clickhouseAuth -X POST http://localhost:8123 -d $peak_viewers_mv

Write-Host "`nCreating chat_analytics table..."
curl.exe -s @clickhouseAuth -X POST http://localhost:8123 -d $chat_analytics

Write-Host "`nShowing tables..."
curl.exe -s @clickhouseAuth "http://localhost:8123/?query=SHOW+TABLES"
