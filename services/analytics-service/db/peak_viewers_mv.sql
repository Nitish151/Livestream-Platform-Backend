CREATE MATERIALIZED VIEW IF NOT EXISTS peak_viewers_mv 
ENGINE = AggregatingMergeTree() 
ORDER BY (stream_id, minute) 
AS SELECT stream_id, 
    toStartOfMinute(timestamp) AS minute, 
    uniqState(user_id)         AS unique_viewers 
FROM viewer_events WHERE event_type = 'STREAM_JOIN' 
GROUP BY stream_id, minute;
