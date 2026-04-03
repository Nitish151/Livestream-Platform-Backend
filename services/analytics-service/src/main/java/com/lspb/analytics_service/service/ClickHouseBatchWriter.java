package com.lspb.analytics_service.service;

import com.lspb.analytics_service.model.ChatEvent;
import com.lspb.analytics_service.model.ViewerEvent;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class ClickHouseBatchWriter {

    private final JdbcTemplate jdbcTemplate;
    private final MeterRegistry meterRegistry;

    private final List<ViewerEvent> viewerEventsBuffer = new CopyOnWriteArrayList<>();
    private final List<ChatEvent> chatEventsBuffer = new CopyOnWriteArrayList<>();

    private Counter insertCounter;

    @PostConstruct
    public void init() {
        Gauge.builder("analytics_events_buffered", () -> viewerEventsBuffer.size() + chatEventsBuffer.size())
             .description("Number of events currently in the buffer")
             .register(meterRegistry);

        insertCounter = Counter.builder("analytics_clickhouse_inserts_total")
             .description("Total number of events inserted into ClickHouse")
             .register(meterRegistry);
    }

    public void addViewerEvent(ViewerEvent event) {
        viewerEventsBuffer.add(event);
    }

    public void addChatEvent(ChatEvent event) {
        chatEventsBuffer.add(event);
    }

    @Scheduled(fixedDelay = 500)
    public void flush() {
        flushViewerEvents();
        flushChatEvents();
    }

    private void flushViewerEvents() {
        if (viewerEventsBuffer.isEmpty()) {
            return;
        }

        List<ViewerEvent> batch = new ArrayList<>(viewerEventsBuffer);
        
        try {
            int[][] results = jdbcTemplate.batchUpdate(
                "INSERT INTO viewer_events (event_type, stream_id, user_id, session_id, timestamp, watch_duration, country_code, region, user_agent, rendition) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                batch,
                Math.max(batch.size(), 100),
                (ps, argument) -> {
                    ps.setString(1, argument.getEventType());
                    ps.setString(2, argument.getStreamId());
                    ps.setString(3, argument.getUserId());
                    ps.setString(4, argument.getSessionId());
                    
                    Timestamp ts = argument.getTimestamp() != null ? Timestamp.from(Instant.parse(argument.getTimestamp())) : Timestamp.from(Instant.now());
                    ps.setTimestamp(5, ts);
                    
                    ps.setObject(6, argument.getWatchDurationSeconds() != null ? argument.getWatchDurationSeconds() : 0);
                    ps.setString(7, argument.getCountryCode() != null ? argument.getCountryCode() : "XX");
                    ps.setString(8, argument.getRegion() != null ? argument.getRegion() : "Unknown");
                    ps.setString(9, argument.getUserAgent() != null ? argument.getUserAgent() : "Unknown");
                    ps.setString(10, argument.getRendition() != null ? argument.getRendition() : "Unknown");
                }
            );

            int insertedCount = 0;
            for (int[] batchResult : results) {
                for (int r : batchResult) {
                    if (r > 0) insertedCount += r;
                }
            }
            
            insertCounter.increment(insertedCount);
            viewerEventsBuffer.removeAll(batch);
            log.debug("Successfully flushed {} ViewerEvents to ClickHouse", batch.size());

        } catch (Exception e) {
            log.error("Failed to insert ViewerEvents into ClickHouse! Retaining {} events in buffer. Reason: {}", batch.size(), e.getMessage());
        }
    }

    private void flushChatEvents() {
        if (chatEventsBuffer.isEmpty()) {
            return;
        }

        List<ChatEvent> batch = new ArrayList<>(chatEventsBuffer);
        
        try {
            int[][] results = jdbcTemplate.batchUpdate(
                "INSERT INTO chat_analytics (event_type, stream_id, user_id, timestamp, country_code) " +
                "VALUES (?, ?, ?, ?, ?)",
                batch,
                Math.max(batch.size(), 100),
                (ps, argument) -> {
                    ps.setString(1, argument.getEventType());
                    ps.setString(2, argument.getStreamId());
                    ps.setString(3, argument.getUserId());
                    
                    Timestamp ts = argument.getTimestamp() != null ? Timestamp.from(Instant.parse(argument.getTimestamp())) : Timestamp.from(Instant.now());
                    ps.setTimestamp(4, ts);
                    
                    ps.setString(5, argument.getCountryCode() != null ? argument.getCountryCode() : "XX");
                }
            );

            int insertedCount = 0;
            for (int[] batchResult : results) {
                for (int r : batchResult) {
                    if (r > 0) insertedCount += r;
                }
            }
            
            insertCounter.increment(insertedCount);
            chatEventsBuffer.removeAll(batch);
            log.debug("Successfully flushed {} ChatEvents to ClickHouse", batch.size());

        } catch (Exception e) {
            log.error("Failed to insert ChatEvents into ClickHouse! Retaining {} events in buffer. Reason: {}", batch.size(), e.getMessage());
        }
    }
}
