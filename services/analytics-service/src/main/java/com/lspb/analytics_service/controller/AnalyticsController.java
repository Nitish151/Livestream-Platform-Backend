package com.lspb.analytics_service.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@Slf4j
@RestController
@RequestMapping("/v1/analytics")
@RequiredArgsConstructor
public class AnalyticsController {

    private final JdbcTemplate jdbcTemplate;

    @GetMapping("/{streamId}/summary")
    public SummaryResponse getSummary(@PathVariable("streamId") String streamId) {
        log.info("Fetching summary for stream {}", streamId);
        
        long totalUniqueViewers = 0;
        long totalWatchSeconds = 0;
        long peakConcurrentViewers = 0;

        try {
            String totalQuery = "SELECT uniq(user_id) as total_viewers, sum(watch_duration) as total_watch_seconds FROM viewer_events WHERE stream_id = ?";
            var totalRow = jdbcTemplate.queryForMap(totalQuery, streamId);
            totalUniqueViewers = totalRow.get("total_viewers") != null ? ((Number) totalRow.get("total_viewers")).longValue() : 0L;
            totalWatchSeconds = totalRow.get("total_watch_seconds") != null ? ((Number) totalRow.get("total_watch_seconds")).longValue() : 0L;
        } catch (org.springframework.dao.EmptyResultDataAccessException e) {
            log.debug("No viewer events found for stream {}", streamId);
        }

        try {
            String peakQuery = "SELECT max(val) as peak_concurrent FROM (SELECT uniqMerge(unique_viewers) as val FROM peak_viewers_mv WHERE stream_id = ?)";
            var peakRow = jdbcTemplate.queryForMap(peakQuery, streamId);
            peakConcurrentViewers = peakRow.get("peak_concurrent") != null ? ((Number) peakRow.get("peak_concurrent")).longValue() : 0L;
        } catch (org.springframework.dao.EmptyResultDataAccessException e) {
            log.debug("No peak views found for stream {}", streamId);
        }
        
        return new SummaryResponse(totalUniqueViewers, peakConcurrentViewers, totalWatchSeconds);
    }

    @GetMapping("/{streamId}/viewers/timeseries")
    public List<TimeseriesData> getTimeseries(@PathVariable("streamId") String streamId) {
        log.info("Fetching timeseries for stream {}", streamId);
        String query = "SELECT toStartOfMinute(timestamp) AS minute_ts, uniq(user_id) as viewers FROM viewer_events WHERE stream_id = ? GROUP BY minute_ts ORDER BY minute_ts";
        
        return jdbcTemplate.query(query, (rs, rowNum) -> new TimeseriesData(
                rs.getString("minute_ts"),
                rs.getLong("viewers")
        ), streamId);
    }

    @GetMapping("/{streamId}/geography")
    public List<GeographyData> getGeography(@PathVariable("streamId") String streamId) {
        log.info("Fetching geography for stream {}", streamId);
        String query = "SELECT country_code, count() as viewers FROM viewer_events WHERE stream_id = ? GROUP BY country_code ORDER BY viewers DESC LIMIT 20";
        
        return jdbcTemplate.query(query, (rs, rowNum) -> new GeographyData(
                rs.getString("country_code"),
                rs.getLong("viewers")
        ), streamId);
    }

    // DTO records
    public record SummaryResponse(long totalUniqueViewers, long peakConcurrentViewers, long totalWatchSeconds) {}
    public record TimeseriesData(String minute, long viewers) {}
    public record GeographyData(String countryCode, long viewers) {}
}
