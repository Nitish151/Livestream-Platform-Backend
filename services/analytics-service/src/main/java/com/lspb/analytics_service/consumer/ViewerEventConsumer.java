package com.lspb.analytics_service.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.lspb.analytics_service.model.ViewerEvent;
import com.lspb.analytics_service.service.GeoIpService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import com.lspb.analytics_service.service.ClickHouseBatchWriter;

@Slf4j
@Service
@RequiredArgsConstructor
public class ViewerEventConsumer {

    private final ObjectMapper objectMapper;
    private final GeoIpService geoIpService;
    private final ClickHouseBatchWriter clickHouseBatchWriter;

    @KafkaListener(topics = "viewer-events", groupId = "analytics-group")
    public void consume(String message) {
        try {
            ViewerEvent event = objectMapper.readValue(message, ViewerEvent.class);            
            log.debug("Received ViewerEvent: {}", event.getEventType());

            if ((isBlank(event.getCountryCode()) || isBlank(event.getRegion())) && !isBlank(event.getIp())) {
                GeoIpService.GeoData geoData = geoIpService.getGeoData(event.getIp());
                if (isBlank(event.getCountryCode()) && !isBlank(geoData.countryCode())) {
                    event.setCountryCode(geoData.countryCode());
                }
                if (isBlank(event.getRegion()) && !isBlank(geoData.region())) {
                    event.setRegion(geoData.region());
                }
            }

            log.info("Enriched ViewerEvent: streamId={}, userId={}, countryCode={}, region={}",
                    event.getStreamId(), event.getUserId(), event.getCountryCode(), event.getRegion());
            
            clickHouseBatchWriter.addViewerEvent(event);

        } catch (JsonProcessingException e) {
            log.error("Failed to deserialize ViewerEvent message: {}", message, e);
        } catch (Exception e) {
            log.error("Error processing ViewerEvent", e);
        }
    }

    private boolean isBlank(String value) {
        return value == null || value.isBlank();
    }
}
