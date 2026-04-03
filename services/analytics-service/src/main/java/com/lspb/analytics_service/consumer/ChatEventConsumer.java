package com.lspb.analytics_service.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.lspb.analytics_service.model.ChatEvent;
import com.lspb.analytics_service.service.GeoIpService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import com.lspb.analytics_service.service.ClickHouseBatchWriter;

@Slf4j
@Service
@RequiredArgsConstructor
public class ChatEventConsumer {

    private final ObjectMapper objectMapper;
    private final GeoIpService geoIpService;
    private final ClickHouseBatchWriter clickHouseBatchWriter;

    @KafkaListener(topics = "chat-events", groupId = "analytics-group")
    public void consume(String message) {
        try {
            ChatEvent event = objectMapper.readValue(message, ChatEvent.class);
            log.debug("Received ChatEvent: {}", event.getEventType());

            if (event.getIp() != null) {
                GeoIpService.GeoData geoData = geoIpService.getGeoData(event.getIp());
                event.setCountryCode(geoData.countryCode());
            }

            log.info("Processed ChatEvent: streamId={}, username={}, countryCode={}",
                    event.getStreamId(), event.getUsername(), event.getCountryCode());
            
            clickHouseBatchWriter.addChatEvent(event);

        } catch (JsonProcessingException e) {
            log.error("Failed to deserialize ChatEvent message: {}", message, e);
        } catch (Exception e) {
            log.error("Error processing ChatEvent", e);
        }
    }
}
