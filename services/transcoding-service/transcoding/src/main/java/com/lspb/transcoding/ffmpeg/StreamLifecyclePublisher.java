package com.lspb.transcoding.ffmpeg;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
public class StreamLifecyclePublisher {

    private static final String STREAM_LIFECYCLE_TOPIC = "stream-lifecycle";

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public void publishTranscodeStarted(String streamId) throws Exception {
        Map<String, Object> event = Map.of(
            "eventType", "TRANSCODE_STARTED",
            "streamId", streamId,
            "timestamp", Instant.now().toString()
        );

        String payload = objectMapper.writeValueAsString(event);
        kafkaTemplate.send(STREAM_LIFECYCLE_TOPIC, streamId, payload);
        log.info("Published TRANSCODE_STARTED for streamId={} on topic={}", streamId, STREAM_LIFECYCLE_TOPIC);
    }
}
