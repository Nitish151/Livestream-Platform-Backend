package com.lspb.analytics_service.metrics;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
@Service
@RequiredArgsConstructor
public class KafkaLagMetricsService {

    private static final String GROUP_ID = "analytics-group";

    private final MeterRegistry meterRegistry;

    @Value("${kafka.bootstrap-servers:localhost:9092}")
    private String bootstrapServers;

    private final Map<String, AtomicLong> lagGaugeValues = new ConcurrentHashMap<>();
    private AdminClient adminClient;

    @PostConstruct
    public void init() {
        Map<String, Object> config = new HashMap<>();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        adminClient = AdminClient.create(config);

        for (String topic : trackedTopics()) {
            ensureLagGauge(GROUP_ID, topic).set(0);
        }
    }

    @PreDestroy
    public void close() {
        if (adminClient != null) {
            adminClient.close();
        }
    }

    @Scheduled(fixedDelay = 15000)
    public void refreshLagMetrics() {
        for (String topic : trackedTopics()) {
            try {
                long lag = computeLagForTopic(GROUP_ID, topic);
                ensureLagGauge(GROUP_ID, topic).set(lag);
            } catch (Exception ex) {
                log.debug("Failed to refresh Kafka lag metric for topic={} group={}: {}", topic, GROUP_ID, ex.getMessage());
            }
        }
    }

    private List<String> trackedTopics() {
        return List.of("viewer-events", "chat-events", "analytics-events");
    }

    private long computeLagForTopic(String groupId, String topic) throws Exception {
        Map<TopicPartition, OffsetAndMetadata> committedOffsets = adminClient
            .listConsumerGroupOffsets(groupId)
            .partitionsToOffsetAndMetadata()
            .get(5, TimeUnit.SECONDS);

        Map<TopicPartition, OffsetAndMetadata> topicOffsets = new HashMap<>();
        for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : committedOffsets.entrySet()) {
            if (topic.equals(entry.getKey().topic())) {
                topicOffsets.put(entry.getKey(), entry.getValue());
            }
        }

        if (topicOffsets.isEmpty()) {
            return 0;
        }

        Map<TopicPartition, OffsetSpec> latestOffsetSpec = new HashMap<>();
        for (TopicPartition topicPartition : topicOffsets.keySet()) {
            latestOffsetSpec.put(topicPartition, OffsetSpec.latest());
        }

        ListOffsetsResult latestOffsets = adminClient.listOffsets(latestOffsetSpec);

        long lag = 0;
        for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : topicOffsets.entrySet()) {
            TopicPartition topicPartition = entry.getKey();
            long committed = entry.getValue().offset();
            long latest = latestOffsets.partitionResult(topicPartition).get(5, TimeUnit.SECONDS).offset();
            lag += Math.max(0, latest - committed);
        }

        return lag;
    }

    private AtomicLong ensureLagGauge(String groupId, String topic) {
        String key = groupId + ":" + topic;
        return lagGaugeValues.computeIfAbsent(key, ignored -> {
            AtomicLong value = new AtomicLong(0);
            Gauge.builder("kafka_consumer_lag", value::get)
                .description("Kafka consumer lag by topic and consumer group")
                .tag("topic", topic)
                .tag("group", groupId)
                .register(meterRegistry);
            return value;
        });
    }
}
