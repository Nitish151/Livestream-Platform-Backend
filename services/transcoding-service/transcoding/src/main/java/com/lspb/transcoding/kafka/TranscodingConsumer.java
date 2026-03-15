package com.lspb.transcoding.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.lspb.transcoding.ffmpeg.FfmpegRunner;
import com.lspb.transcoding.ffmpeg.SegmentUploader;
import com.lspb.transcoding.model.TranscodingJob;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.BackOff;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

@Slf4j
@Component
@RequiredArgsConstructor
public class TranscodingConsumer {

    private static final String TRANSCODING_JOBS_LISTENER_ID = "transcoding-jobs-listener";
    private static final String STREAM_LIFECYCLE_LISTENER_ID = "stream-lifecycle-listener";

    private final ObjectMapper objectMapper;
    private final MeterRegistry meterRegistry;
    private final FfmpegRunner ffmpegRunner;
    private final SegmentUploader segmentUploader;
    private final KafkaListenerEndpointRegistry kafkaRegistry;

    @Value("${transcoding.concurrency:4}")
    private int transcodingConcurrency;

    private final Map<String, ActiveJob> activeJobs = new ConcurrentHashMap<>();
    private final AtomicBoolean jobsListenerPaused = new AtomicBoolean(false);

    @RetryableTopic(attempts = "3", backOff = @BackOff(delay = 1000, multiplier = 2.0))
    @KafkaListener(id = TRANSCODING_JOBS_LISTENER_ID, topics = "transcoding-jobs", groupId = "transcoding-service")
    public void consumeTranscodingJob(
            String payload,
            @Header(value = KafkaHeaders.RECEIVED_KEY, required = false) String messageKey
    ) {
        try {
            TranscodingJob job = objectMapper.readValue(payload, TranscodingJob.class);

            if (job.getStreamId() == null || job.getStreamId().isBlank()) {
                throw new IllegalArgumentException("Transcoding job streamId is missing");
            }
            if (job.getRtmpUrl() == null || job.getRtmpUrl().isBlank()) {
                throw new IllegalArgumentException("Transcoding job rtmpUrl is missing");
            }

            if (activeJobs.containsKey(job.getStreamId())) {
                log.info("Ignoring duplicate transcoding job for streamId={}", job.getStreamId());
                return;
            }

            Path outputBase = Paths.get("/tmp", job.getStreamId());
            Process ffmpegProcess = ffmpegRunner.startTranscoding(job.getRtmpUrl(), job.getStreamId(), outputBase);
            segmentUploader.watchStream(job.getStreamId(), outputBase);

            ActiveJob activeJob = new ActiveJob(job.getStreamId(), outputBase, ffmpegProcess);
            activeJobs.put(job.getStreamId(), activeJob);

            monitorJobCompletion(activeJob);
            applyConcurrencyState();

            log.info(
                "Started transcoding job: jobId={}, streamId={}, key={}, activeJobs={}",
                    job.getJobId(),
                    job.getStreamId(),
                    messageKey,
                    activeJobs.size()
            );
        } catch (Exception ex) {
            throw new RuntimeException("Failed to parse transcoding job payload", ex);
        }
    }

    @KafkaListener(id = STREAM_LIFECYCLE_LISTENER_ID, topics = "stream-lifecycle", groupId = "transcoding-service")
    public void consumeStreamLifecycle(
        String payload,
        @Header(value = KafkaHeaders.RECEIVED_KEY, required = false) String messageKey
    ) {
        try {
            com.fasterxml.jackson.databind.JsonNode tree = objectMapper.readTree(payload);
            String eventType = tree.path("eventType").asText("");
            String streamId = tree.path("streamId").asText("");

            if (!"STREAM_ENDED".equals(eventType)) {
                return;
            }

            if (streamId.isBlank()) {
                log.warn("STREAM_ENDED received without streamId. key={}, payload={}", messageKey, payload);
                return;
            }

            stopAndCleanup(streamId, "stream-ended-event");
            log.info("Handled STREAM_ENDED for streamId={} key={}", streamId, messageKey);
        } catch (Exception ex) {
            throw new RuntimeException("Failed to process stream lifecycle event", ex);
        }
    }

    private void monitorJobCompletion(ActiveJob job) {
        Thread monitor = new Thread(() -> {
            try {
                job.process.waitFor();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            stopAndCleanup(job.streamId, "ffmpeg-process-exit");
        }, "transcoding-job-monitor-" + job.streamId);
        monitor.setDaemon(true);
        monitor.start();
    }

    private void stopAndCleanup(String streamId, String reason) {
        ActiveJob removed = activeJobs.remove(streamId);
        if (removed == null) {
            return;
        }

        segmentUploader.stopWatching(streamId);
        ffmpegRunner.stopTranscoding(streamId);
        deleteOutputDirectory(removed.outputBase);

        meterRegistry.counter("transcoding.jobs.completed.total", "reason", reason).increment();
        applyConcurrencyState();
        log.info("Cleaned up transcoding job streamId={} reason={} activeJobs={}", streamId, reason, activeJobs.size());
    }

    private void applyConcurrencyState() {
        if (activeJobs.size() >= transcodingConcurrency) {
            pauseJobsListener();
        } else {
            resumeJobsListener();
        }
    }

    private void pauseJobsListener() {
        if (!jobsListenerPaused.compareAndSet(false, true)) {
            return;
        }

        MessageListenerContainer container = kafkaRegistry.getListenerContainer(TRANSCODING_JOBS_LISTENER_ID);
        if (container != null && container.isRunning()) {
            container.pause();
            log.warn("Paused {} due to concurrency limit: activeJobs={}, limit={}",
                TRANSCODING_JOBS_LISTENER_ID,
                activeJobs.size(),
                transcodingConcurrency);
        }
    }

    private void resumeJobsListener() {
        if (!jobsListenerPaused.compareAndSet(true, false)) {
            return;
        }

        MessageListenerContainer container = kafkaRegistry.getListenerContainer(TRANSCODING_JOBS_LISTENER_ID);
        if (container != null && container.isRunning()) {
            container.resume();
            log.info("Resumed {} activeJobs={}, limit={}",
                TRANSCODING_JOBS_LISTENER_ID,
                activeJobs.size(),
                transcodingConcurrency);
        }
    }

    private void deleteOutputDirectory(Path outputBase) {
        if (outputBase == null || !Files.exists(outputBase)) {
            return;
        }

        try (Stream<Path> walk = Files.walk(outputBase)) {
            walk.sorted(Comparator.reverseOrder()).forEach(path -> {
                try {
                    Files.deleteIfExists(path);
                } catch (IOException e) {
                    log.warn("Failed to delete {}", path, e);
                }
            });
        } catch (IOException e) {
            log.warn("Failed to cleanup outputBase={}", outputBase, e);
        }
    }

    private static final class ActiveJob {
        private final String streamId;
        private final Path outputBase;
        private final Process process;

        private ActiveJob(String streamId, Path outputBase, Process process) {
            this.streamId = streamId;
            this.outputBase = outputBase;
            this.process = process;
        }
    }

    @DltHandler
    public void handleDlt(
        String payload,
        @Header(value = KafkaHeaders.RECEIVED_KEY, required = false) String messageKey,
        @Header(value = KafkaHeaders.RECEIVED_TOPIC, required = false) String topic
    ) {
        meterRegistry.counter("transcoding.jobs.dlt.total").increment();
        System.err.println(
            "[DLT] Received failed transcoding job payload on topic=" + topic + ", key=" + messageKey
                + ", payload=" + payload
        );
    }
}
