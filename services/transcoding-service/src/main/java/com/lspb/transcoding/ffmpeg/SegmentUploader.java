package com.lspb.transcoding.ffmpeg;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
@Service
@RequiredArgsConstructor
public class SegmentUploader {

    private static final Set<String> REQUIRED_RENDITIONS = Set.of("1080p", "720p", "480p", "360p");

    private final SegmentStorageService storageService;
    private final StreamLifecyclePublisher lifecyclePublisher;
    private final MeterRegistry meterRegistry;

    private final Map<String, WatchRuntime> streamWatchers = new ConcurrentHashMap<>();
    private final Map<String, DistributionSummary> uploadDurationByRendition = new ConcurrentHashMap<>();

    public void watchStream(String streamId) {
        Path outputBase = Paths.get("/tmp", streamId);
        watchStream(streamId, outputBase);
    }

    public void watchStream(String streamId, Path outputBase) {
        if (streamWatchers.containsKey(streamId)) {
            log.info("Segment watcher already active for streamId={}", streamId);
            return;
        }

        try {
            ensureOutputLayout(outputBase);

            WatchService watchService = FileSystems.getDefault().newWatchService();
            registerDirectories(watchService, outputBase);

            WatchRuntime runtime = new WatchRuntime(streamId, outputBase, watchService);
            Thread watchThread = new Thread(() -> watchLoop(runtime), "segment-uploader-" + streamId);
            watchThread.setDaemon(true);
            runtime.thread = watchThread;
            streamWatchers.put(streamId, runtime);
            watchThread.start();

            log.info("Started segment uploader watcher for streamId={} at {}", streamId, outputBase);
        } catch (Exception ex) {
            throw new RuntimeException("Failed to start segment watcher for streamId=" + streamId, ex);
        }
    }

    public void stopWatching(String streamId) {
        WatchRuntime runtime = streamWatchers.remove(streamId);
        if (runtime == null) {
            return;
        }

        runtime.running.set(false);
        try {
            runtime.watchService.close();
        } catch (IOException e) {
            log.warn("Error closing watch service for streamId={}", streamId, e);
        }

        log.info("Stopped segment uploader watcher for streamId={}", streamId);
    }

    private void watchLoop(WatchRuntime runtime) {
        while (runtime.running.get()) {
            WatchKey key;
            try {
                key = runtime.watchService.take();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (Exception e) {
                if (runtime.running.get()) {
                    log.error("Watch loop failed for streamId={}", runtime.streamId, e);
                }
                return;
            }

            Path watchedDir = (Path) key.watchable();
            for (WatchEvent<?> event : key.pollEvents()) {
                WatchEvent.Kind<?> kind = event.kind();
                if (kind == StandardWatchEventKinds.OVERFLOW) {
                    continue;
                }

                Path relativePath = (Path) event.context();
                Path changedPath = watchedDir.resolve(relativePath);

                if (kind == StandardWatchEventKinds.ENTRY_CREATE
                    && changedPath.toString().endsWith(".ts")) {
                    handleNewSegment(runtime, changedPath);
                }
            }

            boolean valid = key.reset();
            if (!valid) {
                log.warn("Watch key invalidated for streamId={}", runtime.streamId);
                break;
            }
        }
    }

    private void handleNewSegment(WatchRuntime runtime, Path segmentPath) {
        String fileName = segmentPath.getFileName().toString();
        String rendition = segmentPath.getParent().getFileName().toString();
        if (!REQUIRED_RENDITIONS.contains(rendition)) {
            return;
        }

        try {
            waitForFileReady(segmentPath);

            long uploadStartNanos = System.nanoTime();
            storageService.uploadTs(segmentPath, runtime.streamId, rendition, fileName);
            double uploadDurationMs = (System.nanoTime() - uploadStartNanos) / 1_000_000.0;
            getUploadDurationSummary(rendition).record(uploadDurationMs);

            Path renditionManifest = runtime.outputBase.resolve(rendition).resolve("index.m3u8");
            uploadManifestWithRetries(runtime, rendition, renditionManifest);

            if ("seg_0000.ts".equals(fileName)) {
                runtime.firstSegmentsUploaded.add(rendition);
            }

            if (runtime.firstSegmentsUploaded.containsAll(REQUIRED_RENDITIONS)
                && runtime.startedPublished.compareAndSet(false, true)) {
                Path masterManifest = runtime.outputBase.resolve("master.m3u8");
                uploadMasterWithRetries(runtime, masterManifest);
                lifecyclePublisher.publishTranscodeStarted(runtime.streamId);
            }
        } catch (Exception ex) {
            log.error("Segment upload failed for streamId={} path={}", runtime.streamId, segmentPath, ex);
        }
    }

    private DistributionSummary getUploadDurationSummary(String rendition) {
        return uploadDurationByRendition.computeIfAbsent(rendition, key -> DistributionSummary.builder("hls_segment_upload_duration_ms")
            .description("Duration in milliseconds to upload HLS segment files")
            .baseUnit("milliseconds")
            .serviceLevelObjectives(100, 250, 500, 1000, 1500, 2000, 3000, 5000)
            .tag("rendition", key)
            .register(meterRegistry));
    }

    private void uploadManifestWithRetries(WatchRuntime runtime, String rendition, Path filePath) throws Exception {
        Exception last = null;
        for (int i = 0; i < 5; i++) {
            try {
                if (Files.exists(filePath)) {
                    storageService.uploadRenditionManifest(filePath, runtime.streamId, rendition);
                    return;
                }
            } catch (Exception ex) {
                last = ex;
            }
            Thread.sleep(400);
        }
        if (last != null) {
            throw last;
        }
        throw new IllegalStateException("Timed out waiting for file: " + filePath);
    }

    private void uploadMasterWithRetries(WatchRuntime runtime, Path filePath) throws Exception {
        Exception last = null;
        for (int i = 0; i < 5; i++) {
            try {
                if (Files.exists(filePath)) {
                    storageService.uploadMasterManifest(filePath, runtime.streamId);
                    return;
                }
            } catch (Exception ex) {
                last = ex;
            }
            Thread.sleep(400);
        }
        if (last != null) {
            throw last;
        }
        throw new IllegalStateException("Timed out waiting for file: " + filePath);
    }

    private void ensureOutputLayout(Path outputBase) throws IOException {
        Files.createDirectories(outputBase);
        for (String rendition : REQUIRED_RENDITIONS) {
            Files.createDirectories(outputBase.resolve(rendition));
        }
    }

    private void registerDirectories(WatchService watchService, Path outputBase) throws IOException {
        List<Path> directories = List.of(
            outputBase.resolve("1080p"),
            outputBase.resolve("720p"),
            outputBase.resolve("480p"),
            outputBase.resolve("360p")
        );

        for (Path dir : directories) {
            dir.register(
                watchService,
                StandardWatchEventKinds.ENTRY_CREATE,
                StandardWatchEventKinds.ENTRY_MODIFY
            );
        }
    }

    private void waitForFileReady(Path path) throws Exception {
        long previousSize = -1;
        for (int i = 0; i < 10; i++) {
            if (!Files.exists(path)) {
                Thread.sleep(150);
                continue;
            }

            long size = Files.size(path);
            if (size > 0 && size == previousSize) {
                return;
            }
            previousSize = size;
            Thread.sleep(150);
        }
    }

    private static final class WatchRuntime {
        private final String streamId;
        private final Path outputBase;
        private final WatchService watchService;
        private final AtomicBoolean running = new AtomicBoolean(true);
        private final Set<String> firstSegmentsUploaded = ConcurrentHashMap.newKeySet();
        private final AtomicBoolean startedPublished = new AtomicBoolean(false);

        private volatile Thread thread;

        private WatchRuntime(String streamId, Path outputBase, WatchService watchService) {
            this.streamId = streamId;
            this.outputBase = outputBase;
            this.watchService = watchService;
        }
    }
}