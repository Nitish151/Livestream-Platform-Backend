package com.lspb.transcoding.ffmpeg;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
@Component
public class FfmpegRunner {

    private static final Duration SILENCE_TIMEOUT = Duration.ofSeconds(30);
    private static final int MAX_RETRIES = 3;

    private final Map<String, StreamProcessState> runningStreams = new ConcurrentHashMap<>();

    public Process startTranscoding(String rtmpUrl, String streamId) throws IOException {
        Path outputBase = Paths.get("/tmp", streamId);
        return startTranscoding(rtmpUrl, streamId, outputBase);
    }

    public Process startTranscoding(String rtmpUrl, String streamId, Path outputBase) throws IOException {
        createOutputDirectories(outputBase);

        StreamProcessState existingState = runningStreams.get(streamId);
        if (existingState != null && existingState.process != null && existingState.process.isAlive()) {
            log.info("FFmpeg process already running for streamId={}", streamId);
            return existingState.process;
        }

        StreamProcessState newState = new StreamProcessState(rtmpUrl, streamId, outputBase);
        runningStreams.put(streamId, newState);
        return launchProcess(newState);
    }

    public void stopTranscoding(String streamId) {
        StreamProcessState state = runningStreams.remove(streamId);
        if (state == null) {
            return;
        }

        state.stopRequested.set(true);
        Process process = state.process;
        if (process != null && process.isAlive()) {
            process.destroy();
            try {
                if (!process.waitFor(5, TimeUnit.SECONDS)) {
                    process.destroyForcibly();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                process.destroyForcibly();
            }
        }

        log.info("Stopped FFmpeg process for streamId={}", streamId);
    }

    public boolean isTranscodingHealthy(String streamId) {
        StreamProcessState state = runningStreams.get(streamId);
        if (state == null || state.process == null || !state.process.isAlive()) {
            return false;
        }

        long silenceMs = System.currentTimeMillis() - state.lastOutputAtMs.get();
        return silenceMs <= SILENCE_TIMEOUT.toMillis();
    }

    private Process launchProcess(StreamProcessState state) throws IOException {
        List<String> cmd = buildFfmpegCommand(state.rtmpUrl, state.outputBase);
        ProcessBuilder processBuilder = new ProcessBuilder(cmd)
            .redirectErrorStream(true);

        Process process = processBuilder.start();
        state.process = process;
        state.lastOutputAtMs.set(System.currentTimeMillis());

        log.info("Started FFmpeg for streamId={} with pid={} and outputBase={}",
            state.streamId,
            process.pid(),
            state.outputBase);

        Thread outputThread = new Thread(() -> readProcessOutput(state, process), "ffmpeg-output-" + state.streamId);
        outputThread.setDaemon(true);
        outputThread.start();

        Thread healthThread = new Thread(() -> monitorProcessHealth(state, process), "ffmpeg-health-" + state.streamId);
        healthThread.setDaemon(true);
        healthThread.start();

        return process;
    }

    private void readProcessOutput(StreamProcessState state, Process process) {
        try (BufferedReader reader = process.inputReader()) {
            String line;
            while ((line = reader.readLine()) != null) {
                state.lastOutputAtMs.set(System.currentTimeMillis());
                log.info("[ffmpeg][{}] {}", state.streamId, line);
            }
        } catch (IOException e) {
            if (!state.stopRequested.get()) {
                log.warn("Error reading FFmpeg output for streamId={}: {}", state.streamId, e.getMessage());
            }
        }
    }

    private void monitorProcessHealth(StreamProcessState state, Process watchedProcess) {
        try {
            while (!state.stopRequested.get() && watchedProcess.isAlive()) {
                long silenceMs = System.currentTimeMillis() - state.lastOutputAtMs.get();
                if (silenceMs > SILENCE_TIMEOUT.toMillis()) {
                    log.warn("FFmpeg silent for {} ms on streamId={}; killing and retrying", silenceMs, state.streamId);
                    restartProcess(state, watchedProcess);
                    return;
                }

                Thread.sleep(5000);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private synchronized void restartProcess(StreamProcessState state, Process expectedProcess) {
        Process current = state.process;
        if (current != expectedProcess || state.stopRequested.get()) {
            return;
        }

        if (state.retryCount.incrementAndGet() > MAX_RETRIES) {
            log.error("Exceeded FFmpeg retry budget for streamId={}", state.streamId);
            stopTranscoding(state.streamId);
            return;
        }

        if (current.isAlive()) {
            current.destroyForcibly();
        }

        try {
            Process restarted = launchProcess(state);
            state.process = restarted;
            log.info("Restarted FFmpeg for streamId={} attempt={}", state.streamId, state.retryCount.get());
        } catch (IOException ex) {
            log.error("Failed to restart FFmpeg for streamId={}", state.streamId, ex);
            stopTranscoding(state.streamId);
        }
    }

    private List<String> buildFfmpegCommand(String rtmpUrl, Path outputBase) {
        // Use forward slashes so FFmpeg handles the %v placeholder correctly
        // on both Linux and Windows.
        String base = outputBase.toString().replace('\\', '/');
        String segPattern  = base + "/%v/seg_%04d.ts";
        String playPattern = base + "/%v/index.m3u8";

        List<String> cmd = new ArrayList<>();
        cmd.add("ffmpeg");
        // Exit automatically if no data received from RTMP for 10 seconds (in microseconds).
        // Without this, FFmpeg hangs forever printing frozen progress when the stream ends.
        cmd.add("-rw_timeout");
        cmd.add("10000000");
        cmd.add("-i");
        cmd.add(rtmpUrl);

        cmd.addAll(List.of(
            "-map", "0:v", "-map", "0:a",  // rendition 0 — 1080p
            "-map", "0:v", "-map", "0:a",  // rendition 1 — 720p
            "-map", "0:v", "-map", "0:a",  // rendition 2 — 480p
            "-map", "0:v", "-map", "0:a",  // rendition 3 — 360p
            "-c:v:0", "libx264", "-b:v:0", "6000k", "-s:v:0", "1920x1080",
            "-c:v:1", "libx264", "-b:v:1", "3000k", "-s:v:1", "1280x720",
            "-c:v:2", "libx264", "-b:v:2", "1500k", "-s:v:2", "854x480",
            "-c:v:3", "libx264", "-b:v:3", "800k",  "-s:v:3", "640x360",
            "-c:a", "aac", "-b:a", "128k",
            "-f", "hls",
            "-hls_time", "4",
            "-hls_list_size", "10",
            "-hls_flags", "delete_segments+append_list+independent_segments",
            "-hls_segment_type", "mpegts",
            "-hls_segment_filename", segPattern,
            "-master_pl_name", "master.m3u8",
            "-var_stream_map", "v:0,a:0,name:1080p v:1,a:1,name:720p v:2,a:2,name:480p v:3,a:3,name:360p",
            playPattern
        ));

        return cmd;
    }

    private void createOutputDirectories(Path outputBase) throws IOException {
        Files.createDirectories(outputBase.resolve("1080p"));
        Files.createDirectories(outputBase.resolve("720p"));
        Files.createDirectories(outputBase.resolve("480p"));
        Files.createDirectories(outputBase.resolve("360p"));
    }

    private static final class StreamProcessState {
        private final String rtmpUrl;
        private final String streamId;
        private final Path outputBase;
        private final AtomicInteger retryCount = new AtomicInteger(0);
        private final AtomicLong lastOutputAtMs = new AtomicLong(System.currentTimeMillis());
        private final AtomicBoolean stopRequested = new AtomicBoolean(false);

        private volatile Process process;

        private StreamProcessState(String rtmpUrl, String streamId, Path outputBase) {
            this.rtmpUrl = rtmpUrl;
            this.streamId = streamId;
            this.outputBase = outputBase;
        }
    }
}
