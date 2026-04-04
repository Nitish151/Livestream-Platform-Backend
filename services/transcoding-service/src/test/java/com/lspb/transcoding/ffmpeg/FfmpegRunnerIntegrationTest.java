package com.lspb.transcoding.ffmpeg;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration test for FfmpegRunner.
 *
 * Feeds scripts/test.mp4 directly into FFmpeg (no live RTMP source needed)
 * and asserts that .ts segment files are produced in each rendition folder.
 */
class FfmpegRunnerIntegrationTest {

    private static final String STREAM_ID = "test-stream-001";
    private static final Path TEST_INPUT = Paths.get(
            System.getProperty("user.dir"))
            .resolve("../../../scripts/test.mp4")
            .normalize();

    private FfmpegRunner runner;
    private Path outputBase;

    @BeforeEach
    void setUp() throws IOException {
        runner = new FfmpegRunner();
        outputBase = Paths.get(System.getProperty("java.io.tmpdir"), STREAM_ID);
    }

    @AfterEach
    void tearDown() throws IOException {
        runner.stopTranscoding(STREAM_ID);

        if (Files.exists(outputBase)) {
            try (Stream<Path> walk = Files.walk(outputBase)) {
                walk.sorted(Comparator.reverseOrder()).forEach(p -> {
                    try { Files.delete(p); } catch (IOException ignored) {}
                });
            }
        }
    }

    @Test
    void shouldProduceSegmentsInAllRenditionFolders() throws Exception {
        assertTrue(Files.exists(TEST_INPUT),
                "test.mp4 not found at: " + TEST_INPUT.toAbsolutePath());

        System.out.println("[test] Input  : " + TEST_INPUT.toAbsolutePath());
        System.out.println("[test] Output : " + outputBase.toAbsolutePath());

        Process process = runner.startTranscoding(TEST_INPUT.toString(), STREAM_ID, outputBase);
        assertNotNull(process, "FFmpeg process must not be null");
        assertTrue(process.isAlive(), "FFmpeg process should start successfully");

        List<String> renditions = List.of("1080p", "720p", "480p", "360p");

        // Poll up to 60 seconds for .ts files in every rendition folder.
        // FFmpeg may encode slower-than-realtime on a test host.
        long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(60);
        boolean allFound = false;

        while (System.currentTimeMillis() < deadline) {
            if (allRenditionsHaveSegments(outputBase, renditions)) {
                allFound = true;
                break;
            }
            Thread.sleep(1000);
        }

        // Report what we actually found before asserting
        System.out.println("[test] --- segment inventory ---");
        for (String rendition : renditions) {
            Path dir = outputBase.resolve(rendition);
            if (Files.exists(dir)) {
                try (Stream<Path> files = Files.list(dir)) {
                    files.sorted().forEach(f -> System.out.println("[test]   " + f.getFileName()));
                }
            } else {
                System.out.println("[test]   " + rendition + "/ — directory missing");
            }
        }

        assertTrue(allFound,
                "Expected .ts segments in all 4 rendition folders within 30 s. " +
                "Check the segment inventory above for details.");

        // Check the master playlist is present
        assertTrue(Files.exists(outputBase.resolve("master.m3u8")),
                "master.m3u8 should exist in " + outputBase);

        System.out.println("[test] PASSED - .ts segments found in all rendition folders");
    }

    @Test
    void stopShouldTerminateProcess() throws Exception {
        assertTrue(Files.exists(TEST_INPUT),
                "test.mp4 not found at: " + TEST_INPUT.toAbsolutePath());

        Process process = runner.startTranscoding(TEST_INPUT.toString(), STREAM_ID, outputBase);
        assertTrue(process.isAlive());

        runner.stopTranscoding(STREAM_ID);

        // Give OS a moment to reap the process
        boolean terminated = process.waitFor(5, TimeUnit.SECONDS);
        assertTrue(terminated, "FFmpeg process should have terminated within 5 s of stopTranscoding()");
        assertFalse(runner.isTranscodingHealthy(STREAM_ID),
                "isTranscodingHealthy should return false after stop");
    }

    // ---- helpers ----

    private boolean allRenditionsHaveSegments(Path base, List<String> renditions) {
        for (String rendition : renditions) {
            Path dir = base.resolve(rendition);
            if (!Files.exists(dir)) return false;
            try (Stream<Path> files = Files.list(dir)) {
                boolean hasTs = files.anyMatch(p -> p.toString().endsWith(".ts"));
                if (!hasTs) return false;
            } catch (IOException e) {
                return false;
            }
        }
        return true;
    }
}
