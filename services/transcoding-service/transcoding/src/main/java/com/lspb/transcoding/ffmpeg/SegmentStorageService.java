package com.lspb.transcoding.ffmpeg;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
@Service
@RequiredArgsConstructor
public class SegmentStorageService {

    public static final String SEGMENT_BUCKET = "lspb-segments";

    private final AmazonS3 s3Client;
    private final AtomicBoolean bucketReady = new AtomicBoolean(false);

    public void uploadTs(Path localPath, String streamId, String rendition, String fileName) throws Exception {
        String key = streamId + "/" + rendition + "/" + fileName;
        uploadObject(localPath, key, "video/MP2T", "max-age=31536000");
    }

    public void uploadRenditionManifest(Path localPath, String streamId, String rendition) throws Exception {
        String key = streamId + "/" + rendition + "/index.m3u8";
        uploadObject(localPath, key, "application/vnd.apple.mpegurl", "no-cache");
    }

    public void uploadMasterManifest(Path localPath, String streamId) throws Exception {
        String key = streamId + "/master.m3u8";
        uploadObject(localPath, key, "application/vnd.apple.mpegurl", "no-cache");
    }

    private void uploadObject(Path localPath, String key, String contentType, String cacheControl) throws Exception {
        ensureBucketExists();

        long length = Files.size(localPath);
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(length);
        metadata.setContentType(contentType);
        metadata.setCacheControl(cacheControl);

        try (InputStream in = Files.newInputStream(localPath)) {
            PutObjectRequest request = new PutObjectRequest(SEGMENT_BUCKET, key, in, metadata);
            s3Client.putObject(request);
        }

        log.info("Uploaded object to MinIO/S3: bucket={}, key={}", SEGMENT_BUCKET, key);
    }

    private synchronized void ensureBucketExists() {
        if (bucketReady.get()) {
            return;
        }

        if (!s3Client.doesBucketExistV2(SEGMENT_BUCKET)) {
            s3Client.createBucket(SEGMENT_BUCKET);
            log.info("Created bucket: {}", SEGMENT_BUCKET);
        }

        bucketReady.set(true);
    }
}
