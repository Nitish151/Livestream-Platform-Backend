package com.lspb.shared.kafka.schemas;

import lombok.Data;

@Data
public class TranscodingJob {
    private String eventType;
    private String streamId;
    private String userId;
    private String timestamp;
    private String traceId;
    private String jobId;
    private String rtmpUrl;
    private String requestedAt;
}
